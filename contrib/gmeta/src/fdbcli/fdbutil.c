#include "fdbcli/fdbutil.h"

#include <assert.h>
#include <sys/time.h>
#include <arpa/inet.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include <inttypes.h>

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 710
#endif

#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

typedef struct RunResult {
	int res;
	fdb_error_t e;
} RunResult;

static void addError(struct ResultSet* rs, const char* message) {
	struct Error* e = malloc(sizeof(struct Error));
	e->message = (char*)malloc(strlen(message) + 1);
	strcpy(e->message, message);
	e->next = rs->errors;
	rs->errors = e;
}

static fdb_error_t 
getError(fdb_error_t err, const char* context, struct ResultSet* rs) 
{
	if (err) {
		char* msg = (char*)malloc(strlen(context) + 100);
		sprintf(msg, "Error in %s: %s", context, fdb_get_error(err));
		fprintf(stderr, "%s\n", msg);
		if (rs != NULL) {
			addError(rs, msg);
		}

		free(msg);
	}

	return err;
}

static fdb_error_t waitError(FDBFuture* f) {
	fdb_error_t blockError = fdb_future_block_until_ready(f);
	if (!blockError) {
		return fdb_future_get_error(f);
	} else {
		return blockError;
	}
}

static void writeResultSet(struct ResultSet* rs) {
	uint64_t id = ((uint64_t)rand() << 32) + rand();
	char name[100];
	sprintf(name, "fdb-c_result-%" SCNu64 ".json", id);
	FILE* fp = fopen(name, "w");
	if (!fp) {
		fprintf(stderr, "Could not open results file %s\n", name);
		exit(1);
	}

	fprintf(fp, "\t\"errors\": [\n");

	struct Error* e = rs->errors;
	while (e != NULL) {
		fprintf(fp, "\t\t\"%s\"", e->message);
		if (e->next != NULL) {
			fprintf(fp, ",");
		}
		fprintf(fp, "\n");
		e = e->next;
	}

	fprintf(fp, "\t]\n");
	fprintf(fp, "}\n");

	fclose(fp);
}

static void addResultSetData(struct ResultSet *rs, FDBKeyValue *data)
{
	KeyValueNode *next = rs->kvs;
	KeyValueNode *kv = (KeyValueNode *) malloc(sizeof(KeyValueNode));
	kv->data = data;
	kv->next = next;
	rs->kvs = kv;
}

void 
freeResultSet(struct ResultSet* rs) 
{
	struct KeyValueNode* kv = rs->kvs;
	while (kv != NULL)
	{
		KeyValueNode *next = kv->next;
		free(kv->data->key);
		free(kv->data->value);
		free(kv->data);

		kv = next;
	}

	struct Error* e = rs->errors;
	while (e != NULL) {
		struct Error* next = e->next;
		free(e->message);
		free(e);
		e = next;
	}

	free(rs);
}

static void 
checkError(fdb_error_t err, const char* context, struct ResultSet* rs) 
{
	if (getError(err, context, rs)) {
		if (rs != NULL) {
			writeResultSet(rs);
			freeResultSet(rs);
		}
		exit(1);
	}
}

static fdb_error_t logError(fdb_error_t err, const char* context, struct ResultSet* rs) {
	char* msg = (char*)malloc(strlen(context) + 100);
	sprintf(msg, "Error in %s: %s", context, fdb_get_error(err));
	fprintf(stderr, "%s\n", msg);
	if (rs != NULL) {
		addError(rs, msg);
	}

	free(msg);
	return err;
}

static fdb_error_t maybeLogError(fdb_error_t err, const char* context, struct ResultSet* rs) {
	if (err && !fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
		return logError(err, context, rs);
	}
	return err;
}

static void* runNetwork() {
	checkError(fdb_run_network(), "run network", NULL);
	return NULL;
}

static FDBDatabase* openDatabase(char *cluster_file_path, struct ResultSet* rs, pthread_t* netThread) {
	checkError(fdb_setup_network(), "setup network", rs);
	pthread_create(netThread, NULL, (void*)(&runNetwork), NULL);

	FDBDatabase* db;
	checkError(fdb_create_database(cluster_file_path, &db), "create database", rs);

	return db;
}

#define RES(x, y)                                                                                                      \
	(struct RunResult) { x, y }

static struct RunResult 
run(Connection *conn, void *params, ResultSet* rs,
    struct RunResult (*func)(FDBTransaction*, void*, ResultSet*)) 
{
	assert(conn != NULL);
	assert(conn->db != NULL);

	FDBTransaction* tr = NULL;
	fdb_error_t e = fdb_database_create_transaction(conn->db, &tr);
	checkError(e, "create transaction", rs);

	while (1) {
		struct RunResult r = func(tr, params, rs);
		e = r.e;
		if (!e) {
			FDBFuture* f = fdb_transaction_commit(tr);
			e = waitError(f);
			fdb_future_destroy(f);
		}

		if (e) {
			FDBFuture* f = fdb_transaction_on_error(tr, e);
			fdb_error_t retryE = waitError(f);
			fdb_future_destroy(f);
			if (retryE) {
				fdb_transaction_destroy(tr);
				return (struct RunResult){ 0, retryE };
			}
		} else {
			fdb_transaction_destroy(tr);
			return r;
		}
	}

	return RES(0, 4100); // internal_error ; we should never get here
}

int CreateConnection(Connection *conn, char *cluster_file_path)
{
	ResultSet *rs = (ResultSet *) malloc(sizeof(ResultSet));
	memset(rs, 0, sizeof(ResultSet));

	FDBDatabase* db = openDatabase(cluster_file_path, rs, &conn->netThread);
	conn->db = db;

	freeResultSet(rs);

	return 0;
}

int CloseConnection(Connection *conn)
{
	assert(conn != NULL);
	assert(conn->db != NULL);

	ResultSet *rs = (ResultSet *) malloc(sizeof(ResultSet));
	memset(rs, 0, sizeof(ResultSet));

	fdb_database_destroy(conn->db);
	checkError(fdb_stop_network(), "stop network", rs);
	freeResultSet(rs);

	return 0;
}

static struct RunResult 
setimpl(FDBTransaction* tr, void* params, ResultSet* rs)
{
	FDBKeyValue *kv = (FDBKeyValue *)params;

	fdb_transaction_set(tr, kv->key, kv->key_length, kv->value, kv->value_length);

	return RES(0, 0);
}

int 
KVSet(Connection *conn, FDBKeyValue *kvs, ResultSet *rs)
{
	struct RunResult r = run(conn, kvs, rs, setimpl);
	return r.e;
}

static struct RunResult
getimpl(FDBTransaction* tr, void* params, ResultSet* rs)
{
	FDBKeyValue *kv = (FDBKeyValue *)params;
	fdb_bool_t present;
	uint8_t const* outValue;
	int outValueLength;

	FDBFuture* future = fdb_transaction_get(tr, kv->key, kv->key_length, 0);
	fdb_error_t e = maybeLogError(fdb_future_block_until_ready(future), "waiting for get future", rs);
	if (e) {
		fdb_future_destroy(future);
		return RES(0, e);
	}

	FDBKeyValue *data = (FDBKeyValue *) malloc(sizeof(FDBKeyValue));
	e = maybeLogError(fdb_future_get_value(future, &present, &outValue, &outValueLength), "getting future value", rs);
	if (e) {
		fdb_future_destroy(future);
		return RES(0, e);
	}
	data->key = (uint8_t *) malloc(kv->key_length);
	memcpy(data->key, kv->key, kv->key_length);
	data->key_length = kv->key_length;

	data->value = (uint8_t *) malloc(outValueLength);
	memcpy(data->value, outValue, outValueLength);
	data->value_length = outValueLength;
	addResultSetData(rs, data);

	fdb_future_destroy(future);

	return RES(0, 0);
}

int 
KVGet(Connection *conn, FDBKeyValue *kvs, ResultSet *rs)
{
	return run(conn, kvs, rs, getimpl).e;
}

#if 1
int main(int argc, char** argv) {
	struct ResultSet* rs0 = malloc(sizeof(ResultSet));
	memset(rs0, 0, sizeof(ResultSet));

	checkError(fdb_select_api_version(710), "select API version", rs0);
	printf("Running performance test at client version: %s\n", fdb_get_client_version());

	char *cluster_file_path = "/workspace/gpdb/contrib/gmeta/config/fdb.cluster";

	Connection conn;
	CreateConnection(&conn, cluster_file_path);

	char *a = "aaa11aa";
	char *value = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

	FDBKeyValue kv;
	kv.key = (uint8_t *)a;
	kv.key_length = strlen(a) + 1;
	kv.value = (uint8_t *)value;
	kv.value_length = strlen(value) + 1;

	ResultSet rs = {NULL, NULL};
	ResultSet rs2 = {NULL, NULL};
	KVSet(&conn, &kv, &rs);

	KVGet(&conn, &kv, &rs2);

	KeyValueNode *kvdata = rs2.kvs;
	while (kvdata != NULL) 
	{
		printf("key %s, get value %s.\n", kvdata->data->key, kvdata->data->value);

		kvdata = kvdata->next;
	}

	CloseConnection(&conn);

	freeResultSet(rs0);
}

#endif