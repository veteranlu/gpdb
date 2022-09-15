#ifndef _FDBUTIL_H_
#define _FDBUTIL_H_

#include <stdio.h>
#include <pthread.h>

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 710
#endif

#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

typedef struct Connection
{
    /* data */
    FDBDatabase* db;
    pthread_t netThread;
} Connection;

typedef struct Transaction
{
    FDBTransaction* tr;
} Transaction;

struct Error {
	char* message;

	struct Error* next;
};

typedef struct KeyValueNode {
    FDBKeyValue *data;

    struct KeyValueNode *next;
} KeyValueNode;

typedef struct ResultSet {
	struct KeyValueNode *kvs;
	struct Error* errors;
} ResultSet;

void freeResultSet(struct ResultSet* rs);

int CreateConnection(Connection *conn, char *cluster_file_path);
int CloseConnection(Connection *conn);

int KVSet(Connection *conn, FDBKeyValue *kvs, ResultSet *result);
int KVGet(Connection *conn, FDBKeyValue *kvs, ResultSet *result);
int KVGetRange(Connection *conn, FDBKeyValue *kvs, ResultSet *result);

#endif