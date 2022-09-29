
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "catalog/heap.h"

#include "storage/shm_mq.h"
#include "storage/shm_toc.h"

#include "comm/comm.h"
#include "comm/handler.h"

PG_FUNCTION_INFO_V1(pg_show_gmetadata);

typedef struct GlobalMetadata
{
    char *data;
    int count;
    int next;
} GlobalMetadata;

Datum
pg_show_gmetadata(PG_FUNCTION_ARGS)
{
    /* return pg_class's tuple */        
    FuncCallContext *funcctx;
    

    if (SRF_IS_FIRSTCALL())
	{
        char *key;    
        size_t key_size;   

        char *data;
        size_t len;

        Oid			rel_oid;
        Oid         db_oid;
        /* Default get pg_class's info */
        Oid         catalog_oid; 

        static dsm_segment     *seg;
        static shm_mq_handle   *inqh;
        static shm_mq_handle   *outqh;

        key = palloc0(128);

        /* Basic sanity checking. */
        if (PG_ARGISNULL(0))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("database cannot be null")));
        db_oid = PG_GETARG_OID(0);

        if (PG_ARGISNULL(1))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("relation cannot be null")));
        rel_oid = PG_GETARG_OID(1);

        if (PG_ARGISNULL(2))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("catalog cannot be null")));
        catalog_oid = PG_GETARG_OID(2);
        if (RelationRelationId != catalog_oid)
            elog(ERROR, "still not support read catalog %d", catalog_oid);

        Relation pg_class_desc = table_open(RelationRelationId, AccessShareLock);
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->tuple_desc = RelationGetDescr(pg_class_desc);

        setup_front_msgqueue(&seg, &inqh, &outqh);
	
        CommOperateType op_type = OPTYPE_GET;

        /* 1. Send it back out. */
        int res = shm_mq_send(outqh, sizeof(op_type), &op_type, false);
        if (res != SHM_MQ_SUCCESS)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("fdb send request by msg queue failed, error code: %d.", res)));

        /* 2. send request data */
        sprintf(key, "%u/%u/%u", db_oid, catalog_oid, rel_oid);
        key_size = strlen(key) + 1;
        elog(LOG, "send get request key %s.", key);

        size_t block_size = sizeof(KeyValueBlock) + key_size;
        KeyValueBlock *kv_block = palloc0(block_size);
        kv_block->key_size = key_size;
        kv_block->value_size = 0;
        char *data_curser = (char *)kv_block + SizeofKeyValueBlockHeader;
        memcpy(data_curser, key, kv_block->key_size);
        res = shm_mq_send(outqh, block_size, kv_block, false);
        if (res != SHM_MQ_SUCCESS)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("fdb send request by msg queue failed, error code: %d.", res)));

        /* 3. Receive a message. */
        res = shm_mq_receive(inqh, &len, &data, false);
        if (res != SHM_MQ_SUCCESS)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("fdb receive by msg queue failed, error code: %d.", res)));

        GlobalMetadata *gmetadata = palloc0(sizeof(GlobalMetadata));
        /* TODO(kaka): hardcode, handle the failed logic */
        gmetadata->next = 0;
        gmetadata->count = 1;
        gmetadata->data = palloc(len);
        memcpy(gmetadata->data, data, len);
        
        funcctx->user_fctx = gmetadata;

        /* clean resourse */
        pfree(kv_block);

        release_front_msgqueue(seg);
        table_close(pg_class_desc, AccessShareLock);

        /* collect_visibility_data will verify the relkind */
		MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    GlobalMetadata *gmetadata = (GlobalMetadata *)funcctx->user_fctx;

    if (gmetadata->next < gmetadata->count)
    {
        Datum		values[Natts_pg_class];
        bool		nulls[Natts_pg_class];
        HeapTuple	tup;
        int         tup_len;

        // Form_pg_class ctuple = (Form_pg_class) GETSTRUCT(tup);
        Form_pg_class rd_rel = gmetadata->data;

        /* This is a tad tedious, but way cleaner than what we used to do... */
        memset(values, 0, sizeof(values));
        memset(nulls, false, sizeof(nulls));   

        values[Anum_pg_class_oid - 1] = ObjectIdGetDatum(rd_rel->oid);
        values[Anum_pg_class_relname - 1] = NameGetDatum(&rd_rel->relname);
        values[Anum_pg_class_relnamespace - 1] = ObjectIdGetDatum(rd_rel->relnamespace);
        values[Anum_pg_class_reltype - 1] = ObjectIdGetDatum(rd_rel->reltype);
        values[Anum_pg_class_reloftype - 1] = ObjectIdGetDatum(rd_rel->reloftype);
        values[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(rd_rel->relowner);
        values[Anum_pg_class_relam - 1] = ObjectIdGetDatum(rd_rel->relam);
        values[Anum_pg_class_relfilenode - 1] = ObjectIdGetDatum(rd_rel->relfilenode);
        values[Anum_pg_class_reltablespace - 1] = ObjectIdGetDatum(rd_rel->reltablespace);
        values[Anum_pg_class_relpages - 1] = Int32GetDatum(rd_rel->relpages);
        values[Anum_pg_class_reltuples - 1] = Float4GetDatum(rd_rel->reltuples);
        values[Anum_pg_class_relallvisible - 1] = Int32GetDatum(rd_rel->relallvisible);
        values[Anum_pg_class_reltoastrelid - 1] = ObjectIdGetDatum(rd_rel->reltoastrelid);
        values[Anum_pg_class_relhasindex - 1] = BoolGetDatum(rd_rel->relhasindex);
        values[Anum_pg_class_relisshared - 1] = BoolGetDatum(rd_rel->relisshared);
        values[Anum_pg_class_relpersistence - 1] = CharGetDatum(rd_rel->relpersistence);
        values[Anum_pg_class_relkind - 1] = CharGetDatum(rd_rel->relkind);
        values[Anum_pg_class_relnatts - 1] = Int16GetDatum(rd_rel->relnatts);
        values[Anum_pg_class_relchecks - 1] = Int16GetDatum(rd_rel->relchecks);
        values[Anum_pg_class_relhasrules - 1] = BoolGetDatum(rd_rel->relhasrules);
        values[Anum_pg_class_relhastriggers - 1] = BoolGetDatum(rd_rel->relhastriggers);
        values[Anum_pg_class_relrowsecurity - 1] = BoolGetDatum(rd_rel->relrowsecurity);
        values[Anum_pg_class_relforcerowsecurity - 1] = BoolGetDatum(rd_rel->relforcerowsecurity);
        values[Anum_pg_class_relhassubclass - 1] = BoolGetDatum(rd_rel->relhassubclass);
        values[Anum_pg_class_relispopulated - 1] = BoolGetDatum(rd_rel->relispopulated);
        values[Anum_pg_class_relreplident - 1] = CharGetDatum(rd_rel->relreplident);
        values[Anum_pg_class_relispartition - 1] = BoolGetDatum(rd_rel->relispartition);
        values[Anum_pg_class_relrewrite - 1] = ObjectIdGetDatum(rd_rel->relrewrite);
        values[Anum_pg_class_relfrozenxid - 1] = TransactionIdGetDatum(rd_rel->relfrozenxid);
        values[Anum_pg_class_relminmxid - 1] = MultiXactIdGetDatum(rd_rel->relminmxid);
        
        /* TODO(kaka): donot implement */
        nulls[Anum_pg_class_relacl - 1] = true;
        nulls[Anum_pg_class_reloptions - 1] = true;
        /* relpartbound is set by updating this tuple, if necessary */
        nulls[Anum_pg_class_relpartbound - 1] = true;

        tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        gmetadata->next++;
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup));
    }

    SRF_RETURN_DONE(funcctx);
}

