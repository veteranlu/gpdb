#include "hook/hook_impl.h"

#include "storage/shm_toc.h"

#include "comm/comm.h"
#include "comm/handler.h"

#define MAX_SYSTABLE_OID 13369

static MetaSyncSharedState *ms_state = NULL;

void 
heap_insert_hook_impl(Relation relation,
                    HeapTuple tup,
                    CommandId cid,
                    int options,
                    BulkInsertState bistate)
{
    dsm_segment *seg;
	shm_mq_handle *inqh;
	shm_mq_handle *outqh;

    void	   *data;
	shm_mq_result res;
    size_t len;
    size_t tup_len;

    if (relation->rd_node.relNode > MAX_SYSTABLE_OID)
        return;

    if (relation->rd_node.relNode != RelationRelationId)
        return;

    /* encode data tuple */
    Form_pg_class ctuple = (Form_pg_class) GETSTRUCT(tup);
    tup_len = sizeof(FormData_pg_class);

    /* send to sync worker */
    setup_front_msgqueue(&seg, &inqh, &outqh);

    CommOperateType op_type = OPTYPE_SET;
    /* 1. Send it back out. */
    res = shm_mq_send(outqh, sizeof(OPTYPE_SET), &op_type, false);
    if (res != SHM_MQ_SUCCESS)
        ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("fdb send request by msg queue failed, error code: %d.", res)));

    /* 2. send request data */
    char *key = palloc0(128);
    sprintf(key, "%u/%u/%u", relation->rd_node.dbNode, RelationRelationId, ctuple->oid);
    elog(LOG, "[kaka] insert into fdb key: %s. ", key);

    KeyValueBlock *kv_block;
    size_t block_size = sizeof(KeyValueBlock) + strlen(key) + 1 + tup_len;
    kv_block = (KeyValueBlock *) palloc0(block_size);
    kv_block->key_size = strlen(key) + 1;
    kv_block->value_size = tup_len;
  
    char *data_curser = (char *)kv_block + SizeofKeyValueBlockHeader;
    memcpy(data_curser, key, kv_block->key_size);
    data_curser += kv_block->key_size;
    memcpy(data_curser, ctuple, tup_len);
    
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

    if (memcmp(data, MSG_QUEUE_SUCC, sizeof(MSG_QUEUE_SUCC)) != 0)
        ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("fdb send worker failed, error msg: %s.", (char*) data)));

    release_front_msgqueue(seg);
    
    elog(LOG, "send relation %d, successful.", relation->rd_node.relNode);
}

