#include "sync_worker/sync_worker.h"
#include "storage/shm_toc.h"

#define MAX_SYSTABLE_OID 13369

static MetaSyncSharedState *ms_state = NULL;

void 
heap_insert_hook_impl(Relation relation,
                    HeapTuple tup,
                    CommandId cid,
                    int options,
                    BulkInsertState bistate)
{
    bool found;
    dsm_segment *seg;
    shm_toc    *toc;
    shm_mq	   *inq;
	shm_mq	   *outq;
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
    found = ms_init_shmem(&ms_state);
    if (!found)
        ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shm should be inited")));
    
    elog(LOG, "handle %ld", ms_state->seg_handle);

    seg = dsm_attach(ms_state->seg_handle);
    toc = shm_toc_attach(PG_TEST_SHM_MQ_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));
    
    /*
	 * Attach to the appropriate message queues.
	 */
    outq = shm_toc_lookup(toc, 1, false);
	shm_mq_set_sender(outq, MyProc);
	outqh = shm_mq_attach(outq, seg, NULL);

    inq = shm_toc_lookup(toc, 2, false);
	shm_mq_set_receiver(inq, MyProc);
	inqh = shm_mq_attach(inq, seg, NULL);

    /* Send it back out. */



    res = shm_mq_send(outqh, tup_len, ctuple, false);
    if (res != SHM_MQ_SUCCESS)
        ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("fdb send request by msg queue failed, error code: %d.", res)));

    /* Receive a message. */
    res = shm_mq_receive(inqh, &len, &data, false);
    if (res != SHM_MQ_SUCCESS)
        ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("fdb receive by msg queue failed, error code: %d.", res)));

    if (memcmp(data, MSG_QUEUE_SUCC, sizeof(MSG_QUEUE_SUCC)) != 0)
        ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("fdb send worker failed, error msg: %s.", (char*) data)));

    dsm_detach(seg);
    
    elog(LOG, "send relation %d, successful.", relation->rd_node.relNode);
}

