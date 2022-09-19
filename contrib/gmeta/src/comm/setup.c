/*--------------------------------------------------------------------------
 *
 * setup.c
 *		Code to set up a dynamic shared memory segments and a specified
 *		number of background workers for shared memory message queue
 *		testing.
 *
 * Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_shm_mq/setup.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/procsignal.h"
#include "storage/shm_toc.h"
#include "utils/memutils.h"

#include "comm/comm.h"

typedef struct
{
	int			nworkers;
	BackgroundWorkerHandle *handle[FLEXIBLE_ARRAY_MEMBER];
} worker_state;

/* Pointer to shared-memory state. */
static MetaSyncSharedState *ms_state = NULL;

static worker_state *setup_background_workers(int64 queue_size, int nworkers);
static bool check_worker_status(worker_state *wstate);

/*
 * Set up a dynamic shared memory segment and zero or more background workers
 * for a test run.
 */
void
sync_worker_setup(int64 queue_size, int32 nworkers)
{
	worker_state *wstate;

	/* Set up a dynamic shared memory segment. */


	/* Register background workers. */
	/* TODO(kaka): we should setup multi-bgworker, just like a thread pool */
	wstate = setup_background_workers(queue_size, nworkers);
}

/*
 * Register background workers.
 */
static worker_state *
setup_background_workers(int64 queue_size, int nworkers)
{
	MemoryContext oldcontext;
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	int			i;
	pid_t		pid;

	Assert(nworkers == 1);


	/* Configure a worker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	strcpy(worker.bgw_library_name, "gmeta");
	strcpy(worker.bgw_function_name, "gmeta_bgworker_main");
	strcpy(worker.bgw_name, "gmeta bgworker");
	strcpy(worker.bgw_type, "gmeta_bgworker");
	worker.bgw_main_arg = Int64GetDatum(queue_size);
	/* set bgw_notify_pid, so we can detect if the worker stops */
	worker.bgw_notify_pid = 0;


	/* Register the workers. */
	if (process_shared_preload_libraries_in_progress)
	{
		RegisterBackgroundWorker(&worker);
		return;
	}
		
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					errmsg("could not register background process"),
					errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	return NULL;
}


void 
setup_front_msgqueue(dsm_segment **p_seg, shm_mq_handle **p_inqh, shm_mq_handle **p_outqh)
{
	bool found;
    dsm_segment *seg;
    shm_toc    *toc;
    shm_mq	   *inq;
	shm_mq	   *outq;
	shm_mq_handle *inqh;
	shm_mq_handle *outqh;

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

	*p_seg = seg;
	*p_inqh = inqh;
	*p_outqh = outqh;
}

void release_front_msgqueue(dsm_segment *seg)
{
	dsm_detach(seg);
}