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

#include "sync_worker/sync_worker.h"

typedef struct
{
	int			nworkers;
	BackgroundWorkerHandle *handle[FLEXIBLE_ARRAY_MEMBER];
} worker_state;

static void setup_dynamic_shared_memory(int64 queue_size, int nworkers,
										dsm_segment **segp,
										sync_worker_mq_header **hdrp,
										shm_mq **outp, shm_mq **inp);
static worker_state *setup_background_workers(int64 queue_size, int nworkers);
static void cleanup_background_workers(dsm_segment *seg, Datum arg);
static void wait_for_workers_to_become_ready(worker_state *wstate,
											 volatile sync_worker_mq_header *hdr);
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
	strcpy(worker.bgw_library_name, "meta_sync");
	strcpy(worker.bgw_function_name, "sync_worker_main");
	strcpy(worker.bgw_name, "meta_sync bgworker");
	strcpy(worker.bgw_type, "meta_sync_bgworker");
	worker.bgw_main_arg = Int64GetDatum(queue_size);
	/* set bgw_notify_pid, so we can detect if the worker stops */
	worker.bgw_notify_pid = 0;

	elog(LOG, "111111111");

	/* Register the workers. */
	if (process_shared_preload_libraries_in_progress)
	{
		RegisterBackgroundWorker(&worker);
		return;
	}

	elog(LOG, "2222222222222");
		
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

static void
cleanup_background_workers(dsm_segment *seg, Datum arg)
{
	worker_state *wstate = (worker_state *) DatumGetPointer(arg);

	while (wstate->nworkers > 0)
	{
		--wstate->nworkers;
		TerminateBackgroundWorker(wstate->handle[wstate->nworkers]);
	}
}

static void
wait_for_workers_to_become_ready(worker_state *wstate,
								 volatile sync_worker_mq_header *hdr)
{
	bool		result = false;

	for (;;)
	{
		int			workers_ready;

		/* If all the workers are ready, we have succeeded. */
		SpinLockAcquire(&hdr->mutex);
		workers_ready = hdr->workers_ready;
		SpinLockRelease(&hdr->mutex);
		if (workers_ready >= wstate->nworkers)
		{
			result = true;
			break;
		}

		/* If any workers (or the postmaster) have died, we have failed. */
		if (!check_worker_status(wstate))
		{
			result = false;
			break;
		}

		/* Wait to be signalled. */
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
						 PG_WAIT_EXTENSION);

		/* Reset the latch so we don't spin. */
		ResetLatch(MyLatch);

		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();
	}

	if (!result)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("one or more background workers failed to start")));
}

static bool
check_worker_status(worker_state *wstate)
{
	int			n;

	/* If any workers (or the postmaster) have died, we have failed. */
	for (n = 0; n < wstate->nworkers; ++n)
	{
		BgwHandleStatus status;
		pid_t		pid;

		status = GetBackgroundWorkerPid(wstate->handle[n], &pid);
		if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
			return false;
	}

	/* Otherwise, things still look OK. */
	return true;
}

