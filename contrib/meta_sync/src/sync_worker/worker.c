/*--------------------------------------------------------------------------
 *
 * worker.c
 *		Code for sample worker making use of shared memory message queues.
 *		Our test worker simply reads messages from one message queue and
 *		writes them back out to another message queue.  In a real
 *		application, you'd presumably want the worker to do some more
 *		complex calculation rather than simply returning the input,
 *		but it should be possible to use much of the control logic just
 *		as presented here.
 *
 * Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_shm_mq/worker.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"

#include "sync_worker/sync_worker.h"
#include "fdbcli/fdbutil.h"

static void handle_sigterm(SIGNAL_ARGS);
static void ms_sighup_handler(SIGNAL_ARGS);

static void attach_to_queues(dsm_segment *seg, shm_toc *toc,
							 int myworkernumber, shm_mq_handle **inqhp,
							 shm_mq_handle **outqhp);
static void handle_message(shm_mq_handle *inqh, shm_mq_handle *outqh);

static void ms_detach_shmem(int code, Datum arg);


/* Pointer to shared-memory state. */
static MetaSyncSharedState *ms_state = NULL;
/* Flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/*
 * Set up a dynamic shared memory segment.
 *
 * We set up a small control region that contains only a sync_worker_mq_header,
 * plus one region per message queue.  There are as many message queues as
 * the number of workers, plus one.
 */
static void
setup_dynamic_shared_memory(int64 queue_size, int nworkers,
							dsm_segment **segp, sync_worker_mq_header **hdrp)
{
	shm_toc_estimator e;
	int			i;
	Size		segsize;
	dsm_segment *seg;
	shm_toc    *toc;
	sync_worker_mq_header *hdr;

	/* Ensure a valid queue size. */
	if (queue_size < 0 || ((uint64) queue_size) < shm_mq_minimum_size)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("queue size must be at least %zu bytes",
						shm_mq_minimum_size)));
	if (queue_size != ((Size) queue_size))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("queue size overflows size_t")));

	/*
	 * Estimate how much shared memory we need.
	 *
	 * Because the TOC machinery may choose to insert padding of oddly-sized
	 * requests, we must estimate each chunk separately.
	 *
	 * We need one key to register the location of the header, and we need
	 * nworkers + 1 keys to track the locations of the message queues.
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(sync_worker_mq_header));
	for (i = 0; i <= nworkers; ++i)
		shm_toc_estimate_chunk(&e, (Size) queue_size);
	shm_toc_estimate_keys(&e, 2 + nworkers);
	segsize = shm_toc_estimate(&e);

	/* Create the shared memory segment and establish a table of contents. */
	seg = dsm_create(shm_toc_estimate(&e), 0);
	toc = shm_toc_create(PG_TEST_SHM_MQ_MAGIC, dsm_segment_address(seg),
						 segsize);

	/* Set up the header region. */
	hdr = shm_toc_allocate(toc, sizeof(sync_worker_mq_header));
	SpinLockInit(&hdr->mutex);
	hdr->workers_total = nworkers;
	hdr->workers_attached = 0;
	hdr->workers_ready = 0;
	shm_toc_insert(toc, 0, hdr);

	/* Set up one message queue per worker, plus one. */
	for (i = 0; i <= nworkers; ++i)
	{
		shm_mq	   *mq;

		mq = shm_mq_create(shm_toc_allocate(toc, (Size) queue_size),
						   (Size) queue_size);
		shm_toc_insert(toc, i + 1, mq);
	}

	/* Return results to caller. */
	*segp = seg;
	*hdrp = hdr;
}

/*
 * Background worker entrypoint.
 *
 * This is intended to demonstrate how a background worker can be used to
 * facilitate a parallel computation.  Most of the logic here is fairly
 * boilerplate stuff, designed to attach to the shared memory segment,
 * notify the user backend that we're alive, and so on.  The
 * application-specific bits of logic that you'd replace for your own worker
 * are attach_to_queues() and copy_messages().
 */
void
sync_worker_main(Datum main_arg)
{
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq_handle *inqh;
	shm_mq_handle *outqh;
	volatile sync_worker_mq_header *hdr;
	int			myworkernumber;
	PGPROC	   *registrant;
	bool 		first_time;
	int64      queue_size = DatumGetInt64(main_arg);


	elog(LOG, "22222223333333333333222222");



	/*
	 * Establish signal handlers.
	 *
	 * We want CHECK_FOR_INTERRUPTS() to kill off this worker process just as
	 * it would a normal user backend.  To make that happen, we establish a
	 * signal handler that is a stripped-down version of die().
	 */
	pqsignal(SIGTERM, handle_sigterm);
	pqsignal(SIGHUP, ms_sighup_handler);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "161");
	setup_dynamic_shared_memory(queue_size, 1, &seg, &hdr);


	/*
	 * Connect to the dynamic shared memory segment.
	 *
	 * The backend that registered this worker passed us the ID of a shared
	 * memory segment to which we must attach for further instructions.  Once
	 * we've mapped the segment in our address space, attach to the table of
	 * contents so we can locate the various data structures we'll need to
	 * find within the segment.
	 *
	 * Note: at this point, we have not created any ResourceOwner in this
	 * process.  This will result in our DSM mapping surviving until process
	 * exit, which is fine.  If there were a ResourceOwner, it would acquire
	 * ownership of the mapping, but we have no need for that.
	 */
	toc = shm_toc_attach(PG_TEST_SHM_MQ_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	first_time = ms_init_shmem(&ms_state);
	if(!first_time)
		ereport(ERROR,
				(errmsg("meta sync worker is already running under PID %lu",
						(unsigned long) ms_state->bgworker_pid)));
	on_shmem_exit(ms_detach_shmem, 0);
	/*
	 * Store our PID in the shared memory area --- unless there's already
	 * another worker running, in which case just exit.
	 */
	LWLockAcquire(&ms_state->lock, LW_EXCLUSIVE);
	if (ms_state->bgworker_pid != InvalidPid)
	{
		LWLockRelease(&ms_state->lock);
		ereport(ERROR,
				(errmsg("meta sync worker is already running under PID %lu",
						(unsigned long) ms_state->bgworker_pid)));
	}
	ms_state->bgworker_pid = MyProcPid;
	ms_state->seg_handle = dsm_segment_handle(seg);
	LWLockRelease(&ms_state->lock);

	elog(LOG, "handle %ld", ms_state->seg_handle);

	elog(LOG, "207");

	/*
	 * Acquire a worker number.
	 *
	 * By convention, the process registering this background worker should
	 * have stored the control structure at key 0.  We look up that key to
	 * find it.  Our worker number gives our identity: there may be just one
	 * worker involved in this parallel operation, or there may be many.
	 */
	hdr = shm_toc_lookup(toc, 0, false);
	SpinLockAcquire(&hdr->mutex);
	myworkernumber = ++hdr->workers_attached;
	SpinLockRelease(&hdr->mutex);
	if (myworkernumber > hdr->workers_total)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("too many message queue testing workers already")));


	elog(LOG, "227");

	/*
	 * Attach to the appropriate message queues.
	 */
	attach_to_queues(seg, toc, myworkernumber, &inqh, &outqh);

	elog(LOG, "254");

	/* Do the work. */
	handle_message(inqh, outqh);

	elog(LOG, "259");

	/*
	 * We're done.  For cleanliness, explicitly detach from the shared memory
	 * segment (that would happen anyway during process exit, though).
	 */
	dsm_detach(seg);
	proc_exit(1);
}

/*
 * Attach to shared memory message queues.
 *
 * We use our worker number to determine to which queue we should attach.
 * The queues are registered at keys 1..<number-of-workers>.  The user backend
 * writes to queue #1 and reads from queue #<number-of-workers>; each worker
 * reads from the queue whose number is equal to its worker number and writes
 * to the next higher-numbered queue.
 */
static void
attach_to_queues(dsm_segment *seg, shm_toc *toc, int myworkernumber,
				 shm_mq_handle **inqhp, shm_mq_handle **outqhp)
{
	shm_mq	   *inq;
	shm_mq	   *outq;

	inq = shm_toc_lookup(toc, myworkernumber, false);
	shm_mq_set_receiver(inq, MyProc);
	*inqhp = shm_mq_attach(inq, seg, NULL);
	outq = shm_toc_lookup(toc, myworkernumber + 1, false);
	shm_mq_set_sender(outq, MyProc);
	*outqhp = shm_mq_attach(outq, seg, NULL);
}

/*
 * Loop, receiving and sending messages, until the connection is broken.
 *
 * This is the "real work" performed by this worker process.  Everything that
 * happens before this is initialization of one form or another, and everything
 * after this point is cleanup.
 */
static void
handle_message(shm_mq_handle *inqh, shm_mq_handle *outqh)
{
	Size		in_len, out_len;
	void	   *in_data, *out_data;
	shm_mq_result res;
	int 		ret;

	fdb_error_t err = fdb_select_api_version(710);
	if (err)
		elog(ERROR, "select API version failed, erorr code %d. ", err);

	elog(LOG, "Running test at client version: %s\n", fdb_get_client_version());

	char *cluster_file_path = "/workspace/gpdb/contrib/meta_sync/config/fdb.cluster";
	Connection conn;
	CreateConnection(&conn, cluster_file_path);


	for (;;)
	{
		/* Notice any interrupts that have occurred. */
		CHECK_FOR_INTERRUPTS();

		/* Receive a message. */
		res = shm_mq_receive(inqh, &in_len, &in_data, false);
		if (res != SHM_MQ_SUCCESS)
			break;

		Form_pg_class ctuple = (Form_pg_class) in_data;


		char *key = palloc0(sizeof(128));
		sprintf(key, "%d/%d", ctuple->relnamespace, ctuple->oid);
		elog(LOG, "[kaka] insert into fdb key: %s. ", key);

		/* send value to fdb */	
		FDBKeyValue kv;
		kv.key = (uint8_t *)key;
		kv.key_length = strlen(key) + 1;
		kv.value = (uint8_t *)ctuple;
		kv.value_length = sizeof(FormData_pg_class);

		ResultSet rs = {NULL, NULL};
		ret = KVSet(&conn, &kv, &rs);
		if (ret == 0)
		{
			out_data = MSG_QUEUE_SUCC;
			out_len = strlen(MSG_QUEUE_SUCC);
		} else {
			out_data = MSG_QUEUE_FAIL;
			out_len = strlen(MSG_QUEUE_FAIL);
		}
		
		/* Send it back out. */
		res = shm_mq_send(outqh, out_len, out_data, false);
		if (res != SHM_MQ_SUCCESS)
			break;
	}

	CloseConnection(&conn);

}

/*
 * When we receive a SIGTERM, we set InterruptPending and ProcDiePending just
 * like a normal backend.  The next CHECK_FOR_INTERRUPTS() will do the right
 * thing.
 */
static void
handle_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	SetLatch(MyLatch);

	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	errno = save_errno;
}

/*
 * Allocate and initialize autoprewarm related shared memory, if not already
 * done, and set up backend-local pointer to that state.  Returns true if an
 * existing shared memory segment was found.
 */
bool
ms_init_shmem(MetaSyncSharedState **pms_state)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	MetaSyncSharedState *state = ShmemInitStruct("metasync",
								sizeof(MetaSyncSharedState),
								&found);
	if (!found)
	{
		/* First time through ... */
		LWLockInitialize(&state->lock, LWLockNewTrancheId());
		state->bgworker_pid = InvalidPid;
	}
	LWLockRelease(AddinShmemInitLock);

	LWLockRegisterTranche(state->lock.tranche, "zbytesyncworker");

	*pms_state = state;

	return found;
}

/*
 * Clear our PID from autoprewarm shared state.
 */
static void
ms_detach_shmem(int code, Datum arg)
{
	LWLockAcquire(&ms_state->lock, LW_EXCLUSIVE);
	if (ms_state->bgworker_pid == MyProcPid)
		ms_state->bgworker_pid = InvalidPid;
	LWLockRelease(&ms_state->lock);
}

/*
 * Signal handler for SIGHUP
 */
static void
ms_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}