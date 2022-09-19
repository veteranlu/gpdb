/*--------------------------------------------------------------------------
 *
 * test_shm_mq.h
 *		Definitions for shared memory message queues
 *
 * Copyright (c) 2013-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_shm_mq/test_shm_mq.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _SYNC_WORKER_H_
#define _SYNC_WORKER_H_

#include "postgres.h"
#include "access/heapam.h"
#include "storage/dsm.h"
#include "storage/dsm_impl.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "storage/lwlock.h"


/* Identifier for shared memory segments used by this extension. */
#define		PG_TEST_SHM_MQ_MAGIC		0x79fb2447

/*
 * This structure is stored in the dynamic shared memory segment.  We use
 * it to determine whether all workers started up OK and successfully
 * attached to their respective shared message queues.
 */
typedef struct
{
	slock_t		mutex;
	int			workers_total;
	int			workers_attached;
	int			workers_ready;
} sync_worker_mq_header;

/* Shared state information for meta sync bgworker. */
typedef struct MetaSyncSharedState
{
	LWLock		lock;			/* mutual exclusion */
	pid_t		bgworker_pid;	/* for main bgworker */

    /* Following items are for communication with sync worker, though shm-mq */
    dsm_handle  seg_handle;
} MetaSyncSharedState;

typedef struct KeyValueBlock
{
	int 	key_size;
	int 	value_size;

	/* data following the header->key, and header->value */
	char 		data[1];
} KeyValueBlock;

#define SizeofKeyValueBlockHeader offsetof(KeyValueBlock, data)

/**
 * the memory layout of seg's toc.
 * toc's key: shm_mq
 *  0: header
 *  1: bgworker 1's reciver shm_mq, test backend's send shm_mq
 *  2: bgworker 1's send shm_mq, and bgworker 2's reciver shm_mq
 *  ..
 *  n + 1: bgworker n's send shm_mq, and test backend's reciver shm_mq
 * 
 *  the msg copy pipe line, test backend send to 1, 1 recive and send to 2, .. , 
 *  n-1 recive and send to n, n recive and send to test backend.
 */

/* Set up dynamic shared memory and background workers for test run. */
extern void sync_worker_setup(int64 queue_size, int32 nworkers);

/* Main entrypoint for a worker. */
extern void gmeta_bgworker_main(Datum) pg_attribute_noreturn();

extern bool ms_init_shmem(MetaSyncSharedState **pms_state);

extern void setup_front_msgqueue(dsm_segment **p_seg, shm_mq_handle **p_inqh, shm_mq_handle **p_outqh);
extern void release_front_msgqueue(dsm_segment *seg);



#endif
