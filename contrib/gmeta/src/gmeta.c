/*-------------------------------------------------------------------------
 *
 * meta_sync.c
 *		Sync metadata to FDB, metadata contains the system table from pg_catalog
 *		schema, also contrains the file_visibilty table.
 *
 *
 *	Copyright (c) 2022-2023, zbyte tech inc.
 *
 *	IDENTIFICATION
 *		contrib/meta_sync/src/meta_sync.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "access/relation.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/buf_internals.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "utils/resowner.h"

#include "comm/comm.h"


/* GUC variables. */
static bool sync_metadata = true; /* start worker? */


static heap_insert_hook_type prev_heap_insert = NULL;

PG_MODULE_MAGIC;


/*
 * Module load callback.
 */
void
_PG_init(void)
{
    /*
	DefineCustomIntVariable("pg_prewarm.autoprewarm_interval",
							"Sets the interval between dumps of shared buffers",
							"If set to zero, time-based dumping is disabled.",
							&autoprewarm_interval,
							300,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);
    */

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* can't define PGC_POSTMASTER variable after startup */
    /*
	DefineCustomBoolVariable("pg_prewarm.autoprewarm",
							 "Starts the autoprewarm worker.",
							 NULL,
							 &autoprewarm,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);
    */

    /* Install hooks. */
    /*
    TODO(kaka): hook heaptuple_insert,heaptuple_update,heaptuple_delete
    */
    prev_heap_insert = heap_insert_hook;
    heap_insert_hook = heap_insert_hook_impl;

	EmitWarningsOnPlaceholders("meta_sync");

	RequestAddinShmemSpace(MAXALIGN(sizeof(MetaSyncSharedState)));

	/* Register autoprewarm worker, if enabled. */
	if (sync_metadata)
    {
        int nworkers = 1;
        int queue_size = 1000;

        /* Set up dynamic shared memory segment and background workers. */
	    sync_worker_setup(queue_size, nworkers);
    }
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
    /*
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
    */
   heap_insert_hook = prev_heap_insert;

   /* Clean up. */
}

