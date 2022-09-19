
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

#ifndef _HANLDER_H_
#define _HANLDER_H_

#include "postgres.h"
#include "access/heapam.h"
#include "storage/dsm.h"
#include "storage/dsm_impl.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "storage/lwlock.h"
#include "fdbcli/fdbutil.h"

#define MSG_QUEUE_SUCC "s"
#define MSG_QUEUE_FAIL "e"


typedef enum CommOperateType
{
	OPTYPE_GET = 1,
	OPTYPE_GETRANGE = 2,
	OPTYPE_SET = 3,
	OPTYPE_SETBATCH = 4, 
	OPTYPE_CLEAR = 5,
} CommOperateType;

typedef struct OperateResult
{
    void *data;
    size_t data_len;
} OperateResult;

typedef OperateResult (*HandlerFunc) (Connection *fdbconn, void *msg, size_t len);

extern HandlerFunc GetHandlerFunc(void *msg, size_t len);
extern void FreeOperateResult(OperateResult oret);


extern OperateResult HandleSetMsg(Connection *fdbconn, void *msg, size_t len);
extern OperateResult HandleGetMsg(Connection *fdbconn, void *msg, size_t len);

#endif