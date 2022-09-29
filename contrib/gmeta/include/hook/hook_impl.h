
#ifndef _HOOK_IMPL_H_
#define _HOOK_IMPL_H_

#include "postgres.h"
#include "access/heapam.h"
#include "storage/dsm.h"



extern void heap_insert_hook_impl(Relation relation,
										 HeapTuple tup,
										 CommandId cid,
										 int options,
										 BulkInsertState bistate);


#endif