
#include "postgres.h"

#include "comm/comm.h"
#include "comm/handler.h"
#include "fdbcli/fdbutil.h"

void 
FreeOperateResult(OperateResult oret)
{
    Assert(oret.data != NULL);
    pfree(oret.data);
}

HandlerFunc 
GetHandlerFunc(void *msg, size_t len)
{
    Assert(len == sizeof(CommOperateType));

    CommOperateType op_type = *(CommOperateType *)msg;
    switch (op_type)
    {
        case OPTYPE_GET:
            return HandleGetMsg;
        case OPTYPE_SET:
            return HandleSetMsg;
        default:
            elog(ERROR, "this kind of operate %d still not implement. ", op_type);
    }
    return NULL;
}

OperateResult 
HandleSetMsg(Connection *fdbconn, void *msg, size_t len)
{
    int 		ret;
    void       *in_data = msg;
    OperateResult oret = {0, 0};

    KeyValueBlock *kv_block;
    kv_block = (KeyValueBlock *) msg;

    char *data_curser = (char *)kv_block + SizeofKeyValueBlockHeader;

    /* send value to fdb */	
    FDBKeyValue kv;
    kv.key = (uint8_t *)data_curser;
    kv.key_length = kv_block->key_size;

    data_curser += kv_block->key_size;
    kv.value = (uint8_t *)data_curser;
    kv.value_length = kv_block->value_size;

    elog(LOG, "[kaka] set fdb key %s, value len: %d. ", (char *)kv.key, kv.value_length);

    ResultSet rs = {NULL, NULL};
    ret = KVSet(fdbconn, &kv, &rs);
    if (ret == 0)
    {
        oret.data_len = strlen(MSG_QUEUE_SUCC);
        oret.data = palloc(oret.data_len);
        memcpy(oret.data, MSG_QUEUE_SUCC, oret.data_len);
    } else {
        oret.data_len = strlen(MSG_QUEUE_FAIL);
        oret.data = palloc(oret.data_len);
        memcpy(oret.data, MSG_QUEUE_FAIL, oret.data_len);
    }

    return oret;
}

OperateResult 
HandleGetMsg(Connection *fdbconn, void *msg, size_t len)
{
    int 		ret;
    void *in_data = msg;
    KeyValueBlock *kv_block;
    kv_block = (KeyValueBlock *) msg;
    OperateResult oret = {0, 0};

    /* send value to fdb */	
    char *data_curser = (char *)kv_block + SizeofKeyValueBlockHeader;
    FDBKeyValue kv;
    kv.key = (uint8_t *)data_curser;
    kv.key_length = kv_block->key_size;

    data_curser += kv_block->key_size;
    kv.value = (uint8_t *)data_curser;
    kv.value_length = kv_block->value_size;

    /* send value to fdb */	
    elog(LOG, "[kaka] get fdb key: %s. ", (char *)kv.key);


    ResultSet rs = {NULL, NULL};
    ret = KVGet(fdbconn, &kv, &rs);
    if (ret != 0)
        elog(WARNING, "fdb get failed, error code %d", ret);

    KeyValueNode *kvdata = rs.kvs;

    /* TODO(kaka): not handle the multi-record */
    if (kvdata != NULL)
    {
        oret.data_len = kvdata->data->value_length;
        oret.data = palloc(oret.data_len); 
        memcpy(oret.data, kvdata->data->value, oret.data_len);
    }

    return oret;
}