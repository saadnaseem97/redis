#define REDISMODULE_EXPERIMENTAL_API

#include "../redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>

#define IDSIZE 10

static RedisModuleDict *VirtualKeyspaceMetadata;
static long dbnum;


static void genRandomData(char *data, int count) {
    static uint32_t state = 1234;
    int i = 0;

    while (count--) {
        state = (state*1103515245+12345);
        data[i++] = '0'+((state>>16)&63);
    }
}


int DictSetMetrics(long dbnum, const char *metric, void *ptr) {
    RedisModuleString *key = RedisModule_CreateStringFromLongLong(NULL, (long long)dbnum);
    RedisModule_StringAppendBuffer(NULL, key, ":", 1);
    RedisModule_StringAppendBuffer(NULL, key, metric, strlen(metric));
    if (RedisModule_DictSet(VirtualKeyspaceMetadata,key,ptr) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    return REDISMODULE_OK;

}

void *DictGetMetrics(long dbnum, const char *metric) {
    RedisModuleString *key = RedisModule_CreateStringFromLongLong(NULL, (long long)dbnum);
    RedisModule_StringAppendBuffer(NULL, key, ":", 1);
    RedisModule_StringAppendBuffer(NULL, key, metric, strlen(metric));
    return RedisModule_DictGet(VirtualKeyspaceMetadata,key,NULL);

}

double GetDBNumOnKey(const char *key) {

    return strtoll(key, NULL,0);

}

int MultidbMappingSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) return RedisModule_WrongArity(ctx);
    const char *dbnum = RedisModule_StringPtrLen(argv[1], NULL);
    size_t keylen;
    RedisModuleString *s = RedisModule_CreateString(ctx, "", 0);
    const char *key = RedisModule_StringPtrLen(argv[2], &keylen);
    RedisModule_StringAppendBuffer(ctx, s, dbnum, strlen(dbnum));
    RedisModule_StringAppendBuffer(ctx, s, ":", 1);
    RedisModule_StringAppendBuffer(ctx, s, key, keylen);

    RedisModuleCallReply *reply;
    reply = RedisModule_Call(ctx,"SET","sv",s,argv+3,argc-3);
    if (reply) {
        RedisModule_ReplyWithCallReply(ctx, reply);
        RedisModule_FreeCallReply(reply);
    } else {
        RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    return REDISMODULE_OK;
    
}

int MultidbMappingGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleString *s = RedisModule_CreateString(ctx, "", 0);
    const char *dbnum = RedisModule_StringPtrLen(argv[1], NULL);
    size_t keylen;
    const char *key = RedisModule_StringPtrLen(argv[2], &keylen);
    RedisModule_StringAppendBuffer(ctx, s, dbnum, strlen(dbnum));
    RedisModule_StringAppendBuffer(ctx, s, ":", 1);
    RedisModule_StringAppendBuffer(ctx, s, key, keylen);
    
    RedisModuleCallReply *reply;
    reply = RedisModule_Call(ctx,"GET","s",s);
    if (reply) {
        RedisModule_ReplyWithCallReply(ctx, reply);
        RedisModule_FreeCallReply(reply);
    }
    
    
    return REDISMODULE_OK;
    
}

int MultidbMappingInfoDB_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_ReplyWithArray(ctx,dbnum);
    for (int i = 0;i<dbnum;i++){
        long long *dbsizeval,*expiresizeval;
        char *id;
        dbsizeval = DictGetMetrics(i,"dbsize");
        expiresizeval = DictGetMetrics(i,"expiresize");
        id = DictGetMetrics(i,"id");
        RedisModuleString *s = RedisModule_CreateStringPrintf(ctx,"dbnum %d, id %s, dbsize %lld, expiresize %lld", i,id,*dbsizeval,*expiresizeval);
        RedisModule_ReplyWithString(ctx,s);
    }

    return REDISMODULE_OK;
    
}

/*  TODO: since this function may be triggered async, like asyncdeete, therefore it may require locking when 
updating metadata
*/
int KeyspaceChangeCallback(RedisModuleCtx *ctx, int type, const char *event,
                   RedisModuleString *key) {

    RedisModule_Log(ctx, "notice", "Got event type %d, event %s, key %s", type,
                  event, RedisModule_StringPtrLen(key, NULL));

    long dbnum = GetDBNumOnKey(RedisModule_StringPtrLen(key, NULL));

    char *metrics;
    long long diff;
    if (!strcmp(event, "add")) {
        metrics = "dbsize";
        diff = 1;
    } else if (!strcmp(event, "remove")) {
        metrics = "dbsize";
        diff = -1;
    } else if (!strcmp(event, "expire_add")) {
        metrics = "expiresize";
        diff = 1;
    } else if (!strcmp(event, "expire_remove")) {
        metrics = "expiresize";
        diff = -1;
    }else {
        return REDISMODULE_ERR;
    }

    long long *val = DictGetMetrics(dbnum,metrics);
    *val += diff;
    DictSetMetrics(dbnum,metrics,val);

    return REDISMODULE_OK;
}


int InitVirtualKeyspaceMetadata(long long dbnum){

    if (dbnum < 1) {
        dbnum = 1;
    }

    if (dbnum > 16) {
        dbnum = 16;
    }

    VirtualKeyspaceMetadata = RedisModule_CreateDict(NULL);

    for (int i = 0;i<dbnum;i++){

        char *id = (char*) RedisModule_Alloc(IDSIZE+1);
        genRandomData(id, IDSIZE);
        id[IDSIZE] = '\0';

        if (DictSetMetrics(i,"id",id) == REDISMODULE_ERR)
            return REDISMODULE_ERR;

        long long *dbsizeval = (long long*) RedisModule_Calloc(1,sizeof(long long));
        if (DictSetMetrics(i,"dbsize",dbsizeval) == REDISMODULE_ERR)
            return REDISMODULE_ERR;
        long long *expiresizeval = (long long*) RedisModule_Calloc(1,sizeof(long long));
        if (DictSetMetrics(i,"expiresize",expiresizeval) == REDISMODULE_ERR)
            return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {


    if (RedisModule_Init(ctx,"multidbmapping",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModule_Log(ctx,"notice","%d",argc);
    if (argc != 1) {
        return REDISMODULE_ERR;
    }

    long long numtmp;

    if (RedisModule_StringToLongLong(argv[0],&numtmp) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    dbnum = (long)numtmp;

    if (RedisModule_CreateCommand(ctx,"multidbmapping.set",
        MultidbMappingSet_RedisCommand, "write",
        2, 2, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    
    if (RedisModule_CreateCommand(ctx,"multidbmapping.get",
        MultidbMappingGet_RedisCommand, "readonly fast",
        2, 2, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"multidbmapping.infodb",
        MultidbMappingInfoDB_RedisCommand, "readonly fast",
        0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    
    if (RedisModule_SubscribeToKeyspaceEvents(ctx,
        REDISMODULE_NOTIFY_KEYSPACE_CHANGE, KeyspaceChangeCallback) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    if (InitVirtualKeyspaceMetadata(dbnum) != REDISMODULE_OK) 
        return REDISMODULE_ERR;
    

    return REDISMODULE_OK;
}
