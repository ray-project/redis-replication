/* Helloworld module -- A few examples of the Redis Modules API in the form
 * of commands showing how to accomplish common tasks.
 *
 * This module does not do anything useful, if not for a few commands. The
 * examples are designed in order to show the API.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

int rdbLoad(char *filename, void *rsi);

int loadAppendOnlyFile(char *filename);

int logging = 1;

int ReplicationWrite_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (logging) {
      RedisModule_ReplicateVerbatim(ctx);
      RedisModule_ReplyWithLongLong(ctx,RedisModule_GetSelectedDb(ctx));
      return REDISMODULE_OK;
    }
    RedisModuleKey *key;
    key = RedisModule_OpenKey(ctx,argv[1],REDISMODULE_WRITE);
    long long l;
    RedisModule_StringToLongLong(argv[2], &l);

    size_t len;
    char *str = RedisModule_StringDMA(key,&len,REDISMODULE_WRITE);

    char number[10] = {0};
    memcpy(&number[0], str, len);
    if (l > strtol(&number[0], NULL, 10)) {
      printf("dropping\n");
      RedisModule_StringSet(key,argv[2]);
    }
    RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx,RedisModule_GetSelectedDb(ctx));
    return REDISMODULE_OK;
}

int ReplicationLoad_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    rdbLoad("/tmp/redis.rdb", NULL);
    RedisModule_ReplyWithLongLong(ctx,RedisModule_GetSelectedDb(ctx));
    return REDISMODULE_OK;
}

int ReplicationReplay_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    logging = 0;
    loadAppendOnlyFile("appendonly.aof");
    RedisModule_ReplyWithLongLong(ctx,RedisModule_GetSelectedDb(ctx));
    return REDISMODULE_OK;
}

int ReplicationReady_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    logging = 0;
    RedisModule_ReplyWithLongLong(ctx,RedisModule_GetSelectedDb(ctx));
    return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"replication",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR)
      return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"replication.write",
        ReplicationWrite_RedisCommand,"write",1,1,1) == REDISMODULE_ERR)
      return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"replication.load",
        ReplicationLoad_RedisCommand,"write",1,1,1) == REDISMODULE_ERR)
      return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"replication.replay",
        ReplicationReplay_RedisCommand,"write",1,1,1) == REDISMODULE_ERR)
      return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"replication.ready",
        ReplicationReady_RedisCommand,"write",1,1,1) == REDISMODULE_ERR)
      return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
