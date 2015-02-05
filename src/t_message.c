#include "redis.h"

#define MAX_FIELDS 50
#define MAX_VECTOR 20


void msgappendCommand(redisClient *c){
    addReplyStatus(c, "hello");
}

void msglenCommand(redisClient *c){
    addReplyStatus(c, "hello");
}
void msgfetchCommand(redisClient *c){
    addReplyStatus(c, "hello");
}
