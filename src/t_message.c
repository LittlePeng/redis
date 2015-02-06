#include "redis.h"
#include <assert.h>

#define MAX_FIELDS 10
#define MAX_VECTOR 20

#define MAX_UNALIGN 3
#define QUEUE_INIT_COUNT 1
#define QUEUE_VALUE_SIZE sizeof(vectorEntry)

#define min(a, b)   (a) < (b) ? a : b
#define is_new_obj(obj) (obj->vmax == 0 && obj->vmin == 0)

typedef struct {
    int64_t field;
    int32_t count;
    int32_t free;
    char *data;
} fqueue;

typedef struct {
    int64_t vcurrent;
    int64_t vprev;
    int64_t value;
} vectorEntry;

typedef struct {
    int64_t field;
    vectorEntry vector;
} msgEntry;

typedef struct {
    int64_t vmax;
    int64_t vmin;
    int64_t vmin_full;
    int count;
    list *aligned;
    list *unaligned;
} msgObject;

static fqueue *createFqueue(int64_t field) {
    fqueue *queue = (fqueue*)zmalloc(sizeof(fqueue));
    queue->data = (char*)zmalloc(QUEUE_VALUE_SIZE  *QUEUE_INIT_COUNT);
    queue->field = field;
    queue->count = 0;
    queue->free = QUEUE_INIT_COUNT;
    return queue;
}

static void *enFqueue(fqueue *queue, void *val){
    void *del = NULL;

    if(queue->free == 0){
        if(queue->count < MAX_FIELDS) {
            int recount = min(MAX_VECTOR, 2  *queue->count);
            queue->data = zrealloc(queue->data, QUEUE_VALUE_SIZE  *recount);
            queue->free = recount - queue->count;
        } else {
            del = zmalloc(QUEUE_VALUE_SIZE);
            memcpy(del, queue->data, QUEUE_VALUE_SIZE);
            memcpy(queue->data, queue->data + QUEUE_VALUE_SIZE, QUEUE_VALUE_SIZE*(queue->count - 1));
            queue->count--;
            queue->free++;
        }
    }
    assert(queue->free > 0);

    void *dest = queue->data + QUEUE_VALUE_SIZE*queue->count;
    memcpy(dest, &val, QUEUE_VALUE_SIZE);
    queue->count++;
    queue->free--;
    return del;
}

static int fqueue_search(void *queue, void *field) {
    return ((fqueue*)queue)->field == *(int64_t*)field;
}

static msgObject *newObj() {
    msgObject *obj = (msgObject*)zmalloc(sizeof(msgObject));
    obj->vmax = 0;
    obj->vmin = 0;
    obj->vmin_full = 0;
    obj->aligned = listCreate();
    obj->unaligned = NULL;
    listSetFreeMethod(obj->aligned, zfree);
    listSetMatchMethod(obj->aligned, fqueue_search);

    return obj;
}

static fqueue *getFqueue(msgObject *obj, int64_t field) {
    listNode *node = listSearchKey(obj->aligned, &field);
    if(node != NULL)
        return node->value;
    if(listLength(obj->aligned) == MAX_FIELDS)
        return NULL;
    fqueue *queue = createFqueue(field);
    listAddNodeTail(obj->aligned, queue);
    return queue;
}

static void del_vector(msgObject *obj, vectorEntry *del){
    if(del != NULL){
        redisLog(REDIS_DEBUG, "evict vector: %lld", del->vcurrent);
        obj->count--;
        if(obj->vmin == del->vprev)
            obj->vmin = del->vcurrent;
        if(obj->vmin_full < del->vprev)
            obj->vmin_full = del->vprev;
        zfree(del);
    }
}

static void try_align(msgObject *obj){
    redisLog(REDIS_DEBUG, "begin try_align ...");
}

static int appendMsg(msgObject *obj, int64_t field, vectorEntry *val) {
    obj->count++;
    if(is_new_obj(obj) || obj->vmax == val->vprev) {
        fqueue *queue = getFqueue(obj, field);
        if(queue == NULL)
            return 0;
        vectorEntry *del = (vectorEntry*)enFqueue(queue, val);
        obj->vmax = val->vcurrent;
        if(is_new_obj(obj)){
            obj->vmin = val->vprev;
            obj->vmin_full = val->vprev;
        }
        if(del != NULL)
            del_vector(obj, del);
        return obj->count;
    }
    else { //unaligned
        redisLog(REDIS_DEBUG, "unaligned");
        try_align(obj);
        return 0;
    }
}

void freeMsgObject(robj *val) {
    //todo
}

static robj *msgLookupWriteOrCreate(redisClient *c) {
    robj *key = c->argv[1];

    robj *val = lookupKeyWrite(c->db, key);
    if(val == NULL){
        msgObject *obj = newObj();
        val = createObject(REDIS_MSG, obj);
        val->encoding = REDIS_ENCODING_MSG;
        dbAdd(c->db, key, val);
    } else {
        if(val->type != REDIS_MSG){
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    return val;
}

void msgappendCommand(redisClient *c) {
    robj *o = NULL;
    int64_t field;
    vectorEntry vector;

    if((o = msgLookupWriteOrCreate(c)) == NULL) return;
    if(!string2ll(c->argv[2]->ptr, sdslen(c->argv[2]->ptr), &field) ||
            !string2ll(c->argv[3]->ptr, sdslen(c->argv[3]->ptr), &vector.vcurrent) ||
            !string2ll(c->argv[4]->ptr, sdslen(c->argv[4]->ptr), &vector.vprev) ||
            !string2ll(c->argv[5]->ptr, sdslen(c->argv[5]->ptr), &vector.value)) {
       return addReply(c, shared.syntaxerr);
    }
    int len = appendMsg(o->ptr, field, &vector);
    addReplyLongLong(c, len);
}

void msglenCommand(redisClient *c){
    addReplyStatus(c, "hello");
}
void msgfetchCommand(redisClient *c){
    addReplyStatus(c, "hello");
}
