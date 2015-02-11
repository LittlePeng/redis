#include "redis.h"
#include <assert.h>

#define DEFAULT_MAX_FIELDS 5
#define DEFAULT_MAX_FIELD_LEN 5

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
    int16_t len; // msg length (0-65535)
    int8_t max_fields; //max field count (0-255)
    int8_t max_field_len; // max msg in a field (0-255)

    int64_t vmax;
    int64_t vmin;
    int64_t vmin_full;
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

static void enFqueue(fqueue *queue, int max_len, void *val, void *del){
    if(queue->free == 0){
        if(queue->count < max_len) {
            int recount = min(max_len, 2*queue->count);
            queue->data = zrealloc(queue->data, QUEUE_VALUE_SIZE*recount);
            queue->free = recount - queue->count;
        } else {
            memcpy(del, queue->data, QUEUE_VALUE_SIZE);
            memcpy(queue->data, queue->data + QUEUE_VALUE_SIZE, QUEUE_VALUE_SIZE*(queue->count - 1));
            queue->count--;
            queue->free++;
        }
    }
    assert(queue->free > 0);

    void *dest = queue->data + QUEUE_VALUE_SIZE*queue->count;
    memcpy(dest, val, QUEUE_VALUE_SIZE);
    queue->count++;
    queue->free--;
}

static int fqueue_search(void *queue, void *field) {
    return ((fqueue*)queue)->field == *(int64_t*)field;
}

static msgObject *newObj() {
    msgObject *obj = (msgObject*)zmalloc(sizeof(msgObject));
    obj->vmax = 0;
    obj->vmin = 0;
    obj->vmin_full = 0;
    obj->len = 0;
    obj->max_fields = DEFAULT_MAX_FIELDS;
    obj ->max_field_len = DEFAULT_MAX_FIELD_LEN;

    obj->aligned = listCreate();
    listSetFreeMethod(obj->aligned, zfree);
    listSetMatchMethod(obj->aligned, fqueue_search);
    obj->unaligned = NULL;

    return obj;
}

static fqueue *getFqueue(msgObject *obj, int64_t field) {
    listNode *node = listSearchKey(obj->aligned, &field);
    if(node != NULL)
        return node->value;
    if(listLength(obj->aligned) == obj->max_fields)
        return NULL;
    fqueue *queue = createFqueue(field);
    listAddNodeTail(obj->aligned, queue);
    return queue;
}

static void del_vector(msgObject *obj, vectorEntry *del){
    redisLog(REDIS_DEBUG, "evict vector:{vc:%lld, vp:%lld, val:%lld}",
            del->vcurrent, del->vprev, del->value);
    obj->len--;
    if(obj->vmin == del->vprev)
        obj->vmin = del->vcurrent;
    if(obj->vmin_full < del->vprev)
        obj->vmin_full = del->vprev;
}

static void try_align(msgObject *obj){
    redisLog(REDIS_DEBUG, "begin try_align ...");
}

static int appendMsg(msgObject *obj, int64_t field, vectorEntry *val) {
    obj->len++;
    if(is_new_obj(obj) || obj->vmax == val->vprev) {
        fqueue *queue = getFqueue(obj, field);
        if(queue == NULL)
            return 0;

        redisLog(REDIS_DEBUG, "enFqueue vector:{vc:%lld, vp:%lld, val:%lld}",
            val->vcurrent, val->vprev, val->value);

        vectorEntry del;
        del.vcurrent = 0;

        enFqueue(queue, obj->max_field_len, val, &del);

        obj->vmax = val->vcurrent;
        if(is_new_obj(obj)){
            obj->vmin = val->vprev;
            obj->vmin_full = val->vprev;
        }
        if(del.vcurrent > 0)
            del_vector(obj, &del);

        return obj->len;
    }
    else { //unaligned
        redisLog(REDIS_DEBUG, "unaligned");
        try_align(obj);
        return 0;
    }
}

static robj *dbAddmsgObject(redisClient *c, robj *key, msgObject *obj) {
        robj *val = createObject(REDIS_MSG, obj);
        val->encoding = REDIS_ENCODING_MSG;
        dbAdd(c->db, key, val);
        return val;
}

static robj *msgLookupWriteOrCreate(redisClient *c) {
    robj *key = c->argv[1];
    robj *o = lookupKeyWrite(c->db, key);

    if(o == NULL)
        o = dbAddmsgObject(c, key, newObj());
    else if(!checkType(c, o, REDIS_MSG))
        return NULL;

    return o;
}

void msgcreateCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *o = lookupKeyRead(c->db, key);

    if(o != NULL) {
        addReplyLongLong(c, 0);
        return;
    }
    long max_fields;
    long max_field_len;
    long expire_ttl;

    if(!string2l(c->argv[2]->ptr, sdslen(c->argv[2]->ptr), &max_fields) ||
            !string2l(c->argv[3]->ptr, sdslen(c->argv[3]->ptr), &max_field_len) ||
            !string2l(c->argv[4]->ptr, sdslen(c->argv[4]->ptr), &expire_ttl) ||
            max_fields <= 0 || max_fields > 255 ||
            max_field_len <=0 || max_field_len > 255){
        return addReply(c, shared.syntaxerr);
    }
    msgObject *obj = newObj();
    obj->max_fields = max_fields;
    obj->max_field_len = max_field_len;
    o = dbAddmsgObject(c, key, obj);

    if(expire_ttl > 0) {
        setExpire(c->db, key, mstime() + expire_ttl*1000);
    }

    addReplyLongLong(c, 1);
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

void msgappendxCommand(redisClient *c) {
    robj *o = lookupKeyRead(c->db, c->argv[1]);
    if(o == NULL) {
        addReplyLongLong(c, -1);
        return;
    }
    msgappendCommand(c);
}

void msglenCommand(redisClient *c){
    robj *o = lookupKeyRead(c->db, c->argv[1]);
    if(o == NULL){
        addReplyLongLong(c, -1);
    } else{
        if(checkType(c, o, REDIS_MSG)){
            addReplyLongLong(c, ((msgObject *)o->ptr)->len);
        }
    }
}

// result:
// -1: key notfound
//  1: version not enough
//  list
void msgfetchCommand(redisClient *c){
    robj *key = c->argv[1];
    int64_t vbegin;
    msgObject *obj;

    robj *o = lookupKeyRead(c->db,c->argv[1]);
    if(o == NULL){
        addReplyLongLong(c, -1);
        return;
    }
    if(checkType(c,o,REDIS_MSG)) return;

    obj = o->ptr;
    if(obj->vmax == vbegin){
        addReply(c, shared.emptymultibulk);
        return;
    }

    if(!string2ll(c->argv[2]->ptr, sdslen(c->argv[2]->ptr), &vbegin)) {
        addReply(c, shared.syntaxerr);
    }

    addReplyStatus(c, "TODO");
}

void msgrembyversionCommand(redisClient *c) {
    addReplyStatus(c, "TODO");
}

void freeMsgObject(robj *val) {
    msgObject *o = val->ptr;
    if(o->aligned != NULL)
        listRelease(o->aligned);
    if(o->unaligned != NULL)
        listRelease(o->unaligned);
    zfree(o);
}
