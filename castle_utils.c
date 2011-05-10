#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>

#include "castle.h"
#include "castle_private.h"

/* get next castle_buffer* in free-list */
#define BUF_NEXT(x) (*(castle_buffer**)((x)->buf))
/* min size of pooled buffer */
#define MIN_SIZE sizeof(castle_buffer*)

#define max(a, b) ((a)>(b)?(a):(b))

typedef struct pool_node
{
    size_t size;
    castle_buffer* head;
} pool_node;

struct s_castle_shared_pool
{
    pool_node* free;
    size_t nsizes;

    pthread_mutex_t* lock;
    pthread_cond_t* sig;
    castle_connection* conn;
};

static int node_cmp(const void* a, const void* b)
{
    pool_node* l = (pool_node*)a, *r = (pool_node*)b;
    return l->size < r->size ? -1 : l->size > r->size ? 1 : 0;
}

static pool_node* find_size_locked(castle_shared_pool* pool, size_t size, bool nonempty)
{
    size_t first = 0;
    int last = pool->nsizes - 1;
    if(pool->free[last].size < size)
        return NULL;

    /* binary search for the least upper bound which contains the requested size */
    while(last >= (signed)first)
    {
        size_t test = first + (last-first)/2;
        if(pool->free[test].size > size)
            last = test - 1;
        else if(pool->free[test].size < size)
            first = test + 1;
        else
        {
            first = test;
            break;
        }
    }
    /* increase size until we find a non-empty free-list */
    if(nonempty)
        while(first < pool->nsizes && !pool->free[first].head) ++first;

    if(first < pool->nsizes)
        return &pool->free[first];

    /* all sufficiently large buffers are in use */
    return NULL;
}

int castle_shared_pool_lease(castle_shared_pool* pool, castle_buffer** buffer, unsigned long size)
{
    if(!pool || !buffer || *buffer || size > pool->free[pool->nsizes-1].size)
        return -EINVAL;

    pool_node* node = NULL;

    pthread_mutex_lock(pool->lock);
    while(!(node = find_size_locked(pool, size, true)))
        pthread_cond_wait(pool->sig, pool->lock);

    castle_buffer* head = node->head;
    node->head = BUF_NEXT(head);
    BUF_NEXT(head) = NULL;

    pthread_mutex_unlock(pool->lock);
    *buffer = head;
    return 0;
}

int castle_shared_pool_release(castle_shared_pool* pool, castle_buffer* buffer, __attribute__((unused)) unsigned long size)
{
    if(!pool || !buffer)
        return -EINVAL;

    pthread_mutex_lock(pool->lock);

    pool_node* node = find_size_locked(pool, buffer->buflen, false);
    BUF_NEXT(buffer) = node->head;
    node->head = buffer;

    pthread_cond_signal(pool->sig);
    pthread_mutex_unlock(pool->lock);
    return 0;
}

int castle_shared_pool_create(castle_connection* conn, size_t nsizes, size_t* sizes, size_t* quantities, castle_shared_pool** pool_out)
{
    if(!conn || !nsizes || !sizes || !quantities || !pool_out || *pool_out)
        return -EINVAL;

    castle_shared_pool* pool = (castle_shared_pool*)calloc(1, sizeof(*pool));
    pool->lock = (pthread_mutex_t*)calloc(1, sizeof(*pool->lock));
    pool->sig = (pthread_cond_t*)calloc(1, sizeof(*pool->sig));

    pthread_mutex_init(pool->lock, NULL);
    pthread_cond_init(pool->sig, NULL);
    pool->conn = conn;

    pool->free = (pool_node*)calloc(nsizes, sizeof(*pool->free));
    pool->nsizes = nsizes;

    for(size_t i = 0; i < nsizes; ++i)
    {
        size_t size = max(sizes[i], MIN_SIZE);
        pool->free[i].size = size;

        for(size_t n = 0; n < quantities[i]; ++n)
        {
            castle_buffer* node = NULL;
            int ret = castle_shared_buffer_allocate(conn, &node, size);
            if (ret)
            {
                castle_shared_pool_destroy(pool);
                return -ENOMEM;
            }
            BUF_NEXT(node) = pool->free[i].head;
            pool->free[i].head = node;
        }
    }

    qsort(pool->free, nsizes, sizeof(pool_node), node_cmp);

    *pool_out = pool;
    return 0;
}

int castle_shared_pool_destroy(castle_shared_pool* pool)
{
    if(!pool)
        return 0;

    for(size_t i = 0; i < pool->nsizes; ++i)
    {
        while(pool->free[i].head)
        {
            castle_buffer* head = pool->free[i].head;
            pool->free[i].head = BUF_NEXT(head);
            castle_shared_buffer_release(pool->conn, head);
        }
    }

    free(pool->free);

    pthread_cond_destroy(pool->sig);
    free(pool->sig);

    pthread_mutex_destroy(pool->lock);
    free(pool->lock);

    free(pool);
    return 0;
}

static int dir_exists(const char* path)
{
    struct stat attr;
    if(stat(path, &attr))
        return 0;
    return S_ISDIR(attr.st_mode);
}

static long filesize(FILE* file)
{
    if(!file)
        return -EINVAL;
    long cur = ftell(file);
    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    fseek(file, cur, SEEK_SET);
    return size;
}

static int castle_collection_name_get(const char* coll_path, char** name)
{
    if(!name || *name)
        return -EINVAL;
    int ret = 0;
    FILE* file = fopen(coll_path, "r");
    if(!file)
        return -errno;
    long size = filesize(file);
    *name = (char*)calloc(1, size + 1);
    if(!*name)
    {
        ret = -ENOMEM;
        goto out1;
    }
    char* p = *name;
    while(!feof(file) && p < *name + size)
        p += fread(p, *name + size - p, 1, file);

    p = *name;
    while(*p != '\n' && *p++);
    *p = '\0';

out1: fclose(file);
    return ret;
}

static const char* collections_path = "/sys/fs/castle-fs/collections";

int castle_collection_find(const char* name, castle_collection* coll)
{
    if(!name || !*name || !coll)
        return -EINVAL;
    int ret = 0;
    DIR* dir = opendir(collections_path);
    if(!dir)
    {
        ret = -errno;
        goto out1;
    }
    struct dirent* entry;
    char* cur_name = NULL;
    while((entry = readdir(dir)))
    {
        char path[PATH_MAX] = {0};
        snprintf(path, PATH_MAX, "%s/%s", collections_path, entry->d_name);
        if(dir_exists(path))
        {
            snprintf(path, PATH_MAX, "%s/%s/name", collections_path, entry->d_name);
            castle_collection_name_get(path, &cur_name);
            if(cur_name && 0==strcmp(cur_name, name))
            {
                *coll = strtol(entry->d_name, NULL, 16);
                goto out2;
            }
            free(cur_name); cur_name = NULL;
        }
    }

    ret = -ENOENT;

out2: free(cur_name);
out1: closedir(dir);
    return ret;
}
