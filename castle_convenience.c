#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "castle.h"

#define castle_key_header_size(_nr_dims) castle_object_btree_key_header_size(_nr_dims)

uint32_t
castle_build_key_len(castle_key             *key,
                     size_t                  buf_len,
                     int                     dims,
                     const int              *key_lens,
                     const uint8_t * const  *keys,
                     const uint8_t          *key_flags)
{
    int *lens = (int *)key_lens;

    if (!key_lens && dims)
    {
        if (!keys)
            abort();

        lens = alloca(dims * sizeof(lens[0]));
        for (int i = 0; i < dims; i++)
            lens[i] = strlen((const char *)keys[i]);
    }

    /* Workout the header size (including the dim_head array). */
    uint32_t needed = castle_key_header_size(dims);
    for (int i = 0; i < dims; i++)
        needed += lens[i];

    if (!key || buf_len == 0 || !keys || buf_len < needed)
        return needed;

    uint32_t payload_offset = castle_key_header_size(dims);
    key->length = needed - 4; /* Length doesn't include length field. */
    key->nr_dims = dims;
    *((uint64_t *)key->_unused) = 0;

    /* Go through all okey dimensions and write them in. */
    for(int i=0; i<dims; i++)
    {
        if (key_flags)
            key->dim_head[i] = KEY_DIMENSION_HEADER(payload_offset, key_flags[i]);
        else
            key->dim_head[i] = KEY_DIMENSION_HEADER(payload_offset, 0);
        memcpy((char *)key + payload_offset, keys[i], lens[i]);
        payload_offset += lens[i];
    }

    assert(payload_offset == needed);

    return needed;
}

uint32_t
castle_build_key(castle_key *key, size_t buf_len, int dims, const int *key_lens,
                 const uint8_t * const*keys, const uint8_t *key_flags) {
  uint32_t needed = castle_build_key_len(key, buf_len, dims, key_lens, keys, key_flags);
  if (needed <= buf_len)
    return 0;
  else
    return needed;
}

/* Note: We don't really need key_flags here. But, in future it possible to have a flag which
 * might has an effect on key internal size. */
uint32_t
castle_key_bytes_needed(int dims, const int *key_lens, const uint8_t * const*keys, const uint8_t *key_flags) {
  return castle_build_key(NULL, 0, dims, key_lens, keys, key_flags);
}

castle_key *
castle_malloc_key(int dims, const int *key_lens, const uint8_t * const*keys, const uint8_t *key_flags) {
  uint32_t len = castle_key_bytes_needed(dims, key_lens, keys, key_flags);
  castle_key *key = malloc(len);
  if (!key)
    return NULL;
  if (0 != castle_build_key(key, len, dims, key_lens, keys, key_flags))
    abort();
  return key;
}

static int make_key_buffer(castle_connection *conn, castle_key *key, uint32_t extra_space, char **key_buf_out, uint32_t *key_len_out) {
  int dims = key->nr_dims;
  int lens[dims];
  const uint8_t *keys[dims];
  uint8_t flags[dims];
  char *key_buf;
  uint32_t key_len;
  int err;

  for (int i = 0; i < dims; i++) {
    lens[i] = castle_key_elem_len(key, i);
    keys[i] = castle_key_elem_data(key, i);
    flags[i] = castle_key_elem_flags(key, i);
  }

  key_len = castle_key_bytes_needed(dims, lens, NULL, NULL);

  err = castle_shared_buffer_create(conn, &key_buf, key_len + extra_space);
  if (err)
    return err;

  {
    int r = castle_build_key((castle_key *)key_buf, key_len, dims, lens,
                             (const uint8_t *const *)keys, flags);
    if (r != 0)
      /* impossible */
      abort();
  }

  *key_buf_out = key_buf;
  *key_len_out = key_len;
  return 0;
}

static int make_2key_buffer(castle_connection *conn, castle_key *key1, castle_key *key2, char **key_buf_out, uint32_t *key1_len_out, uint32_t *key2_len_out) {
  int dims1 = key1->nr_dims;
  int dims2 = key2->nr_dims;
  int lens1[dims1];
  int lens2[dims2];
  const uint8_t *keys1[dims1];
  const uint8_t *keys2[dims2];
  uint8_t flags1[dims1];
  uint8_t flags2[dims2];
  char *key_buf;
  uint32_t key1_len;
  uint32_t key2_len;
  int err;

  for (int i = 0; i < dims1; i++) {
    lens1[i] = castle_key_elem_len(key1, i);
    keys1[i] = castle_key_elem_data(key1, i);
    flags1[i] = castle_key_elem_flags(key1, i);
  }

  for (int i = 0; i < dims2; i++) {
    lens2[i] = castle_key_elem_len(key2, i);
    keys2[i] = castle_key_elem_data(key2, i);
    flags2[i] = castle_key_elem_flags(key2, i);
  }

  key1_len = castle_key_bytes_needed(dims1, lens1, NULL, NULL);
  key2_len = castle_key_bytes_needed(dims2, lens2, NULL, NULL);

  err = castle_shared_buffer_create(conn, &key_buf, key1_len + key2_len);
  if (err)
    return err;

  {
    int r = castle_build_key((castle_key *)key_buf, key1_len, dims1, lens1,
                             (const uint8_t *const *)keys1, flags1);
    if (r != 0)
      /* impossible */
      abort();
  }

  {
    int r = castle_build_key((castle_key *)(key_buf + key1_len), key2_len, dims2,
                             lens2, (const uint8_t *const *)keys2, flags2);
    if (r != 0)
      /* impossible */
      abort();
  }

  *key_buf_out = key_buf;
  *key1_len_out = key1_len;
  *key2_len_out = key2_len;
  return 0;
}

/* These two functions are for copying keys out of shared buffers; keys supplied by the user are not contiguous */

/*
 * Assumes key is contiguous in memory
 * Note: bkey is always contiguous in memory.
 */
int castle_key_copy(c_vl_bkey_t *key, void *buf, uint32_t buf_len)
{
    if (!key || !buf)
        return -EINVAL;

    uint32_t key_len = castle_key_length(key);
    if (key_len > buf_len)
        return -ENOMEM;

    memcpy(buf, key, key_len);

    return 0;
}

#define max(_a, _b) ((_a) > (_b) ? (_a) : (_b))

int castle_get(castle_connection *conn,
               c_collection_id_t collection,
               castle_key *key,
               char **value_out, uint32_t *value_len_out)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf, *val_buf;
    int err = 0;
    uint32_t key_len;
    uint32_t val_len = PAGE_SIZE;
    char *value;

    err = make_key_buffer(conn, key, 0, &key_buf, &key_len);
    if (err) goto err0;

    err = castle_shared_buffer_create(conn, &val_buf, val_len);
    if (err) goto err1;

    castle_get_prepare(&req,
                       collection,
                       (castle_key *) key_buf,
                       key_len,
                       val_buf,
                       val_len,
                       CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err2;

    if (call.length > val_len)
    {
        castle_interface_token_t token;
        uint64_t val_len_64;
        uint32_t remaining, buf_len;
        char *buf;

        err = castle_big_get(conn, collection, key, &token, &val_len_64);
        if (err) goto err2;

        /* We can't assign val_len_64 to value_len_out unless val_len_64 fits */
        if (val_len_64 > UINT32_MAX) {
            err = -EFBIG;
            goto err1;
        }

        value = malloc(val_len_64);
        if (!value)
        {
            err = -ENOMEM;
            goto err2;
        }

        remaining = val_len_64;

        while (remaining > 0)
        {
            err = castle_get_chunk(conn, token, &buf, &buf_len);
            if (err)
            {
                free(value);
                goto err2;
            }

            memcpy(value + (val_len_64 - remaining), buf, buf_len);
            free(buf);
            remaining -= buf_len;
        }

        *value_len_out = val_len_64;
        *value_out = value;
    }
    else
    {
        assert(call.length <= UINT32_MAX);
        value = malloc(call.length);
        if (!value)
        {
            err = -ENOMEM;
            goto err2;
        }

        memcpy(value, val_buf, call.length);

        *value_len_out = call.length;
        *value_out = value;
    }

err2: castle_shared_buffer_destroy(conn, val_buf, val_len);
err1: castle_shared_buffer_destroy(conn, key_buf, key_len);
err0: return err;
}

int castle_replace(castle_connection *conn,
                   c_collection_id_t collection,
                   castle_key *key,
                   char *val, uint32_t val_len)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *buf;
    uint32_t key_len;
    int err = 0;

    err = make_key_buffer(conn, key, val_len, &buf, &key_len);
    if (err) goto err0;

    memcpy(buf + key_len, val, val_len);

    castle_replace_prepare(&req,
                           collection,
                           (castle_key *) buf,
                           key_len, buf + key_len,
                           val_len,
                           CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

err1: castle_shared_buffer_destroy(conn, buf, key_len + val_len);
err0: return err;
}

int castle_remove(castle_connection *conn,
                  c_collection_id_t collection,
                  castle_key *key)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf;
    uint32_t key_len;
    int err = 0;

    err = make_key_buffer(conn, key, 0, &key_buf, &key_len);
    if (err) goto err0;

    castle_remove_prepare(&req,
                          collection,
                          (castle_key *) key_buf,
                          key_len,
                          CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

err1: castle_shared_buffer_destroy(conn, key_buf, key_len);
err0: return err;
}

#define VALUE_INLINE(_type)     ((_type == CASTLE_VALUE_TYPE_INLINE) ||             \
                                 (_type == CASTLE_VALUE_TYPE_INLINE_COUNTER))

/**
 * Free allocated list of KVs.
 *
 * Take care not to loop backwards in case of KVs list where the Castle iterator
 * has more keys to return.
 */
void castle_kvs_free(struct castle_key_value_list *kvs)
{
    while (kvs)
    {
        struct castle_key_value_list *next = kvs->next;

        if (kvs->key)
            free(kvs->key);
        if (kvs->val->val)
            free(kvs->val->val);
        if (kvs->val)
            free(kvs->val);
        free(kvs);

        if (next > kvs)
            kvs = next;
        else
            kvs = NULL;
    }
}

/**
 * Process buf and return list of kvs.
 *
 * @param   [out]   kvs     List of keys,values returned by iterator.
 * @param   [out]   more    1 => Iterator has more keys to provide
 *                          0 => Iterator has completed
 *
 * @return  0       Success
 * @return -ENOMEM  Allocation failure
 * @return  *       Other failure
 */
static int castle_iter_process_kvs(castle_connection *conn,
                                   char *buf,
                                   struct castle_key_value_list **kvs,
                                   int *more)
{
    struct castle_key_value_list *curr, *prev, *head = NULL, *tail = NULL, *copy;
    int key_len, err = 0;

    if (more)
        *more = 0;

    curr = (struct castle_key_value_list *)buf;
    assert(curr);

    if (curr->key == NULL)
        /* No keys returned by iterator. */
        goto out;

    prev = curr;
    while (curr)
    {
        if (curr < prev)
        {
            /* We've reached the end of this set of results but the iterator
             * has more results to return.  Call ITER_NEXT. */
            if (more)
                *more = 1;

            goto out;
        }

        /* Otherwise we have a valid key to return. */

        /* Allocate local kvlist-entry. */
        key_len = castle_key_length(curr->key);
        copy = calloc(1, sizeof(*copy));
        if (!copy)
            goto alloc_fail;
        copy->key = malloc(key_len);
        if (!copy->key)
            goto alloc_fail;

        /* Fill key. */
        err = castle_key_copy(curr->key, copy->key, key_len);
        if (err)
            goto err;

        /* Fill val, do castle_get() if necessary. */
        copy->val = malloc(sizeof(*(copy->val)));
        if (!copy->val)
            goto alloc_fail;
        memcpy(copy->val, curr->val, sizeof(*(copy->val)));
        if (VALUE_INLINE(curr->val->type))
        {
            copy->val->val = malloc(copy->val->length);
            if (!copy->val->val)
                goto alloc_fail;
            memcpy(copy->val->val, curr->val->val, copy->val->length);
        }
        else
        {
            char *val;
            uint32_t val_len;
            err = castle_get(conn, curr->val->collection_id, curr->key, &val, &val_len);
            if (err)
                goto err;
            copy->val->length = val_len;
            copy->val->val = (uint8_t *)val;
            copy->val->type = CASTLE_VALUE_TYPE_INLINE; /* fake it for consumer */
        }

        if (!head)
            /* Start the list. */
            head = copy;
        else
            /* Add the newest item to the tail of the list. */
            tail->next = copy;

        tail = copy;
        prev = curr;
        curr = curr->next;
    }

out:
    *kvs = head;
    if (tail)
        tail->next = NULL;
    return 0;

alloc_fail:
    err = -ENOMEM;
err:
    castle_kvs_free(head);
    return err;
}

/**
 * Start an iterator and return a list of keys.
 *
 * @param   [out]   kvs     List of keys,values returned by iterator.
 * @param   [out]   more    1 => Iterator has more keys to provide
 *                          0 => Iterator has completed
 */
int castle_iter_start(castle_connection *conn,
                      c_collection_id_t collection,
                      castle_key *start_key,
                      castle_key *end_key,
                      castle_interface_token_t *token_out,
                      struct castle_key_value_list **kvs,
                      uint32_t buf_size,
                      int *more)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf, *ret_buf;
    uint32_t start_key_len;
    uint32_t end_key_len;
    int err = 0;

    *token_out = 0;

    err = make_2key_buffer(conn, start_key, end_key, &key_buf, &start_key_len, &end_key_len);
    if (err)
        goto err0;

    err = castle_shared_buffer_create(conn, &ret_buf, buf_size);
    if (err)
        goto err1;

    castle_iter_start_prepare(&req,                     /* request          */
                              collection,               /* collection       */
                              (castle_key *) key_buf,   /* start_key        */
                              start_key_len,            /* start_key_len    */
                              (castle_key *) ((unsigned long)key_buf + (unsigned long)start_key_len),   /* end_key */
                              end_key_len,              /* end_key_len      */
                              ret_buf,                  /* buffer           */
                              buf_size,                 /* buffer_len       */
                              CASTLE_RING_FLAG_NONE);   /* flags            */

    err = castle_request_do_blocking(conn, &req, &call);
    if (err)
        goto err2;

    err = castle_iter_process_kvs(conn, ret_buf, kvs, more);
    if (err)
        goto err2;

    *token_out = call.token;

    /* overflow into errs */

err2:
    castle_shared_buffer_destroy(conn, ret_buf, buf_size);
err1:
    castle_shared_buffer_destroy(conn, key_buf, start_key_len + end_key_len);
err0:
    return err;
}

/**
 * Continue an iterator and return a list of keys.
 *
 * @param   [out]   kvs     List of keys,values returned by iterator.
 * @param   [out]   more    1 => Iterator has more keys to provide
 *                          0 => Iterator has completed
 */
int castle_iter_next(castle_connection *conn,
                     castle_interface_token_t token,
                     struct castle_key_value_list **kvs,
                     uint32_t buf_size,
                     int *more)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *buf;
    int err = 0;

    *kvs = NULL;

    err = castle_shared_buffer_create(conn, &buf, buf_size);
    if (err)
        goto err0;

    castle_iter_next_prepare(&req, token, buf, buf_size, CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err)
        goto err1;

    err = castle_iter_process_kvs(conn, buf, kvs, more);
    if (err)
        goto err1;

    castle_shared_buffer_destroy(conn, buf, buf_size);

    return 0;

err1:
    castle_shared_buffer_destroy(conn, buf, buf_size);
err0:
    return err;
}

int castle_iter_finish(castle_connection *conn, castle_token token)
{
    struct castle_blocking_call call;
    castle_request_t req;

    castle_iter_finish_prepare(&req, token, CASTLE_RING_FLAG_NONE);

    return castle_request_do_blocking(conn, &req, &call);
}

/**
 * Perform a range query.
 *
 * @param   [out]   kvs     List of keys,values returned by iterator.
 * @param   [in]    limit   Maximum KVPs to return
 *                          0 => Unlimited
 */
int castle_getslice(castle_connection *conn,
                    c_collection_id_t collection,
                    castle_key *start_key,
                    castle_key *end_key,
                    struct castle_key_value_list **kvs_out,
                    uint32_t limit)
{
    castle_token token;
    int err, more;
    uint32_t count = 0;
    struct castle_key_value_list *head = NULL, *curr = NULL, *tail;

    err = castle_iter_start(conn, collection, start_key, end_key, &token, &curr, PAGE_SIZE, &more);
    if (err)
        goto err;

    head = tail = curr; /* start at curr */

process_loop:
    while (curr)
    {
        if (++count == limit)
        {
            /* Key limit reached.  Terminate iterator. */
            if (curr->next)
                castle_kvs_free(curr->next);
            curr->next = NULL;

            if (more)
                castle_iter_finish(conn, token);

            break;
        }

        tail = curr;
        curr = curr->next;
    }

    if (more && count < limit)
    {
        /* The iterator has more keys. */
        err = castle_iter_next(conn, token, &curr, PAGE_SIZE, &more);
        if (err)
            goto err;

        /* Append new batch to the list. */
        tail->next = curr;

        goto process_loop;
    }

    *kvs_out = head;

    return 0;

err:
    if (head)
        castle_kvs_free(head);

    castle_iter_finish(conn, token); /* iterator may already have finished */

    return err;
}

int castle_big_put(castle_connection *conn,
                   c_collection_id_t collection,
                   castle_key *key,
                   uint64_t val_length,
                   castle_interface_token_t *token_out)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf;
    uint32_t key_len;
    int err = 0;

    *token_out = 0;

    err = make_key_buffer(conn, key, 0, &key_buf, &key_len);
    if (err) goto err0;

    castle_big_put_prepare(&req,
                           collection,
                           (castle_key *) key_buf,
                           key_len,
                           val_length,
                           CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    *token_out = call.token;

err1: castle_shared_buffer_destroy(conn, key_buf, key_len);
err0: return err;
}

int castle_put_chunk(castle_connection *conn,
                     castle_interface_token_t token,
                     char *value,
                     uint32_t value_len)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *buf;
    int err = 0;

    err = castle_shared_buffer_create(conn, &buf, value_len);
    if (err) goto err0;

    memcpy(buf, value, value_len);

    castle_put_chunk_prepare(&req, token, buf, value_len, CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    err1: castle_shared_buffer_destroy(conn, buf, value_len);
    err0: return err;
}

int castle_big_get(castle_connection *conn,
                   c_collection_id_t collection,
                   castle_key *key,
                   castle_interface_token_t *token_out,
                   uint64_t *value_len_out)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf;
    uint32_t key_len;
    int err = 0;

    *token_out = 0;

    err = make_key_buffer(conn, key, 0, &key_buf, &key_len);
    if (err) goto err0;

    castle_big_get_prepare(&req,
                           collection,
                           (castle_key *) key_buf,
                           key_len,
                           CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    *token_out = call.token;
    *value_len_out = call.length;

err1: castle_shared_buffer_destroy(conn, key_buf, key_len);
err0: return err;
}

#define VALUE_LEN (1024 * 1024)
int castle_get_chunk(castle_connection *conn,
                     castle_interface_token_t token,
                     char **value_out,
                     uint32_t *value_len_out)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *buf;
    char *value;
    int err = 0;

    *value_out = NULL;

    err = castle_shared_buffer_create(conn, &buf, VALUE_LEN);
    if (err) goto err0;

    castle_get_chunk_prepare(&req, token, buf, VALUE_LEN, CASTLE_RING_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    value = malloc(VALUE_LEN);
    memcpy(value, buf, VALUE_LEN);

    *value_out = value;
    *value_len_out = call.length;

    err1: castle_shared_buffer_destroy(conn, buf, VALUE_LEN);
    err0: return err;
}

uint32_t castle_device_to_devno(const char *filename)
{
    struct stat st;
    if (0 != stat(filename, &st))
        return 0;

    return st.st_rdev;
}

static char **devnames = NULL;
static int devname_count = 0;

static void
alloc_devnames_to(int minor) {
  if (devname_count > minor)
    return;

  int old_devname_count = devname_count;
  devname_count = minor + 1;
  devnames = realloc(devnames, devname_count * sizeof(devnames[0]));
  for (int i = old_devname_count; i < devname_count; i++) {
    if (-1 == asprintf(&devnames[i], "/dev/castle-fs/castle-fs-%d", i))
      abort();
  }
}

const char *
castle_devno_to_device(uint32_t devno) {
  int minor = minor(devno);
  /* This is a bit wrong, but it'll do for now. castle-fs gets some
     arbitrary major assigned, and then names its devices based on the
     minor. We can find the path from that */

  alloc_devnames_to(minor);
  return devnames[minor];
}

int
castle_claim_dev(castle_connection *conn, const char *filename, castle_slave_uuid *id_out) {
  return castle_claim(conn, castle_device_to_devno(filename), id_out);
}

int
castle_attach_dev(castle_connection *conn, castle_version version, const char **filename_out) {
  uint32_t devno;
  int ret = castle_attach(conn, version, &devno);
  if (ret == 0)
    *filename_out = castle_devno_to_device(devno);
  return ret;
}

int
castle_detach_dev(castle_connection *conn, const char *filename) {
  return castle_detach(conn, castle_device_to_devno(filename));
}

int
castle_snapshot_dev(castle_connection *conn, const char *filename, castle_version *version_out) {
  return castle_snapshot(conn, castle_device_to_devno(filename), version_out);
}
