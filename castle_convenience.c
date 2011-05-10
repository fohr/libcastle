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

uint32_t
castle_build_key_len(castle_key *key, size_t buf_len, int dims, const int *key_lens, const uint8_t * const*keys) {
  int *lens = (int *)key_lens;

  if (!key_lens && dims) {
    if (!keys)
      abort();
    lens = alloca(dims * sizeof(lens[0]));
    for (int i = 0; i < dims; i++)
      lens[i] = strlen((const char *)keys);
  }

  uint32_t needed = sizeof(castle_key) + sizeof(key->dims[0]) * dims + sizeof(*key->dims[0]) * dims;
  for (int i = 0; i < dims; i++)
    needed += lens[i];

  if (!key || buf_len == 0 || !keys || buf_len < needed)
    return needed;

  key->nr_dims = dims;
  char *ptr = (char *)key + sizeof(*key) + sizeof(key->dims[0]) * dims;
  for (int i = 0; i < dims; i++) {
    key->dims[i] = (c_vl_key_t *)ptr;
    key->dims[i]->length = lens[i];
    memcpy(key->dims[i]->key, keys[i], lens[i]);
    ptr += sizeof(*key->dims[i]) + lens[i];
  }

  assert(ptr - (char *)key == (int64_t)needed);

  return needed;
}

uint32_t
castle_build_key(castle_key *key, size_t buf_len, int dims, const int *key_lens, const uint8_t * const*keys) {
  uint32_t needed = castle_build_key_len(key, buf_len, dims, key_lens, keys);
  if (needed <= buf_len)
    return 0;
  else
    return needed;
}

uint32_t
castle_key_bytes_needed(int dims, const int *key_lens, const uint8_t * const*keys) {
  return castle_build_key(NULL, 0, dims, key_lens, keys);
}

castle_key *
castle_malloc_key(int dims, const int *key_lens, const uint8_t * const*keys) {
  uint32_t len = castle_key_bytes_needed(dims, key_lens, keys);
  castle_key *key = malloc(len);
  if (!key)
    return NULL;
  if (0 != castle_build_key(key, len, dims, key_lens, keys))
    abort();
  return key;
}

static int make_key_buffer(castle_connection *conn, castle_key *key, uint32_t extra_space, char **key_buf_out, uint32_t *key_len_out) {
  int dims = key->nr_dims;
  int lens[dims];
  uint8_t *keys[dims];
  char *key_buf;
  uint32_t key_len;
  int err;

  for (int i = 0; i < dims; i++) {
    lens[i] = key->dims[i]->length;
    keys[i] = key->dims[i]->key;
  }

  key_len = castle_key_bytes_needed(dims, lens, NULL);

  err = castle_shared_buffer_create(conn, &key_buf, key_len + extra_space);
  if (err)
    return err;

  {
    int r = castle_build_key((castle_key *)key_buf, key_len, dims, lens, (const uint8_t *const *)keys);
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
  uint8_t *keys1[dims1];
  uint8_t *keys2[dims2];
  char *key_buf;
  uint32_t key1_len;
  uint32_t key2_len;
  int err;

  for (int i = 0; i < dims1; i++) {
    lens1[i] = key1->dims[i]->length;
    keys1[i] = key1->dims[i]->key;
  }

  for (int i = 0; i < dims2; i++) {
    lens2[i] = key2->dims[i]->length;
    keys2[i] = key2->dims[i]->key;
  }

  key1_len = castle_key_bytes_needed(dims1, lens1, NULL);
  key2_len = castle_key_bytes_needed(dims2, lens2, NULL);

  err = castle_shared_buffer_create(conn, &key_buf, key1_len + key2_len);
  if (err)
    return err;

  {
    int r = castle_build_key((castle_key *)key_buf, key1_len, dims1, lens1, (const uint8_t *const *)keys1);
    if (r != 0)
      /* impossible */
      abort();
  }

  {
    int r = castle_build_key((castle_key *)(key_buf + key1_len), key2_len, dims2, lens2, (const uint8_t *const *)keys2);
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
 */
static int copy_key(c_vl_okey_t *key, void *buf, uint32_t key_len)
{
     c_vl_okey_t *new_key = buf;
     unsigned int i;

     memcpy(buf, key, key_len);

     if ((new_key->nr_dims * sizeof(c_vl_key_t *)) > (key_len - sizeof(c_vl_okey_t)))
     {
         return -EINVAL;
     }

     for (i=0; i < new_key->nr_dims; i++)
         new_key->dims[i] = (void *) (((unsigned long) new_key->dims[i]) -
           ((unsigned long) key) + ((unsigned long) buf));

     return 0;
}

#define max(_a, _b) ((_a) > (_b) ? (_a) : (_b))

/*
 * Assumes key is contiguous in memory
 */
static uint32_t get_key_len(c_vl_okey_t *key)
{

    uint32_t i;
    unsigned long end = 0;

    for (i=0; i < key->nr_dims; i++)
        end = max(end, ((unsigned long) key->dims[i]) + sizeof(c_vl_key_t) + key->dims[i]->length);

    return end - (unsigned long) key;
}

int castle_get(castle_connection *conn,
               collection_id_t collection,
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

    castle_get_prepare(&req, collection, (castle_key *) key_buf,
        key_len, val_buf, val_len);

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
                   collection_id_t collection,
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

    castle_replace_prepare(&req, collection, (castle_key *) buf,
        key_len, buf + key_len, val_len);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

err1: castle_shared_buffer_destroy(conn, buf, key_len + val_len);
err0: return err;
}

int castle_remove(castle_connection *conn,
                  collection_id_t collection,
                  castle_key *key)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf;
    uint32_t key_len;
    int err = 0;

    err = make_key_buffer(conn, key, 0, &key_buf, &key_len);
    if (err) goto err0;

    castle_remove_prepare(&req, collection,
        (castle_key *) key_buf,  key_len);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

err1: castle_shared_buffer_destroy(conn, key_buf, key_len);
err0: return err;
}

int castle_iter_start(castle_connection *conn,
                      collection_id_t collection,
                      castle_key *start_key,
                      castle_key *end_key,
                      castle_interface_token_t *token_out)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf;
    uint32_t start_key_len;
    uint32_t end_key_len;
    int err = 0;

    *token_out = 0;

    err = make_2key_buffer(conn, start_key, end_key, &key_buf, &start_key_len, &end_key_len);
    if (err) goto err0;

    castle_iter_start_prepare(&req, collection,
        (castle_key *) key_buf,  start_key_len,
        (castle_key *) ((unsigned long)key_buf + (unsigned long)start_key_len),  end_key_len,
        CASTLE_RING_ITER_FLAG_NONE);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    *token_out = call.token;

err1: castle_shared_buffer_destroy(conn, key_buf, start_key_len + end_key_len);
err0: return err;
}

void castle_kvs_free(struct castle_key_value_list *kvs)
{
    while (kvs)
    {
        struct castle_key_value_list *next = kvs->next;

        if (kvs->key) free(kvs->key);
        if (kvs->val->val) free(kvs->val->val);
        if (kvs->val) free(kvs->val);
        free(kvs);

        kvs = next;
    }
}

int castle_iter_next(castle_connection *conn,
                     castle_interface_token_t token,
                     struct castle_key_value_list **kvs,
                     uint32_t buf_size)
{
    struct castle_blocking_call call;
    castle_request_t req;
    struct castle_key_value_list *head = NULL, *tail = NULL, *copy = NULL, *curr = NULL;
    char *buf;
    int err = 0;

    *kvs = NULL;

    err = castle_shared_buffer_create(conn, &buf, buf_size);
    if (err) goto err0;

    castle_iter_next_prepare(&req, token, buf, buf_size);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    curr = (struct castle_key_value_list *)buf;

    // NULL first key means no entries
    if (curr->key == NULL)
    {
        head = NULL;
    }
    else
    {
        while (curr != NULL)
        {
            unsigned long key_len = get_key_len(curr->key);

            copy = calloc(1, sizeof(*copy));
            if (!copy)
            {
                err = -ENOMEM;
                goto err2;
            }

            copy->key = malloc(key_len);
            if (!copy->key)
            {
                err = -ENOMEM;
                goto err2;
            }
            err = copy_key(curr->key, copy->key, key_len);
            if (err) goto err2;

            copy->val = malloc(sizeof(*(copy->val)));
            if (!copy->val)
            {
                err = -ENOMEM;
                goto err2;
            }
            memcpy(copy->val, curr->val, sizeof(*(copy->val)));

            if (curr->val->type & CVT_TYPE_INLINE)
            {
                copy->val->val = malloc(copy->val->length);
                if (!copy->val->val)
                {
                    err = -ENOMEM;
                    goto err2;
                }
                memcpy(copy->val->val, curr->val->val, copy->val->length);
            }
            else
            {
                char *val;
                uint32_t val_len;
                err = castle_get(conn, curr->val->collection_id, curr->key, &val, &val_len);
                if (err)
                    goto err2;
                copy->val->length = val_len;
                copy->val->val = (uint8_t *)val;

                /* pretend it is inline since the value is now 'inline' */
                copy->val->type = CVT_TYPE_INLINE;
            }

            if (!head)
                head = copy;
            else
                tail->next = copy;

            tail = copy;
            curr = curr->next;
        }
    }

    *kvs = head;

    castle_shared_buffer_destroy(conn, buf, buf_size);

    return 0;

err2: castle_kvs_free(head);
err1: castle_shared_buffer_destroy(conn, buf, buf_size);
err0: return err;
}

int castle_iter_finish(castle_connection *conn,
                       castle_token token)
{
    struct castle_blocking_call call;
    castle_request_t req;
    int err = 0;

    castle_iter_finish_prepare(&req, token);

    err = castle_request_do_blocking(conn, &req, &call);

    return err;
}

// 'limit' means the maximum number of values to retrieve. 0 means unlimited.
int castle_getslice(castle_connection *conn,
                    collection_id_t collection,
                    castle_key *start_key,
                    castle_key *end_key,
                    struct castle_key_value_list **kvs_out,
                    uint32_t limit)
{
    castle_token token;
    int ret;
    uint32_t count = 0;
    struct castle_key_value_list *head = NULL, *tail = NULL, *curr = NULL;

    ret = castle_iter_start(conn, collection, start_key,
        end_key, &token);
    if (ret) goto err0;

    while (!ret  && (limit == 0 || count < limit))
    {
        ret = castle_iter_next(conn, token, &curr, PAGE_SIZE);
        if (ret) goto err1;

        if (!curr)
            break;

        if (!head)
            head = curr;
        else
            tail->next = curr;

        while (curr)
        {
            count++;
            if (count == limit)
            {
                if (curr->next)
                    castle_kvs_free(curr->next);
                curr->next = NULL;
                break;
            }
            tail = curr; // tail will be one behind curr
            curr = curr->next;
        }
    }

    ret = castle_iter_finish(conn, token);
    if (ret)
        goto err1;

    *kvs_out = head;

    return ret;

err1:
    if (head)
        castle_kvs_free(head);
err0:
    return ret;
}

int castle_big_put         (castle_connection *conn,
                            collection_id_t collection,
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

    castle_big_put_prepare(&req, collection,
        (castle_key *) key_buf,  key_len, val_length);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    *token_out = call.token;

err1: castle_shared_buffer_destroy(conn, key_buf, key_len);
err0: return err;
}

int castle_put_chunk       (castle_connection *conn,
                            castle_interface_token_t token,
                            char *value, uint32_t value_len)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *buf;
    int err = 0;

    err = castle_shared_buffer_create(conn, &buf, value_len);
    if (err) goto err0;

    memcpy(buf, value, value_len);

    castle_put_chunk_prepare(&req, token, buf, value_len);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    err1: castle_shared_buffer_destroy(conn, buf, value_len);
    err0: return err;
}

int castle_big_get         (castle_connection *conn,
                            collection_id_t collection,
                            castle_key *key,
                            castle_interface_token_t *token_out, uint64_t *value_len_out)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *key_buf;
    uint32_t key_len;
    int err = 0;

    *token_out = 0;

    err = make_key_buffer(conn, key, 0, &key_buf, &key_len);
    if (err) goto err0;

    castle_big_get_prepare(&req, collection,
        (castle_key *) key_buf,  key_len);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    *token_out = call.token;
    *value_len_out = call.length;

err1: castle_shared_buffer_destroy(conn, key_buf, key_len);
err0: return err;
}

#define VALUE_LEN (1024 * 1024)
int castle_get_chunk       (castle_connection *conn,
                            castle_interface_token_t token,
                            char **value_out, uint32_t *value_len_out)
{
    struct castle_blocking_call call;
    castle_request_t req;
    char *buf;
    char *value;
    int err = 0;

    *value_out = NULL;

    err = castle_shared_buffer_create(conn, &buf, VALUE_LEN);
    if (err) goto err0;

    castle_get_chunk_prepare(&req, token, buf, VALUE_LEN);

    err = castle_request_do_blocking(conn, &req, &call);
    if (err) goto err1;

    value = malloc(VALUE_LEN);
    memcpy(value, buf, VALUE_LEN);

    *value_out = value;
    *value_len_out = call.length;

    err1: castle_shared_buffer_destroy(conn, buf, VALUE_LEN);
    err0: return err;
}

uint32_t
castle_device_to_devno(const char *filename) {
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
