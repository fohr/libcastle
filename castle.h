#include <inttypes.h>
#include <stdio.h>

#include "castle_public.h"

#ifndef __CASTLE_FRONT_H__
#define __CASTLE_FRONT_H__

#define CASTLE_NODE   "/dev/castle-fs/control"

#ifdef __GNUC_STDC_INLINE__
#define ONLY_INLINE extern __inline__ __attribute__((__gnu_inline__))
#else
#define ONLY_INLINE extern __inline__
#endif

//#define TRACE
#ifdef TRACE
extern volatile unsigned long ops_counter;
extern volatile unsigned long selects_counter;
extern volatile unsigned long ioctls_counter;
#endif

/* deref the char buffer from a castle_buffer* */
#define CASTLE_BUF_GET(x) ((x)?(*(char**)(x)):(char*)NULL)

struct castle_front_connection;

struct s_castle_buffer
{
    /* buf must be first member for CASTLE_BUF_GET macro */
    char* buf;
    size_t buflen;
};

/* Type names are all over the place - create a consistent set of typedefs for the public API */
typedef struct castle_front_connection castle_connection;
typedef struct s_castle_buffer castle_buffer;
typedef c_vl_okey_t castle_key;
typedef c_vl_key_t castle_key_part;
typedef castle_request_t castle_request;
typedef castle_response_t castle_response;
typedef castle_interface_token_t castle_token;
typedef c_collection_id_t castle_collection;
typedef c_slave_uuid_t castle_slave_uuid;
typedef c_ver_t castle_version;
typedef c_da_t castle_da;
typedef c_env_var_t castle_env_var_id;

typedef void (*castle_callback)   (castle_connection *connection,
                                   castle_response *response, void *userdata);

struct castle_blocking_call
{
    int                      completed;
    int                      err;
    uint64_t                 length;
    castle_token             token;
};

int castle_shared_buffer_create   (castle_connection *conn,
                                   char **buffer,
                                   unsigned long size) __attribute__((warn_unused_result));
int castle_shared_buffer_destroy  (castle_connection *conn,
                                   char *buffer,
                                   unsigned long size);
int castle_shared_buffer_allocate (castle_connection *conn,
                                   castle_buffer **buffer_out, unsigned long size) __attribute__((warn_unused_result));
int castle_shared_buffer_release  (castle_connection *conn, castle_buffer* buffer);
int castle_connect                (castle_connection **conn) __attribute__((warn_unused_result));
void castle_disconnect            (castle_connection *conn);
void castle_free                  (castle_connection *conn);
void castle_request_send          (castle_connection *conn,
                                   castle_request *req,
                                   castle_callback *callbacks,
                                   void **userdatas,
                                   int reqs_count);
int castle_request_do_blocking    (castle_connection *conn,
                                   castle_request *req,
                                   struct castle_blocking_call *blocking_call);
int castle_request_do_blocking_multi(castle_connection *conn,
                                     castle_request *req,
                                     struct castle_blocking_call *blocking_call,
                                     int count);

int castle_print_key(FILE *f, castle_key *key);
int castle_print_request(FILE *f, castle_request *req, int print_values);
int castle_print_response(FILE *f, castle_response *resp, int print_values);

extern void castle_replace_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, char *value, uint32_t value_len) __attribute__((always_inline));
ONLY_INLINE void castle_replace_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, char *value, uint32_t value_len) {
  req->tag = CASTLE_RING_REPLACE;
  req->replace.collection_id = collection;
  req->replace.key_ptr = key;
  req->replace.key_len = key_len;
  req->replace.value_ptr = value;
  req->replace.value_len = value_len;
}

extern void castle_remove_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len) __attribute__((always_inline));
ONLY_INLINE void castle_remove_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len) {
  req->tag = CASTLE_RING_REMOVE;
  req->replace.collection_id = collection;
  req->replace.key_ptr = key;
  req->replace.key_len = key_len;
}

extern void castle_get_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, char *buffer, uint32_t buffer_len) __attribute__((always_inline));
ONLY_INLINE void castle_get_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, char *buffer, uint32_t buffer_len) {
  req->tag = CASTLE_RING_GET;
  req->get.collection_id = collection;
  req->get.key_ptr = key;
  req->get.key_len = key_len;
  req->get.value_ptr = buffer;
  req->get.value_len = buffer_len;
}

extern void castle_iter_start_prepare(castle_request *req, castle_collection collection, castle_key *start_key, uint32_t start_key_len, castle_key *end_key, uint32_t end_key_len, uint64_t flags) __attribute__((always_inline));
ONLY_INLINE void castle_iter_start_prepare(castle_request *req, castle_collection collection, castle_key *start_key, uint32_t start_key_len, castle_key *end_key, uint32_t end_key_len, uint64_t flags) {
  req->tag = CASTLE_RING_ITER_START;
  req->iter_start.collection_id = collection;
  req->iter_start.start_key_ptr = start_key;
  req->iter_start.start_key_len = start_key_len;
  req->iter_start.end_key_ptr = end_key;
  req->iter_start.end_key_len = end_key_len;
  req->iter_start.flags = flags;
}

extern void castle_iter_next_prepare(castle_request *req, castle_token token, char *buffer, uint32_t buffer_len) __attribute__((always_inline));
ONLY_INLINE void castle_iter_next_prepare(castle_request *req, castle_token token, char *buffer, uint32_t buffer_len) {
  req->tag = CASTLE_RING_ITER_NEXT;
  req->iter_next.token = token;
  req->iter_next.buffer_ptr = buffer;
  req->iter_next.buffer_len = buffer_len;
}

extern void castle_iter_finish_prepare(castle_request *req, castle_token token) __attribute__((always_inline));
ONLY_INLINE void castle_iter_finish_prepare(castle_request *req, castle_token token) {
  req->tag = CASTLE_RING_ITER_FINISH;
  req->iter_finish.token = token;
}

extern void castle_big_put_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, uint64_t value_len) __attribute__((always_inline));
ONLY_INLINE void castle_big_put_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, uint64_t value_len) {
  req->tag = CASTLE_RING_BIG_PUT;
  req->big_put.collection_id = collection;
  req->big_put.key_ptr = key;
  req->big_put.key_len = key_len;
  req->big_put.value_len = value_len;
}

extern void castle_put_chunk_prepare(castle_request *req, castle_token token, char *buffer, uint32_t buffer_len) __attribute__((always_inline));
ONLY_INLINE void castle_put_chunk_prepare(castle_request *req, castle_token token, char *buffer, uint32_t buffer_len) {
  req->tag = CASTLE_RING_PUT_CHUNK;
  req->put_chunk.token = token;
  req->put_chunk.buffer_ptr = buffer;
  req->put_chunk.buffer_len = buffer_len;
}

extern void castle_big_get_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len) __attribute__((always_inline));
ONLY_INLINE void castle_big_get_prepare(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len) {
  req->tag = CASTLE_RING_BIG_GET;
  req->big_get.collection_id = collection;
  req->big_get.key_ptr = key;
  req->big_get.key_len = key_len;
}

extern void castle_get_chunk_prepare(castle_request *req, castle_token token, char *buffer, uint32_t buffer_len) __attribute__((always_inline));
ONLY_INLINE void castle_get_chunk_prepare(castle_request *req, castle_token token, char *buffer, uint32_t buffer_len) {
  req->tag = CASTLE_RING_GET_CHUNK;
  req->get_chunk.token = token;
  req->get_chunk.buffer_ptr = buffer;
  req->get_chunk.buffer_len = buffer_len;
}

/* Assembles a castle_key at the given location, where buf_len is the
 * number of bytes allocated, and dims/key_lens/keys are the
 * parameters of the key. Returns zero on success, or the number of
 * bytes needed to build the key if it won't fit in buf_len bytes.
 *
 * When invoked with key == NULL, buf_len == 0, or keys == NULL, only
 * returns the necessary size
 *
 * When invoked with key_lens == NULL, uses strlen() to compute the lengths
 */
uint32_t castle_build_key(castle_key *key, size_t buf_len, int dims, const int *key_lens, const uint8_t * const*keys);
/* Variation on the theme, always returns the number of bytes needed. Success is when that value is <= buf_len. Mostly just here because java-castle wants it */
uint32_t castle_build_key_len(castle_key *key, size_t buf_len, int dims, const int *key_lens, const uint8_t * const*keys);
/* Returns the number of bytes needed for a key with these parameters */
uint32_t castle_key_bytes_needed(int dims, const int *key_lens, const uint8_t * const*keys) __attribute__((pure));

/* Convenience functions - some of these incur copies */

/* Call as castle_alloca_key("foo") or castle_alloca_key("foo", "bar"). Likely to compile down to nothing. Only really useful when calling the convenience functions */
#define castle_alloca_key(...) ({                                       \
      const char *ks[] = { __VA_ARGS__ };                               \
      uint32_t nr_dims = sizeof(ks) / sizeof(const char *);             \
      castle_key *okey = alloca(sizeof(*okey) + sizeof(okey->dims[0]) * nr_dims); \
      int i;                                                            \
      okey->nr_dims = nr_dims;                                          \
      for (i = 0; i < nr_dims; i++) {                                   \
        size_t len = strlen(ks[i]);                                     \
        castle_key_part *key = alloca(sizeof(*key) + len);              \
        memcpy(key->key, ks[i], len);                                   \
        key->length = len;                                              \
        okey->dims[i] = key;                                            \
      }                                                                 \
      okey; })

castle_key *castle_malloc_key(int dims, const int *key_lens, const uint8_t * const*keys) __attribute__((malloc));

extern uint32_t castle_key_dims(const castle_key *key) __attribute__((always_inline));
ONLY_INLINE uint32_t castle_key_dims(const castle_key *key) {
  return key->nr_dims;
}

extern uint32_t castle_key_elem_len(const castle_key *key, int elem) __attribute__((always_inline));
ONLY_INLINE uint32_t castle_key_elem_len(const castle_key *key, int elem) {
  return key->dims[elem]->length;
}

extern const uint8_t *castle_key_elem_data(const castle_key *key, int elem) __attribute__((always_inline));
ONLY_INLINE const uint8_t *castle_key_elem_data(const castle_key *key, int elem) {
  return key->dims[elem]->key;
}

int castle_get             (castle_connection *conn,
                            castle_collection collection,
                            castle_key *key,
                            char **value_out, uint32_t *value_len_out) __attribute__((warn_unused_result));
int castle_replace         (castle_connection *conn,
                            castle_collection collection,
                            castle_key *key,
                            char *val, uint32_t val_len);
int castle_remove          (castle_connection *conn,
                            castle_collection collection,
                            castle_key *key);
int castle_iter_start      (castle_connection *conn,
                            castle_collection collection,
                            castle_key *start_key,
                            castle_key *end_key,
                            castle_token *token_out) __attribute__((warn_unused_result));
int castle_iter_next       (castle_connection *conn,
                            castle_token token,
                            struct castle_key_value_list **kvs,
                            uint32_t buf_size) __attribute__((warn_unused_result));
int castle_iter_finish     (castle_connection *conn,
                            castle_token token);
int castle_getslice        (castle_connection *conn,
                            castle_collection collection,
                            castle_key *start_key,
                            castle_key *end_key,
                            struct castle_key_value_list **kvs_out,
                            uint32_t limit) __attribute__((warn_unused_result));
void castle_kvs_free       (struct castle_key_value_list *kvs_out);
int castle_big_put         (castle_connection *conn,
                            castle_collection collection,
                            castle_key *key,
                            uint64_t val_length,
                            castle_token *token_out);
int castle_put_chunk       (castle_connection *conn,
                            castle_token token,
                            char *value, uint32_t value_len);
int castle_big_get         (castle_connection *conn,
                            castle_collection collection,
                            castle_key *key,
                            castle_token *token_out, uint64_t *value_len_out) __attribute__((warn_unused_result));
int castle_get_chunk       (castle_connection *conn,
                            castle_token token,
                            char **value_out, uint32_t *value_len_out) __attribute__((warn_unused_result));

/* Control functions - ioctls */

#define C_TYPE_uint32 uint32_t
#define C_TYPE_uint64 uint64_t
#define C_TYPE_slave_uuid castle_slave_uuid
#define C_TYPE_version castle_version
#define C_TYPE_size size_t
#define C_TYPE_string const char *
#define C_TYPE_collection_id castle_collection
#define C_TYPE_env_var castle_env_var_id
#define C_TYPE_int int
#define C_TYPE_int32 int32_t
#define C_TYPE_da_id_t castle_da

#define CASTLE_IOCTL_0IN_0OUT(_id, _name)                                                         \
  int castle_##_id (castle_connection *conn);

#define CASTLE_IOCTL_1IN_0OUT(_id, _name, _arg_1_t, _arg_1)                                       \
  int castle_##_id (castle_connection *conn, C_TYPE_##_arg_1_t _arg_1);

#define CASTLE_IOCTL_1IN_1OUT(_id, _name, _arg_1_t, _arg_1, _ret_1_t, _ret)                       \
  int castle_##_id (castle_connection *conn, C_TYPE_##_arg_1_t _arg_1,                            \
                    C_TYPE_##_ret_1_t * _ret);

#define CASTLE_IOCTL_2IN_0OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2)                     \
  int castle_##_id (castle_connection *conn,                                                      \
                    C_TYPE_##_arg_1_t _arg_1, C_TYPE_##_arg_2_t _arg_2);                          \

#define CASTLE_IOCTL_3IN_1OUT(_id, _name,                                                         \
                              _arg_1_t, _arg_1, _arg_2_t, _arg_2, _arg_3_t, _arg_3,               \
                              _ret_1_t, _ret)                                                     \
  int castle_##_id (castle_connection *conn,                                                      \
                    C_TYPE_##_arg_1_t _arg_1, C_TYPE_##_arg_2_t _arg_2, C_TYPE_##_arg_3_t _arg_3, \
                    C_TYPE_##_ret_1_t * _ret);


#define CASTLE_IOCTLS                                                                             \
  CASTLE_IOCTL_1IN_1OUT(                                                                          \
        claim,                                                                                    \
        CASTLE_CTRL_CLAIM,                                                                        \
        uint32, dev, slave_uuid, id)                                                              \
  CASTLE_IOCTL_1IN_1OUT(                                                                          \
        attach,                                                                                   \
        CASTLE_CTRL_ATTACH,                                                                       \
        version, version, uint32, dev)                                                            \
  CASTLE_IOCTL_1IN_0OUT(                                                                          \
        detach,                                                                                   \
        CASTLE_CTRL_DETACH,                                                                       \
        uint32, dev)                                                                              \
  CASTLE_IOCTL_1IN_1OUT(                                                                          \
        snapshot,                                                                                 \
        CASTLE_CTRL_SNAPSHOT,                                                                     \
        uint32, dev, version, version)                                                            \
  CASTLE_IOCTL_3IN_1OUT(                                                                          \
        collection_attach,                                                                        \
        CASTLE_CTRL_COLLECTION_ATTACH,                                                            \
        version, version, string, name, size, name_length, collection_id, collection)             \
  CASTLE_IOCTL_1IN_0OUT(                                                                          \
        collection_detach,                                                                        \
        CASTLE_CTRL_COLLECTION_DETACH,                                                            \
        collection_id, collection)                                                                \
  CASTLE_IOCTL_1IN_1OUT(                                                                          \
        collection_snapshot,                                                                      \
        CASTLE_CTRL_COLLECTION_SNAPSHOT,                                                          \
        collection_id, collection, version, version)                                              \
  CASTLE_IOCTL_1IN_1OUT(                                                                          \
        create,                                                                                   \
        CASTLE_CTRL_CREATE,                                                                       \
        uint64, size, version, id)                                                                \
  CASTLE_IOCTL_1IN_1OUT(                                                                          \
        clone,                                                                                    \
        CASTLE_CTRL_CLONE,                                                                        \
        version, version, version, clone)                                                         \
  CASTLE_IOCTL_1IN_0OUT(                                                                          \
        delete_version,                                                                           \
        CASTLE_CTRL_DELETE_VERSION,                                                               \
        version, version)                                                                         \
  CASTLE_IOCTL_0IN_0OUT(                                                                          \
        init,                                                                                     \
        CASTLE_CTRL_INIT)                                                                         \
  CASTLE_IOCTL_2IN_0OUT(                                                                          \
        fault,                                                                                    \
        CASTLE_CTRL_FAULT,                                                                        \
        uint32, fault_id, uint32, fault_arg)                                                      \
  CASTLE_IOCTL_2IN_0OUT(                                                                          \
        slave_evacuate,                                                                           \
        CASTLE_CTRL_SLAVE_EVACUATE,                                                               \
        slave_uuid, id, uint32, force)                                                            \
  CASTLE_IOCTL_1IN_0OUT(                                                                          \
        slave_scan,                                                                               \
        CASTLE_CTRL_SLAVE_SCAN,                                                                   \
        uint32, id)                                                                               \
  CASTLE_IOCTL_1IN_0OUT(                                                                          \
        thread_priority,                                                                          \
        CASTLE_CTRL_THREAD_PRIORITY,                                                              \
        uint32, nice_value)                                                                       \
  CASTLE_IOCTL_1IN_0OUT(                                                                          \
        destroy_vertree,                                                                          \
        CASTLE_CTRL_DESTROY_VERTREE,                                                              \
        da_id_t, vertree_id)                                                                      \
  CASTLE_IOCTL_1IN_0OUT(                                                                          \
        vertree_compact,                                                                          \
        CASTLE_CTRL_VERTREE_COMPACT,                                                              \
        da_id_t, vertree_id)

#define PRIVATE_CASTLE_IOCTLS                                                                     \
  CASTLE_IOCTL_3IN_1OUT(                                                                          \
        environment_set,                                                                          \
        CASTLE_CTRL_ENVIRONMENT_SET,                                                              \
        env_var, var_id, string, var_str, size, var_len, int, ret)                                \
  CASTLE_IOCTL_2IN_0OUT(                                                                          \
        trace_setup,                                                                              \
        CASTLE_CTRL_TRACE_SETUP,                                                                  \
        string, dir_str, size, dir_len)                                                           \
  CASTLE_IOCTL_0IN_0OUT(                                                                          \
        trace_start,                                                                              \
        CASTLE_CTRL_TRACE_START)                                                                  \
  CASTLE_IOCTL_0IN_0OUT(                                                                          \
        trace_stop,                                                                               \
        CASTLE_CTRL_TRACE_STOP)                                                                   \
  CASTLE_IOCTL_0IN_0OUT(                                                                          \
        trace_teardown,                                                                           \
        CASTLE_CTRL_TRACE_TEARDOWN)                                                               \

CASTLE_IOCTLS
PRIVATE_CASTLE_IOCTLS

#undef CASTLE_IOCTL_0IN_0OUT
#undef CASTLE_IOCTL_1IN_0OUT
#undef CASTLE_IOCTL_1IN_1OUT
#undef CASTLE_IOCTL_2IN_0OUT
#undef CASTLE_IOCTL_3IN_1OUT

uint32_t castle_device_to_devno(const char *filename);
const char *castle_devno_to_device(uint32_t devno);

/* Convenience methods which don't use the hated device number */
int castle_claim_dev(castle_connection *conn, const char *filename, castle_slave_uuid *id_out);
int castle_attach_dev(castle_connection *conn, castle_version version, const char **filename_out) __attribute__((warn_unused_result));
int castle_detach_dev(castle_connection *conn, const char *filename);
int castle_snapshot_dev(castle_connection *conn, const char *filename, castle_version *version_out);

uint32_t castle_max_buffer_size(void);

/* Shared buffer pool */
typedef struct s_castle_shared_pool castle_shared_pool;
int castle_shared_pool_create(castle_connection* conn, size_t nsizes, size_t* sizes, size_t* quantities, castle_shared_pool** pool_out);
int castle_shared_pool_destroy(castle_shared_pool* pool);
int castle_shared_pool_lease(castle_shared_pool* pool, castle_buffer** buffer_out, unsigned long size);
int castle_shared_pool_release(castle_shared_pool* pool, castle_buffer* buffer, unsigned long size);

/* Collection utils */
int castle_collection_find(const char* name, castle_collection* collection_out);

#endif /* __CASTLE_FRONT_H__ */
