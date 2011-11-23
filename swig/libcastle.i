%module libcastle
%header %{
/* start of swig header from libcastle.i */
#define __attribute__(x)
#include <castle/castle.h>
#include <castle/castle_public.h>
typedef struct castle_blocking_call castle_blocking_call_t;
#define DEBUG_LIBCASTLE_SWIG 0
#define libcastle_debug(fmt, ...) do { if (DEBUG_LIBCASTLE_SWIG) printf(fmt, __VA_ARGS__ ); } while (0)
/* end of swig header from libcastle.i */
%}

/* these cause problems... */
%ignore castle_error_strings;
%ignore castle_print_request;
%ignore castle_print_key;
%ignore castle_print_response;

typedef char uint8_t;
typedef unsigned long long uint64_t;
typedef unsigned long uint32_t;
typedef uint32_t castle_version;
typedef c_collection_id_t castle_collection;
typedef struct castle_blocking_call castle_blocking_call_t;

#define c_ver_t castle_version
#define __attribute__(x)
%include <castle/castle.h>
%include <castle/castle_public.h>

%include cpointer.i
/* the following cannot be opaque; they are usually needed by test programs */
%pointer_class(castle_version, castle_version_p);
/* it's okay if the following are opaque */
%pointer_class(c_collection_id_t, c_collection_id_t_p);

%include cmalloc.i
%malloc(castle_request)
%free(castle_request)
%malloc(castle_blocking_call_t)
%free(castle_blocking_call_t)

%include cdata.i

%include carrays.i
%array_functions(long long, longlongArray);

%inline %{
/* start of swig inline from libcastle.i */
/* helper function to establish a castle connection in a more scripty way */
castle_connection * c_connect()
{
    castle_connection *conn;
    if(castle_connect(&conn))
        return NULL;
    return conn;
}
/* helper functions for shared buffer management; stick to void* so that they never appear as
   str in Python, which somehow then mangles the pointer (and yes i tried %pointer_class). */
void * c_shared_buffer_create(castle_connection * conn, unsigned int size)
{
    char * buf = NULL;
    int ret = castle_shared_buffer_create(conn, &buf, size);
    if (ret)
        return NULL;
    libcastle_debug("%s::Made buffer at %p of size %u\n", __FUNCTION__, buf, size);
    return (void *)buf;
}
void c_shared_buffer_destroy(castle_connection * conn, void * buf, unsigned int size)
{
    castle_shared_buffer_destroy(conn, (char *)buf, size);
}
/************************************** key helper **************************************/
#include <stdio.h>
#include <string.h>
/* this structure is opaque in Python, but is used fine here */
typedef struct{
    int           *key_lens;
    uint8_t      **key_ptrs;
    void          *key_buf;
    char          *tmp_key_buf;
    unsigned int   dims;
    uint32_t       buffer_capacity;
    uint32_t       current_buffer_usage;
} key_builder_t;
/* Initialize a key_builder_t that will be used to build up a multidimensional key */
key_builder_t * make_key_builder(unsigned int dims, void *key_buf, uint32_t key_buf_max)
{
    key_builder_t *builder = NULL;

    libcastle_debug("preparing to build a key with %u parameters at buffer %p\n", dims, key_buf);
    if(!(builder = malloc(sizeof(key_builder_t)))) goto err0;
    if(!(builder->key_lens = malloc(dims * sizeof(int)))) goto err1;
    memset(builder->key_lens, 0, dims*sizeof(int));
    if(!(builder->key_ptrs = malloc(dims * sizeof(uint8_t *)))) goto err2;
    memset(builder->key_ptrs, 0, dims*sizeof(uint8_t *));
    if(!(builder->tmp_key_buf = malloc(key_buf_max * sizeof(char)))) goto err3;


    builder->key_buf = (char *)key_buf;
    builder->dims = dims;
    builder->buffer_capacity = key_buf_max;
    builder->current_buffer_usage = 0;

    libcastle_debug("returning a key_builder_t at %p\n", builder);
    return builder;
err3:
    free(builder->tmp_key_buf);
err2:
    free(builder->key_lens);
err1:
    free(builder);
err0:
    return NULL;
}
/* Helper function to build up a key */
int build_key(key_builder_t *builder, unsigned int dim, char* key_dim, unsigned int key_dim_size)
{
    libcastle_debug("building dimension %u of key with builder %p\n", dim, builder);
    libcastle_debug("%s::key_dim_size %u, key_dim %p: %s (0x%x)\n", __FUNCTION__, key_dim_size, key_dim, key_dim, *key_dim);

    /* Check we haven't been given too many dimensions */
    if (dim >= builder->dims)
    {
        libcastle_debug("%s::Only expecting %u dims; user requested build of dimension %u.\n",
            __FUNCTION__, builder->dims, dim);
        return -1;
    }

    /* Update dimension state arrays */
    if (dim==0)
        builder->key_ptrs[0] = builder->tmp_key_buf;
    else
        builder->key_ptrs[dim] = builder->key_ptrs[dim-1] + (builder->key_lens[dim-1]*sizeof(uint8_t));
    builder->key_lens[dim] = key_dim_size;
    builder->current_buffer_usage += builder->key_lens[dim];

    /* Check we haven't been given too much data */
    if (builder->current_buffer_usage > builder->buffer_capacity)
    {
        libcastle_debug("%s::Expecting no mote than %lu bytes; user requested build of %lu worth of key data.\n",
            __FUNCTION__, builder->buffer_capacity, builder->current_buffer_usage);
        return -2;
    }

    /* Put key_dim into the buffer */
    memcpy(builder->key_ptrs[dim], key_dim, builder->key_lens[dim]);

    return 0;
}
castle_key * castle_key_ptr(key_builder_t *builder)
{
    castle_key * ck = (castle_key *)(builder->key_buf);
    return ck;
}
void free_key_builder(key_builder_t *builder)
{
    libcastle_debug("freeing builder %p\n", builder);
    free(builder->key_lens);
    free(builder->key_ptrs);
    free(builder->tmp_key_buf);
    free(builder);
}
#include <assert.h>
size_t finalize_key(key_builder_t *builder)
{
    int i;
    size_t total_size = 31;
    libcastle_debug("%s::running castle_build_key_len with params in builder %p\n",
        __FUNCTION__, builder);

    /*
    for(i=0; i<builder->dims; i++)
        printf("%s::key_ptrs[%u] = %p key_lens[%u] = %u first char = 0x%x\n",
            __FUNCTION__, i, builder->key_ptrs[i], i, builder->key_lens[i], *builder->key_ptrs[i]);
    printf("%s::", __FUNCTION__);
    for(i=0; i<total_size; i++)
        printf("(0x%x)", ((char *)(builder->key_buf))[i]);
    printf("\n");
    */

    total_size = castle_build_key_len(builder->key_buf,
                                      builder->buffer_capacity,
                                      builder->dims,
                                      builder->key_lens,
                                      (const uint8_t * const *)builder->key_ptrs,
                                      NULL);
    /*
    printf("%s::", __FUNCTION__);
    for(i=0; i<total_size; i++)
        printf("(0x%x)", ((char *)(builder->key_buf))[i]);
    printf("\n");
    */
    libcastle_debug("%s::castle_build_key_len with builder %p produced key_len %u\n",
        __FUNCTION__, builder, total_size);

    assert(total_size <= builder->buffer_capacity);
    free_key_builder(builder);
    return total_size;
}
/************************************** request helpers **************************************/
void c_get_prep(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, void *value, uint32_t value_len, uint8_t flags)
{
    libcastle_debug("%s::\n", __FUNCTION__);
    castle_get_prepare(req, collection, key, key_len, (char *)value, value_len, flags);
}
void c_replace_prep(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, void *value, uint32_t value_len, uint8_t flags)
{
    libcastle_debug("%s::\n", __FUNCTION__);
    castle_replace_prepare(req, collection, key, key_len, (char *)value, value_len, flags);
}
void c_add_prep(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, void *value, uint32_t value_len, uint8_t flags)
{
    libcastle_debug("%s::\n", __FUNCTION__);
    castle_counter_add_replace_prepare(req, collection, key, key_len, (char *)value, value_len, flags);
}
void c_set_prep(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, void *value, uint32_t value_len, uint8_t flags)
{
    libcastle_debug("%s::\n", __FUNCTION__);
    castle_counter_set_replace_prepare(req, collection, key, key_len, (char *)value, value_len, flags);
}
void c_rm_prep(castle_request *req, castle_collection collection, castle_key *key, uint32_t key_len, uint8_t flags)
{
    libcastle_debug("%s::\n", __FUNCTION__);
    castle_remove_prepare(req, collection, key, key_len, flags);
}

/************************************** debugging **************************************/
void hello_world()
{
    printf("%s::Hello World\n", __FUNCTION__);
}
void debug_print_void_pointer(void *p)
{
    printf("%s::%p\n", __FUNCTION__, p);
}
void debug_print_char_pointer(char *p)
{
    printf("%s::%p\n", __FUNCTION__, p);
}
void voidp_to_charp(void * vp, char ** cpp)
{
    *cpp = (char *)vp;
    printf("%s::%p -> %p\n", __FUNCTION__, vp, *cpp);
    debug_print_char_pointer(*cpp);
}
/* end of swig inline from libcastle.i */
%}
