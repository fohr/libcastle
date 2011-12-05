#ifndef __CASTLE_PRIVATE_H__
#define __CASTLE_PRIVATE_H__

#include <stdio.h>
#include <pthread.h>

#include "ring.h"
#include "list.h"

#include "castle_public.h"

DEFINE_RING_TYPES(castle, castle_request_t, castle_response_t);

int castle_protocol_version(struct castle_front_connection *conn);

struct castle_front_callback
{
    struct list_head    list;
    castle_callback     callback;
    void               *data;
    castle_interface_token_t token;
};

struct castle_front_connection
{
    int                 fd; /* tests rely on this being the first field */
    castle_sring_t     *shared_ring;
    castle_front_ring_t front_ring;
    int                 next_call_id;
    /* pointer to array of callback pointers, corresponding to requests on ring */

    pthread_mutex_t     free_mutex;
    struct castle_front_callback *callbacks;
    struct list_head    free_callbacks;

    int outstanding_stateful_requests[CASTLE_STATEFUL_OPS];

    pthread_t           response_thread;
    int                 response_thread_exit;
    int                 response_thread_running;

    pthread_mutex_t     ring_mutex;
    pthread_cond_t      ring_cond;

    /* pipe fds to wake up select in the response thread */
    int                 select_pipe[2];

    int debug_flags;
    FILE *              debug_log;
} PACKED;

#define DEBUG_REQS 1
#define DEBUG_VALUES 2
#define DEBUG_STATS 4
#define DEBUG_RESPS 8

#define want_debug(conn, flag) (__builtin_expect(conn->debug_flags & flag, 0))

#endif /* __CASTLE_PRIVATE_H__ */
