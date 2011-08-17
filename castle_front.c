#include <sys/mman.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <inttypes.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#include "castle_public.h"
#include "castle.h"

#include "castle_private.h"

//#define DEBUG
#ifndef DEBUG
#define debug(_f, ...)    ((void)0)
#else
#define debug(_f, _a...)  (printf(_f, ##_a))
#endif

#define atomic_inc(x) ({int z __attribute__((unused)); z = __sync_fetch_and_add(x, 1); })
#define atomic_dec(x) ({int z __attribute__((unused)); z = __sync_fetch_and_sub(x, 1); })

static pthread_mutex_t blocking_call_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  blocking_call_cond  = PTHREAD_COND_INITIALIZER;

static void *castle_response_thread(void *data)
{
    castle_connection *conn = data;
    castle_response_t *resp;
    RING_IDX i, rp;
    fd_set readfds;
    int ret, more_to_do;
    int max_fd = conn->select_pipe[0] > conn->fd ? conn->select_pipe[0] : conn->fd;
    struct timeval last;
    struct timeval *select_timeout = NULL, select_tv;
    
    if (want_debug(conn, DEBUG_STATS)) {
      gettimeofday(&last, NULL);
      select_tv.tv_sec = 1;
      select_tv.tv_usec = 0;
      select_timeout = &select_tv;
    }

    while (!conn->response_thread_exit)
    {
        debug("pre-select %d\n", conn->fd);

        /* select destroys readfds, so must create it every time */
        FD_ZERO(&readfds);
        FD_SET(conn->fd, &readfds);
        FD_SET(conn->select_pipe[0], &readfds);

        ret = select(max_fd + 1, &readfds, (fd_set *)NULL, (fd_set *)NULL, select_timeout);
        if (ret <= 0)
        {
            debug("select returned %d\n", ret);
            continue;
        }

        if (conn->response_thread_exit)
            break;

#ifdef TRACE
        selects_counter++;
#endif

        debug("post-select\n");

        do {
          /* rsp_prod is written from the kernel, but in a strictly
             ordered way and it fits inside a cache line. Reading it
             at any point is safe */
          rp = conn->front_ring.sring->rsp_prod;

          /* This memory barrier is copied from Xen, which runs on
             powerpc, which has weak memory ordering. We only run on
             amd64, which has strong memory ordering: in particular,
             reads are never reordered with respect to other reads. We
             suspect that this is nothing more than a waste of a few
             hundred cycles. Revisit this if we have performance
             issues here - perhaps it can be safely removed */
          xen_rmb();

          /* rsp_cons is safe for concurrency; only read or written from this thread */
          for (i = conn->front_ring.rsp_cons; i != rp; i++) {
            resp = RING_GET_RESPONSE(&conn->front_ring, i);

            if (want_debug(conn, DEBUG_RESPS)) {
              flockfile(conn->debug_log);
              castle_print_response(conn->debug_log, resp, conn->debug_flags & DEBUG_VALUES);
              fprintf(conn->debug_log, "\n");
              fflush(conn->debug_log);
              funlockfile(conn->debug_log);
            }

            if(conn->callbacks[resp->call_id].callback)
                conn->callbacks[resp->call_id].callback(conn, resp, conn->callbacks[resp->call_id].data);

            bool has_token = conn->callbacks[resp->call_id].token != 0;
            unsigned int request_id = conn->callbacks[resp->call_id].token % CASTLE_STATEFUL_OPS;

            pthread_mutex_lock(&conn->free_mutex);
            list_add(&conn->callbacks[resp->call_id].list, &conn->free_callbacks);
            pthread_mutex_unlock(&conn->free_mutex);

            if (has_token) {
              assert(request_id < CASTLE_STATEFUL_OPS);
              assert(conn->outstanding_stateful_requests[request_id] > 0);
              int new = __sync_sub_and_fetch(&conn->outstanding_stateful_requests[request_id], 1);
              if (new == 0)
                atomic_inc(&conn->front_ring.reserved);
            }

            debug("Got response %d\n", resp->call_id);

#ifdef TRACE
            ops_counter++;
#endif
          }

          conn->front_ring.rsp_cons = i;
          assert(conn->front_ring.reserved <= RING_FREE_REQUESTS(&conn->front_ring));

          RING_FINAL_CHECK_FOR_RESPONSES(&conn->front_ring, more_to_do);

          if (want_debug(conn, DEBUG_STATS)) {
            struct timeval end;
            gettimeofday(&end, NULL);
            uint64_t delay_usec = (end.tv_sec - last.tv_sec) * 1000000 + (end.tv_usec - last.tv_usec);
            if (delay_usec > 1000000) {
              memcpy(&last, &end, sizeof(last));
              fprintf(conn->debug_log, "ring free requests %d, reserved %d\n", RING_FREE_REQUESTS(&conn->front_ring), conn->front_ring.reserved);
              fflush(conn->debug_log);
            }
          }

          pthread_mutex_lock(&conn->ring_mutex);
          pthread_cond_broadcast(&conn->ring_cond);
          pthread_mutex_unlock(&conn->ring_mutex);
        } while (more_to_do);
    }

    debug("castle_response_thread exiting...\n");

    pthread_mutex_lock(&conn->ring_mutex);
    conn->response_thread_running = 0;
    pthread_cond_broadcast(&conn->ring_cond);
    pthread_mutex_unlock(&conn->ring_mutex);

    return NULL;
}

int castle_shared_buffer_create(castle_connection *conn,
                                char **buffer_out, unsigned long size)
{
    void *buffer;
    buffer = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, conn->fd, 0);
    if (buffer == MAP_FAILED)
    {
        debug("Failed to map page %d\n", errno);
        return -errno;
    }

    // TODO keep track of buffers to free up

    *buffer_out = buffer;

    return 0;
}

int castle_shared_buffer_destroy(castle_connection *conn __attribute__((unused)),
                                 char *buffer, unsigned long size)
{
    int ret = munmap(buffer, size);

    if (ret == -1)
      return -errno;

    return 0;
}

int castle_shared_buffer_allocate(castle_connection *conn,
                                  castle_buffer **buffer_out, unsigned long size)
{
    castle_buffer* buffer = calloc(1, sizeof(*buffer));
    if(!buffer)
        return -ENOMEM;

    int rc = 0;
    if((rc = castle_shared_buffer_create(conn, &buffer->buf, size)))
    {
        debug("Failed to create shared buffer: %d\n", rc);
        free(buffer);
        return rc;
    }

    buffer->buflen = size;
    *buffer_out = buffer;
    return 0;
}

int castle_shared_buffer_release(castle_connection *conn, castle_buffer* buffer)
{
    int rc = castle_shared_buffer_destroy(conn, buffer->buf, buffer->buflen);
    if(!rc)
        free(buffer);
    return rc;
}

static int set_non_blocking(int fd)
{
    int flags;

    if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
        flags = 0;

    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int castle_fd(castle_connection *conn) {
  return conn->fd;
}

int castle_connect(castle_connection **conn_out)
{
    int err;
    castle_connection *conn = calloc(1, sizeof(*conn));

    *conn_out = NULL;

    if (!conn)
    {
        debug("Failed to malloc\n");
        err = -ENOMEM;
        goto err0;
    }

    conn->fd = open(CASTLE_NODE, O_RDWR);
    if (conn->fd == -1)
    {
        debug("Failed to open %s, errno=%d (\"%s\")\n",
            CASTLE_NODE, errno, strerror(errno));
        err = -errno;
        goto err1;
    }
    debug("Got fd %d\n", conn->fd);

    {
      int version = castle_protocol_version(conn);
      if (version != CASTLE_PROTOCOL_VERSION) {
        debug("Protocol version mismatch (kernel %d, libcastle %d)\n", version, CASTLE_PROTOCOL_VERSION);
        err = -ENOPROTOOPT;
        goto err2;
      }
    }

    conn->shared_ring = mmap(NULL, CASTLE_RING_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, conn->fd, 0);
    if (conn->shared_ring == MAP_FAILED)
    {
        debug("Failed to map page errno=%d (\"%s\")\n",
            errno, strerror(errno));
        err = -errno;
        goto err2;
    }
    debug("Got shared ring at address %p\n", conn->shared_ring);

    FRONT_RING_INIT(&conn->front_ring, conn->shared_ring, CASTLE_RING_SIZE, CASTLE_STATEFUL_OPS);

    conn->callbacks = malloc(sizeof(struct castle_front_callback) * RING_SIZE(&conn->front_ring));
    if (!conn->callbacks)
    {
        debug("Failed to malloc callbacks!");
        err = -ENOMEM;
        goto err3;
    }

    INIT_LIST_HEAD(&conn->free_callbacks);
    for (unsigned int i=0; i<RING_SIZE(&conn->front_ring); i++)
        list_add(&conn->callbacks[i].list, &conn->free_callbacks);

    err = pthread_mutex_init(&conn->free_mutex, NULL);
    if (err)
    {
        debug("Failed to create mutex, err=%d\n", err);
        err = -err;
        goto err4;
    }

    err = pthread_mutex_init(&conn->ring_mutex, NULL);
    if (err)
    {
        debug("Failed to create mutex, err=%d\n", err);
        err = -err;
        goto err5;
    }
    debug("Initialised mutex\n");

    err = pthread_cond_init(&conn->ring_cond, NULL);
    if (err)
    {
        debug("Failed to create condition, err=%d\n", err);
        err = -err;
        goto err6;
    }
    debug("Initialise condition\n");

    if (pipe(conn->select_pipe) == -1)
    {
        debug("Failed to create pipe to unblock select, errno=%d (\"%s\")",
            errno, strerror(errno));
        err = -errno;
        goto err7;
    }

    if (set_non_blocking(conn->select_pipe[0]) == -1)
    {
        debug("Failed to set non-block on fd %d, errno=%d (\"%s\")",
            conn->select_pipe[0], errno, strerror(errno));
        err = -errno;
        goto err8;
    }

    if (set_non_blocking(conn->select_pipe[1]) == -1)
    {
        debug("Failed to set non-block on fd %d, errno=%d (\"%s\")",
            conn->select_pipe[1], errno, strerror(errno));
        err = -errno;
        goto err8;
    }

    {
      const char *debug_env = getenv("CASTLE_DEBUG");
      if (debug_env) {
        const char *debug_file = getenv("CASTLE_DEBUG_FILE");
        const char *debug_fd = getenv("CASTLE_DEBUG_FD");
        if (debug_file)
          conn->debug_log = fopen(debug_file, "a");
        else if (debug_fd)
          conn->debug_log = fdopen(atoi(debug_fd), "a");
        else {
          int err_fd = dup(2);
          if (-1 != err_fd)
            conn->debug_log = fdopen(err_fd, "a");
        }
        char *buf = strdup(debug_env);
        char *buf_ptr = buf;
        char *tok_ptr;
        const char *token;
        while ((token = strtok_r(buf_ptr, ",", &tok_ptr))) {
          buf_ptr = NULL;

          if (0 == strcmp(token, "reqs"))
            conn->debug_flags |= DEBUG_REQS;
          else if (0 == strcmp(token, "values"))
            conn->debug_flags |= DEBUG_VALUES;
          else if (0 == strcmp(token, "stats"))
            conn->debug_flags |= DEBUG_STATS;
          else if (0 == strcmp(token, "resps"))
            conn->debug_flags |= DEBUG_RESPS;
        }
        free(buf);
      }
      else
        conn->debug_log = NULL;
    }

    conn->response_thread_running = 1;
    conn->response_thread_exit = 0;
    err = pthread_create(&conn->response_thread, NULL, castle_response_thread, conn);
    if (err)
    {
        debug("Failed to create response thread, err=%d\n", err);
        err = -err;
        goto err9;
    }
    debug("Response thread started\n");

    *conn_out = conn;

    return 0;

err9: fclose(conn->debug_log);
err8: close(conn->select_pipe[0]); close(conn->select_pipe[1]);
err7: pthread_cond_destroy(&conn->ring_cond);
err6: pthread_mutex_destroy(&conn->ring_mutex);
err5: pthread_mutex_destroy(&conn->free_mutex);
err4: free(conn->callbacks);
err3: munmap(conn->shared_ring, CASTLE_RING_SIZE);
err2: close(conn->fd);
err1: free(conn);
err0: return err;
}

void castle_disconnect(castle_connection *conn)
{
    ssize_t write_ret;

    if (!conn)
      return;

    if (conn->fd == -1)
      return;

    /* It doesn't matter that this flag is not protected by the lock
     * as long as the response thread eventually notices, and by
     * writing to the pipe the select will now never block, so it
     * should wake it up and notice eventually */
    conn->response_thread_exit = 1;
    write_ret = write(conn->select_pipe[1], "\0", 1);
    if (write_ret < 0)
        printf("write failed in castle_front_disconnect, error %d.\n", errno);

    /* Wait for the response thread to go away */
    pthread_mutex_lock(&conn->ring_mutex);
    while(conn->response_thread_running)
        pthread_cond_wait(&conn->ring_cond, &conn->ring_mutex);
    pthread_mutex_unlock(&conn->ring_mutex);

    // TODO: free buffers / wait for them to be free'd?

    pthread_mutex_lock(&conn->ring_mutex);
    munmap(conn->shared_ring, CASTLE_RING_SIZE);
    close(conn->fd);
    conn->fd = -1;
    pthread_mutex_unlock(&conn->ring_mutex);

    pthread_mutex_lock(&blocking_call_mutex);
    pthread_cond_broadcast(&blocking_call_cond);
    pthread_mutex_unlock(&blocking_call_mutex);

    close(conn->select_pipe[0]); close(conn->select_pipe[1]);
    
    if (conn->debug_log)
      fclose(conn->debug_log);
}

void castle_free(castle_connection *conn)
{
    if (!conn)
      return;

    if (conn->fd >= 0)
      castle_disconnect(conn);

    pthread_cond_destroy(&conn->ring_cond);
    pthread_mutex_destroy(&conn->ring_mutex);
    pthread_mutex_destroy(&conn->free_mutex);
    free(conn->callbacks);
    free(conn);
}

static castle_interface_token_t
get_request_token(castle_request_t *req) {
  switch (req->tag) {
  case CASTLE_RING_ITER_NEXT:
    return req->iter_next.token;
  case CASTLE_RING_ITER_FINISH:
    return req->iter_finish.token;
  case CASTLE_RING_PUT_CHUNK:
    return req->put_chunk.token;
  case CASTLE_RING_GET_CHUNK:
    return req->get_chunk.token;
  default:
    return 0;
  }
}

static bool
ring_full_for(castle_connection *conn, castle_request_t *req) {
  castle_interface_token_t token = get_request_token(req);

  if (token) {
    unsigned int x = token % CASTLE_STATEFUL_OPS;
    assert(x < CASTLE_STATEFUL_OPS);
    if (conn->outstanding_stateful_requests[x] == 0)
      return false;
  }

  int space = RING_FREE_REQUESTS(&conn->front_ring);
  int reserved = conn->front_ring.reserved;
  /* space < reserved when we've bumped the reserve count for a new
     reponse but haven't updated the ring yet */
  return space <= reserved;
}

typedef struct
{
    castle_callback callback;
    void* userdata;
    uint32_t remaining;
    uint32_t err;
} castle_batch_userdata;

static void castle_batch_callback(
        castle_connection* conn __attribute__((unused)),
        castle_response_t* resp,
        void* userdata
)
{
    castle_batch_userdata* data = (castle_batch_userdata*)userdata;

    // set the error and ignore any conflict
    if (!data->err && resp->err)
        __sync_val_compare_and_swap(&data->err, 0, resp->err);

    uint32_t remaining = __sync_sub_and_fetch(&data->remaining, 1);
    if (!remaining)
    {
        resp->err = data->err;
        data->callback(conn, resp, data->userdata);
        free(data);
    }
}

int castle_request_send_batch(
        castle_connection *conn,
        castle_request *req,
        castle_callback callback,
        void *userdata,
        int reqs_count
)
{
    int err = 0;
    castle_batch_userdata* data = NULL;
    castle_callback* callbacks = NULL;
    void** userdatas = NULL;

    if (reqs_count > 1)
    {
        data = calloc(1, sizeof(*data));
        if (!data)
        {
            err = -ENOMEM;
            goto out0;
        }
        data->callback = callback;
        data->userdata = userdata;
        data->remaining = reqs_count;

        userdatas = calloc(reqs_count, sizeof(*userdatas));
        if (!userdatas)
        {
            err = -ENOMEM;
            goto err0;
        }
        callbacks = calloc(reqs_count, sizeof(*callbacks));
        if (!callbacks)
        {
            err = -ENOMEM;
            goto err0;
        }

        for (int i = 0; i < reqs_count; ++i)
        {
            callbacks[i] = &castle_batch_callback;
            userdatas[i] = data;
        }
    } else
    {
        callbacks = &callback;
        userdatas = &userdata;
    }

    castle_request_send(conn, req, callbacks, userdatas, reqs_count);
    goto out1;

err0:
    free(data);
out1:
    if (reqs_count > 1)
    {
        free(callbacks);
        free(userdatas);
    }
out0:
    return err;
}

void castle_request_send(castle_connection *conn,
                               castle_request_t *req, castle_callback *callbacks,
                               void **datas, int reqs_count)
{
    // TODO check return codes?
    int notify, i=0, call_id;
    struct castle_front_callback *callback;

    /* This mutex is currently being abused for two distinct purposes,
       creating false scheduling hazards: it is both the condition
       variable mutex for ring_cond, which is used for signalling
       between the response thread and this function, and it is also
       used to protect the value req_prod_pvt from simultaneous
       executions of this function.

       TODO: break it apart into two mutexes

       TODO+1: change req_prod_pvt to be a lock-free atomic
       compare-and-set mechanism instead of using mutexes, so that
       multiple threads can write to the ring without context switches
    */
    pthread_mutex_lock(&conn->ring_mutex);

    while (i < reqs_count)
    {
      if (conn->fd < 0)
        break;

      /* RING_FULL is based on nr_ents (safe), rsp_cons (written only
         by the response thread and always within a cache line, hence
         safe), and req_prod_pvt (currently a concurrency hazard due
         to lack of atomic compare-and-set logic) */

      while (ring_full_for(conn, &req[i]))
            pthread_cond_wait(&conn->ring_cond, &conn->ring_mutex);

        /* Another RING_FULL hazard on req_prod_pvt */
      while (i < reqs_count && !ring_full_for(conn, &req[i]))
        {
            pthread_mutex_lock(&conn->free_mutex);
            assert(!list_empty(&conn->free_callbacks));
            callback = list_entry(conn->free_callbacks.next, struct castle_front_callback, list);
            list_del(&callback->list);
            pthread_mutex_unlock(&conn->free_mutex);

            call_id = callback - conn->callbacks;
            req[i].call_id = call_id;

            callback->callback = callbacks ? callbacks[i] : NULL;
            callback->data = datas ? datas[i] : NULL;
            callback->token = get_request_token(&req[i]);

            if (want_debug(conn, DEBUG_REQS)) {
              flockfile(conn->debug_log);
              castle_print_request(conn->debug_log, &req[i], conn->debug_flags & DEBUG_VALUES);
              fprintf(conn->debug_log, "\n");
              fflush(conn->debug_log);
              funlockfile(conn->debug_log);
            }

            if (callback->token) {
              unsigned int x = callback->token % CASTLE_STATEFUL_OPS;
              assert(x < CASTLE_STATEFUL_OPS);
              int old = __sync_fetch_and_add(&conn->outstanding_stateful_requests[x], 1);
              if (old == 0) {
                assert(conn->front_ring.reserved > 0);
                atomic_dec(&conn->front_ring.reserved);
              }
            }

            /* More req_prod_pvt hazards */
            castle_request_t *ring_req = RING_GET_REQUEST(&conn->front_ring, conn->front_ring.req_prod_pvt);
            debug("Putting request %d at position %d\n", call_id, conn->front_ring.req_prod_pvt);
            conn->front_ring.req_prod_pvt++;

            memcpy(ring_req, req + i, sizeof(*ring_req));

            i++;
        }

        /* This uses req_prod (safe due to strict ordering guarantees) and req_prod_pvt (hazard) */
        RING_PUSH_REQUESTS_AND_CHECK_NOTIFY(&conn->front_ring, notify);

        debug("notify=%d\n", notify);

        if (notify)
        {
#ifdef TRACE
            ioctls_counter++;
#endif
            ioctl(conn->fd, CASTLE_IOCTL_POKE_RING);
        }
    }

    pthread_mutex_unlock(&conn->ring_mutex);
}

static void castle_blocking_callback(castle_connection *conn __attribute__((unused)),
                                    castle_response_t *resp, void *data)
{
    struct castle_blocking_call *call = data;

    call->err = resp->err;
    call->token = resp->token;
    call->length = resp->length;

    pthread_mutex_lock(&blocking_call_mutex);
    call->completed = 1;
    pthread_cond_broadcast(&blocking_call_cond);
    pthread_mutex_unlock(&blocking_call_mutex);
}

int castle_request_do_blocking(castle_connection *conn,
                               castle_request_t *req,
                               struct castle_blocking_call *blocking_call)
{
    /*
     * Warning variables these will be on stack but used elsewhere, only safe as
     * this function sleeps until they are finished with (see castle_blocking_callback)
     */
    void *blocking_calls = blocking_call;
    castle_callback callback = &castle_blocking_callback;

    blocking_call->completed = 0;

    castle_request_send(conn, req, &callback, &blocking_calls, 1);

    pthread_mutex_lock(&blocking_call_mutex);
    while (conn->fd >= 0 && !blocking_call->completed)
        pthread_cond_wait(&blocking_call_cond, &blocking_call_mutex);
    pthread_mutex_unlock(&blocking_call_mutex);

    if (conn->fd < 0 && !blocking_call->completed) {
      blocking_call->completed = 1;
      blocking_call->err = EUNATCH;
    }

    return blocking_call->err;
}

int castle_request_do_blocking_multi(castle_connection *conn,
                                     castle_request_t *req,
                                     struct castle_blocking_call *blocking_call,
                                     int count)
{
    int i;
    void **blocking_calls;
    castle_callback *callbacks;

    blocking_calls = malloc(sizeof(struct castle_blocking_call *) * count);
    callbacks = malloc(sizeof(callbacks[0]) * count);

    for (i = 0; i < count; i++)
    {
        blocking_call[i].completed = 0;
        blocking_calls[i] = &blocking_call[i];
        callbacks[i] = castle_blocking_callback;
    }

    castle_request_send(conn, req, callbacks, blocking_calls, count);

    pthread_mutex_lock(&blocking_call_mutex);
    for (i = 0; i < count; i++)
    {
        while (conn->fd >= 0 && !blocking_call[i].completed)
            pthread_cond_wait(&blocking_call_cond, &blocking_call_mutex);
    }
    pthread_mutex_unlock(&blocking_call_mutex);

    free(blocking_calls);
    free(callbacks);

    for (i = 0; i < count; i++)
      if (conn->fd < 0 && !blocking_call[i].completed) {
        blocking_call[i].completed = 1;
        blocking_call[i].err = EUNATCH;
      }

    for (i = 0; i < count; i++)
        if (blocking_call[i].err)
            return blocking_call[i].err;

    return 0;
}

uint32_t
castle_max_buffer_size(void) {
  return 1048576;
}
