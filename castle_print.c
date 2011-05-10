#include <stdio.h>
#include <ctype.h>
#include <string.h>

#include "castle.h"

/* Because it's awkward to keep track of the proper result when making
   multiple stdio calls, these two handle it - assuming that 'len'
   accumulates the length for return, -1 is returned on error, and... */

/* ...you're calling a function which returns length on success */
#define call_stdio_len(exp) ({ int r = (exp); if (r < 0) return -1; len += r; r; })
/* ...you're calling a function which returns the character written on success */
#define call_stdio_char(exp) ({ int r = (exp); if (r < 0) return -1; len += 1; r; })

static int
print_escaped(FILE *f, const char *str, unsigned int str_len) {
  int len = 0;
  for (unsigned int i = 0; i < str_len; i++) {
    char c = str[i];
    if (isprint(c) && c != ',' && c != '(' && c != ')') {
      call_stdio_char(fputc(c, f));
    }
    else {
      call_stdio_len(fprintf(f, "\\x%02hhx", (uint8_t)c));
    }
  }
  return len;
}

int
castle_print_key(FILE *f, castle_key *key) {
  int len = 0;
  call_stdio_len(fprintf(f, "("));
  for (unsigned int i = 0; i < castle_key_dims(key); i++) {
    if (i > 0)
      call_stdio_char(fputc(',', f));

    const uint8_t *elem = castle_key_elem_data(key, i);
    uint32_t elem_len = castle_key_elem_len(key, i);
    if (elem_len == 0)
      call_stdio_len(fprintf(f, "(invalid zero-length element)"));
    else
      call_stdio_len(print_escaped(f, (const char *)elem, elem_len));
  }
  call_stdio_char(fputc(')', f));
  return len;
}

static const char *command_names[] = {
  [CASTLE_RING_REPLACE] = "replace",
  [CASTLE_RING_BIG_PUT] = "big_put",
  [CASTLE_RING_PUT_CHUNK] = "put_chunk",
  [CASTLE_RING_GET] = "get",
  [CASTLE_RING_BIG_GET] = "big_get",
  [CASTLE_RING_GET_CHUNK] = "get_chunk",
  [CASTLE_RING_ITER_START] = "iter_start",
  [CASTLE_RING_ITER_NEXT] = "iter_next",
  [CASTLE_RING_ITER_FINISH] = "iter_finish",
  [CASTLE_RING_REMOVE] = "remove",
};

int
castle_print_request(FILE *f, castle_request *req, int print_values) {
  int len = 0;
  call_stdio_len(fprintf(f, "%s(call_id=%u, ", command_names[req->tag], req->call_id));
  switch (req->tag) {
  case CASTLE_RING_REPLACE:
    {
      char key_buf[req->replace.key_len];
      memcpy(key_buf, req->replace.key_ptr, req->replace.key_len);
      call_stdio_len(fprintf(f, "collection=%u, key=", req->replace.collection_id));
      call_stdio_len(castle_print_key(f, req->replace.key_ptr));
      if (print_values) {
        call_stdio_len(fprintf(f, ", value="));
        call_stdio_len(print_escaped(f, req->replace.value_ptr, req->replace.value_len));
      }
      break;
    }
  case CASTLE_RING_BIG_PUT:
    call_stdio_len(fprintf(f, "collection=%u, key=", req->big_put.collection_id));
    call_stdio_len(castle_print_key(f, req->big_put.key_ptr));
    call_stdio_len(fprintf(f, ", len=%llu", (long long unsigned)req->big_put.value_len));
    break;
  case CASTLE_RING_PUT_CHUNK:
    call_stdio_len(fprintf(f, "token=%u", req->put_chunk.token));
    if (print_values) {
      call_stdio_len(fprintf(f, ", data="));
      call_stdio_len(print_escaped(f, req->put_chunk.buffer_ptr, req->put_chunk.buffer_len));
    }
    break;
  case CASTLE_RING_GET:
    call_stdio_len(fprintf(f, "collection=%u, key=", req->get.collection_id));
    call_stdio_len(castle_print_key(f, req->get.key_ptr));
    call_stdio_len(fprintf(f, ", buffer=%p, buffer_len=%u", req->get.value_ptr, req->get.value_len));
    break;
  case CASTLE_RING_BIG_GET:
    call_stdio_len(fprintf(f, "collection=%u, key=", req->big_get.collection_id));
    call_stdio_len(castle_print_key(f, req->big_get.key_ptr));
    break;
  case CASTLE_RING_GET_CHUNK:
    call_stdio_len(fprintf(f, "token=%u, buffer=%p, buffer_len=%u", req->get_chunk.token, req->get_chunk.buffer_ptr, req->get_chunk.buffer_len));
    break;
  case CASTLE_RING_ITER_START:
    call_stdio_len(fprintf(f, "collection=%u, start_key=", req->iter_start.collection_id));
    call_stdio_len(castle_print_key(f, req->iter_start.start_key_ptr));
    call_stdio_len(fprintf(f, ", end_key="));
    call_stdio_len(castle_print_key(f, req->iter_start.end_key_ptr));
    call_stdio_len(fprintf(f, ", flags="));
    if (req->iter_start.flags & ~CASTLE_RING_ITER_FLAG_NO_VALUES)
      call_stdio_len(fprintf(f, "error(%llx)", (long long unsigned)req->iter_start.flags));
    else if (req->iter_start.flags & CASTLE_RING_ITER_FLAG_NO_VALUES)
      call_stdio_len(fprintf(f, "no_values"));
    else
      call_stdio_len(fprintf(f, "none"));
    break;
  case CASTLE_RING_ITER_NEXT:
    call_stdio_len(fprintf(f, "token=%u, buffer=%p, buffer_len=%u", req->iter_next.token, req->iter_next.buffer_ptr, req->iter_next.buffer_len));
    break;
  case CASTLE_RING_ITER_FINISH:
    call_stdio_len(fprintf(f, "token=%u", req->iter_next.token));
    break;
  case CASTLE_RING_REMOVE:
    call_stdio_len(fprintf(f, "collection=%u, key=", req->remove.collection_id));
    call_stdio_len(castle_print_key(f, req->remove.key_ptr));
    break;
  default:
    call_stdio_len(fprintf(f, "unknown(%x)", req->tag));
    break;
  }
  call_stdio_char(fputc(')', f));
  return len;
}

/* TODO: implement print_values */
int
castle_print_response(FILE *f, castle_response *resp, int print_values __attribute__((unused))) {
  return fprintf(f, "response(call_id=%u, err=%u, length=%llu, token=%u)", resp->call_id, resp->err, (long long unsigned)resp->length, resp->token);
}
