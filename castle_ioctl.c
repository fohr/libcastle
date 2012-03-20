#include <sys/ioctl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <linux/fs.h>

#include "castle.h"
#include "castle_public.h"

#include "castle_private.h"

/* Define string array for userspace error-codes. */
#undef CASTLE_ERROR_CODE
#define CASTLE_ERROR_CODE(err_no, err_code, err_str)  [err_no] = err_str,
const char *castle_error_strings[CASTLE_ERROR_MAX+1] =
{
    CASTLE_ERRORS
};

const char * castle_error_code_to_str(int error_code)
{
    if ((error_code < 0) || (error_code >= CASTLE_ERROR_MAX))
        error_code = CASTLE_ERROR_MAX;

    return castle_error_strings[error_code];
}

int castle_protocol_version(struct castle_front_connection *conn) {
    struct castle_control_ioctl ctl;
    int ret;
    ctl.cmd = CASTLE_CTRL_PROTOCOL_VERSION;

    ret = ioctl(conn->fd, CASTLE_CTRL_PROTOCOL_VERSION_IOCTL, &ctl);
    if (__builtin_expect(conn->debug_log != NULL, 0)) {
      fprintf(conn->debug_log, "protocol_version() = %d, %d\n", ret, ctl.protocol_version.version);
      fflush(conn->debug_log);
    }
    if (ret)
      return -1;

    return ctl.protocol_version.version;
}

int castle_merge_start(struct castle_front_connection *conn, c_merge_cfg_t merge_cfg,
                       c_merge_id_t *merge_id)
{
    struct castle_control_ioctl ctl;
    int ret;
    ctl.cmd = CASTLE_CTRL_MERGE_START;
    ctl.merge_start.merge_cfg = merge_cfg;

    ret = ioctl(conn->fd, CASTLE_CTRL_MERGE_START_IOCTL, &ctl);
    if (__builtin_expect(conn->debug_log != NULL, 0))
    {
        unsigned int i;

        fprintf(conn->debug_log, "merge_start(nr_arrays = %u, arrays = ",
                                  merge_cfg.nr_arrays);
        for (i=0; i<merge_cfg.nr_arrays; i++)
            fprintf(conn->debug_log, "[0x%lx]", merge_cfg.arrays[i]);

        for (i=0; i<merge_cfg.nr_data_exts; i++)
            fprintf(conn->debug_log, "[0x%lx]", merge_cfg.data_exts[i]);

        fprintf(conn->debug_log, ", metadata_ext_type = %u, met_ext_type = %u\n",
                                  merge_cfg.metadata_ext_type,
                                  merge_cfg.data_ext_type);
        fflush(conn->debug_log);
    }

    if (ret)    return ret;

    *merge_id = ctl.merge_start.merge_id;

    return ctl.merge_start.ret;
}

#define C_PRINTF_uint8 "%u"
#define C_PRINTF_uint32 "%u"
#define C_PRINTF_uint64 "%lu"
#define C_PRINTF_slave_uuid "%u"
#define C_PRINTF_version "%u"
#define C_PRINTF_size "%zu"
#define C_PRINTF_string "%s"
#define C_PRINTF_collection_id "%u"
#define C_PRINTF_env_var "%u"
#define C_PRINTF_int "%d"
#define C_PRINTF_int32 "%d"
#define C_PRINTF_da_id_t "%d"
#define C_PRINTF_merge_id_t "%d"
#define C_PRINTF_thread_id_t "%d"
#define C_PRINTF_work_id_t "%d"
#define C_PRINTF_work_size_t "%lu"
#define C_PRINTF_pid "%u"
#define C_PRINTF_c_da_opts_t "%lu"
#define C_PRINTF_c_state_t "%d"

#define CASTLE_IOCTL_0IN_0OUT(_id, _name)                                                         \
int castle_##_id (struct castle_front_connection *conn)                                           \
{                                                                                                 \
    struct castle_control_ioctl ctl;                                                              \
    int ret;                                                                                      \
    ctl.cmd = _name;                                                                              \
                                                                                                  \
    ret = ioctl(conn->fd, _name##_IOCTL, &ctl);                                                   \
    if (__builtin_expect(conn->debug_log != NULL, 0)) {                                           \
      fprintf(conn->debug_log, #_id "() = %d\n", ret);                                            \
      fflush(conn->debug_log);                                                                    \
    }                                                                                             \
    if (ret)                                                                                      \
        return errno;                                                                             \
                                                                                                  \
    return ctl._id.ret;                                                                           \
}

#define CASTLE_IOCTL_1IN_0OUT(_id, _name, _arg_1_t, _arg_1)                                       \
int castle_##_id (struct castle_front_connection *conn,                                           \
    C_TYPE_##_arg_1_t _arg_1)                                                                     \
{                                                                                                 \
    struct castle_control_ioctl ctl;                                                              \
    int ret;                                                                                      \
    ctl.cmd = _name;                                                                              \
    ctl._id._arg_1 = _arg_1;                                                                      \
                                                                                                  \
    ret = ioctl(conn->fd, _name##_IOCTL, &ctl);                                                   \
    if (__builtin_expect(conn->debug_log != NULL, 0)) {                                           \
      fprintf(conn->debug_log,                                                                    \
              #_id "(" #_arg_1 " = " C_PRINTF_##_arg_1_t ") = %d\n",                              \
              _arg_1, ret);                                                                       \
      fflush(conn->debug_log);                                                                    \
    }                                                                                             \
    if (ret)                                                                                      \
        return errno;                                                                             \
                                                                                                  \
    return ctl._id.ret;                                                                           \
}

#define CASTLE_IOCTL_0IN_1OUT(_id, _name, _ret_1_t, _ret)                                         \
int castle_##_id (struct castle_front_connection *conn,                                           \
    C_TYPE_##_ret_1_t * _ret##_out)                                                               \
{                                                                                                 \
    struct castle_control_ioctl ctl;                                                              \
    int ret;                                                                                      \
    ctl.cmd = _name;                                                                              \
                                                                                                  \
    ret = ioctl(conn->fd, _name##_IOCTL, &ctl);                                                   \
    if (__builtin_expect(conn->debug_log != NULL, 0)) {                                           \
      fprintf(conn->debug_log, #_id "(" #_ret " = " C_PRINTF_##_ret_1_t ") = %d\n",               \
                            ctl._id.ret, ret);                                                    \
      fflush(conn->debug_log);                                                                    \
    }                                                                                             \
    if (ret)                                                                                      \
        return errno;                                                                             \
                                                                                                  \
    * _ret##_out = ctl._id._ret;                                                                  \
                                                                                                  \
    return ctl._id.ret;                                                                           \
}                                                                                                 \

#define CASTLE_IOCTL_1IN_1OUT(_id, _name, _arg_1_t, _arg_1, _ret_1_t, _ret)                       \
int castle_##_id (struct castle_front_connection *conn,                                           \
    C_TYPE_##_arg_1_t _arg_1,                                                                     \
    C_TYPE_##_ret_1_t * _ret##_out)                                                               \
{                                                                                                 \
    struct castle_control_ioctl ctl;                                                              \
    int ret;                                                                                      \
    ctl.cmd = _name;                                                                              \
    ctl._id._arg_1 = _arg_1;                                                                      \
                                                                                                  \
    ret = ioctl(conn->fd, _name##_IOCTL, &ctl);                                                   \
    if (__builtin_expect(conn->debug_log != NULL, 0)) {                                           \
      fprintf(conn->debug_log, #_id "(" #_arg_1 " = " C_PRINTF_##_arg_1_t                         \
              ", " #_ret " = " C_PRINTF_##_ret_1_t ") = %d\n", _arg_1, ctl._id.ret, ret);         \
      fflush(conn->debug_log);                                                                    \
    }                                                                                             \
    if (ret)                                                                                      \
        return errno;                                                                             \
                                                                                                  \
    * _ret##_out = ctl._id._ret;                                                                  \
                                                                                                  \
    return ctl._id.ret;                                                                           \
}                                                                                                 \

#define CASTLE_IOCTL_2IN_0OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2)                     \
int castle_##_id (struct castle_front_connection *conn,                                           \
    C_TYPE_##_arg_1_t _arg_1, C_TYPE_##_arg_2_t _arg_2)                                           \
{                                                                                                 \
    struct castle_control_ioctl ctl;                                                              \
    int ret;                                                                                      \
    ctl.cmd = _name;                                                                              \
    ctl._id._arg_1 = _arg_1;                                                                      \
    ctl._id._arg_2 = _arg_2;                                                                      \
                                                                                                  \
    ret = ioctl(conn->fd, _name##_IOCTL, &ctl);                                                   \
    if (__builtin_expect(conn->debug_log != NULL, 0)) {                                           \
      fprintf(conn->debug_log,                                                                    \
              #_id "(" #_arg_1 " = " C_PRINTF_##_arg_1_t                                          \
                  ", " #_arg_2 " = " C_PRINTF_##_arg_2_t ") = %d\n",                              \
              _arg_1, _arg_2, ret);                                                               \
      fflush(conn->debug_log);                                                                    \
    }                                                                                             \
    if (ret)                                                                                      \
        return errno;                                                                             \
                                                                                                  \
    return ctl._id.ret;                                                                           \
}

#define CASTLE_IOCTL_2IN_1OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2,                     \
                              _ret_1_t, _ret)                                                     \
int castle_##_id (struct castle_front_connection *conn,                                           \
    C_TYPE_##_arg_1_t _arg_1, C_TYPE_##_arg_2_t _arg_2, C_TYPE_##_ret_1_t * _ret##_out)           \
{                                                                                                 \
    struct castle_control_ioctl ctl;                                                              \
    int ret;                                                                                      \
    ctl.cmd = _name;                                                                              \
    ctl._id._arg_1 = _arg_1;                                                                      \
    ctl._id._arg_2 = _arg_2;                                                                      \
                                                                                                  \
    ret = ioctl(conn->fd, _name##_IOCTL, &ctl);                                                   \
    if (__builtin_expect(conn->debug_log != NULL, 0)) {                                           \
      fprintf(conn->debug_log,                                                                    \
              #_id "(" #_arg_1 " = " C_PRINTF_##_arg_1_t                                          \
                  ", " #_arg_2 " = " C_PRINTF_##_arg_2_t                                          \
                  ", " #_ret " = " C_PRINTF_##_ret_1_t ") = %d\n",                                \
              _arg_1, _arg_2, ctl._id.ret, ret);                                                  \
      fflush(conn->debug_log);                                                                    \
    }                                                                                             \
    if (ret)                                                                                      \
        return errno;                                                                             \
                                                                                                  \
    * _ret##_out = ctl._id._ret;                                                                  \
                                                                                                  \
    return ctl._id.ret;                                                                           \
}

#define CASTLE_IOCTL_3IN_1OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2,                     \
    _arg_3_t, _arg_3, _ret_1_t, _ret)                                                             \
int castle_##_id (struct castle_front_connection *conn,                                           \
    C_TYPE_##_arg_1_t _arg_1,                                                                     \
    C_TYPE_##_arg_2_t _arg_2,                                                                     \
    C_TYPE_##_arg_3_t _arg_3,                                                                     \
    C_TYPE_##_ret_1_t * _ret##_out)                                                               \
{                                                                                                 \
    struct castle_control_ioctl ctl;                                                              \
    int ret;                                                                                      \
                                                                                                  \
    ctl.cmd = _name;                                                                              \
    ctl._id._arg_1 = _arg_1;                                                                      \
    ctl._id._arg_2 = _arg_2;                                                                      \
    ctl._id._arg_3 = _arg_3;                                                                      \
                                                                                                  \
    ret = ioctl(conn->fd, _name##_IOCTL, &ctl);                                                   \
    if (__builtin_expect(conn->debug_log != NULL, 0)) {                                           \
      fprintf(conn->debug_log, #_id "(" #_arg_1 " = " C_PRINTF_##_arg_1_t                         \
              ", " #_arg_2 " = " C_PRINTF_##_arg_2_t                                              \
              ", " #_arg_3 " = " C_PRINTF_##_arg_3_t                                              \
              ", " #_ret " = " C_PRINTF_##_ret_1_t ") = %d\n",                                    \
              _arg_1, _arg_2, _arg_3, ctl._id.ret, ret);                                          \
      fflush(conn->debug_log);                                                                    \
    }                                                                                             \
    if (ret)                                                                                      \
        return errno;                                                                             \
                                                                                                  \
    * _ret##_out = ctl._id._ret;                                                                  \
                                                                                                  \
    return ctl._id.ret;                                                                           \
}

CASTLE_IOCTLS
PRIVATE_CASTLE_IOCTLS

/*************************************************************************************************/
/* A thin wrapper executable to issue Castle ioctls. */

#define CASTLE_IOCTLS_EXE_DEBUG
#define STR_TO_CTYPE_version(_v)   atol(_v)
#define STR_TO_CTYPE_string(_v)    (_v)
#define STR_TO_CTYPE_size(_v)      atol(_v)

//#define RET_STR_FMT             ("%lld")
//#define RET_STR_FMT             ("0x%x")
#define RET_STR_FMT             ("0x%x\n")

static int parse_args(int argc, char *argv[], char **cmd, char **str_args[])
{
    int i;
    *cmd = argv[1]; //TODO append a castle_ prefix so the user does not have to specify it?
    for (i=2; i<argc; i++)
        (*str_args)[i-2]=argv[i];
    if (argc>2)
        return argc-2;
    return 0;
}

int main(int argc, char *argv[])
{
    int i;
    int arg_count;
    char *cmd = NULL;
    char **str_args = NULL;
    castle_connection *conn;
    int err = 0;

    if ((err = castle_connect(&conn)))
    {
        fprintf(stderr, "castle_connect failed with error code %d (is Castle running?)\n", err);
        return err; /* After this there can be no returns until the out label! */
    }

    if (argc < 2)
    {
        fprintf(stderr, "Insufficient number of arguments (%d).\n", argc);
        err = -EINVAL;
        goto out;
    }
    if (argc > 2)
    {
        str_args = malloc(sizeof(char *) * (argc-2));
        if (!str_args)
        {
            fprintf(stderr, "Failed to malloc for argument parseing.\n");
            err = -ENOMEM;
            goto out;
        }
    }

    arg_count = parse_args(argc, argv, &cmd, &str_args);

#ifdef CASTLE_IOCTLS_EXE_DEBUG
    fprintf(stderr, "argc: %d\n", argc);
    for(i=0; i<argc; i++)
        fprintf(stderr, "argv[%d]=%s\n", i, argv[i]);
    fprintf(stderr, "cmd: %s, arg_count: %d\n", cmd, arg_count);
    for(i=0; i<arg_count; i++)
        fprintf(stderr, "arg %d: %s\n", i, str_args[i]);
    fprintf(stderr, "Preparing to issue ioctl...\n");
#endif


    // TODO: FUNKY MACRO MAGIC HERE? Mileage may vary...
    if (0) {}
    else if (!strcmp(cmd, "collection_attach"))
    {
        int _i = 0;
        int req_arg_count = 2;
        C_TYPE_version arg1;
        C_TYPE_string arg2;
        //C_TYPE_size arg3 = STR_TO_CTYPE_size(str_args[_i++]);
        C_TYPE_collection_id ret;


        if (arg_count != req_arg_count)
        {
            fprintf(stderr, "Command %s requires %d arguments (got %d).\n", cmd, req_arg_count, arg_count);
            err = -EINVAL;
            goto out;
        }

        arg1 = STR_TO_CTYPE_version(str_args[_i++]);
        arg2 = STR_TO_CTYPE_string(str_args[_i++]);
        //C_TYPE_size arg3 = STR_TO_CTYPE_size(str_args[_i++]);

#ifdef CASTLE_IOCTLS_EXE_DEBUG
        fprintf(stderr, "running %s\n", cmd);
#endif

        if ((err = castle_collection_attach(conn, arg1, arg2, strlen(arg2)+1, &ret)))
        {
            fprintf(stderr, "Command %s failed with error code %d.\n", cmd, err);
            goto out;
        }
        printf(RET_STR_FMT, ret);
        goto out;
    }

    fprintf(stderr, "Command %s not recognized.\n", cmd);
    err = -EINVAL;
out:
    castle_disconnect(conn);
    return err;
}


