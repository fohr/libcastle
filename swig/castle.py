#!/usr/bin/python2.6
import libcastle
import errno
import logging, sys

pycastle_log = logging.getLogger('test')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(funcName)s: %(message)s')
ch.setFormatter(formatter)
pycastle_log.addHandler(ch)

def castle_connect():
    conn = libcastle.c_connect()
    if not conn:
        raise Exception("no connection! is castle alive?")
    pycastle_log.debug("returning conn = "+str(conn))
    pycastle_log.info("Established connection")
    return conn

def castle_disconnect(conn):
    pycastle_log.debug("entering with conn = "+str(conn))
    libcastle.castle_disconnect(conn)
    pycastle_log.info("Disconnected")

def castle_collection_create(conn):
    pycastle_log.debug("entering with conn = "+str(conn))
    v = libcastle.castle_version_p()
    ret = libcastle.castle_create(conn, 0, v.cast())
    if ret != 0:
        raise Exception("returned "+str(ret))
    pycastle_log.debug("returning v = "+str(v)+", v.value() = "+str(v.value()))
    pycastle_log.info("Created collection with version number "+str(v.value()))
    return v

def castle_collection_attach(conn, v, coll_name):
    pycastle_log.debug("entering with conn = "+str(conn)+" v.value() = "+str(v.value())+" coll_name = "+str(coll_name))
    #make a string that's guaranteed to be null-terminated
    nt_coll_name = str(coll_name) + "\0"
    coll = libcastle.c_collection_id_t_p()
    ret = libcastle.castle_collection_attach(conn, v.value(), nt_coll_name, len(nt_coll_name), coll.cast())
    if ret != 0:
        raise Exception("returned "+str(ret))
    pycastle_log.debug("returning coll = "+str(coll))
    pycastle_log.info("Attached to collection with version number "+str(v.value())+" with coll name "+str(nt_coll_name)+" with coll number "+str(coll.value()))
    return coll

def castle_collection_detach(conn, coll):
    pycastle_log.debug("entering with conn = "+str(conn)+" coll = "+str(coll))
    ret = libcastle.castle_collection_detach(conn, coll.value())
    if ret != 0:
        raise Exception("returned "+str(ret))
    pycastle_log.info("Detached from coll number "+str(coll.value()))

def castle_collection_snapshot(conn, coll):
    pycastle_log.debug("entering with conn = "+str(conn)+" coll = "+str(coll))
    old_v = libcastle.castle_version_p()
    ret = libcastle.castle_collection_snapshot(conn, coll.value(), old_v.cast())
    if ret != 0:
        raise Exception("returned "+str(ret))
    pycastle_log.debug("returning old_v = "+str(old_v)+", old_v.value() = "+str(old_v.value()))
    pycastle_log.info("Created snapshot of version "+str(old_v.value()))
    return old_v

def castle_shared_buffer_create(conn, size):
    pycastle_log.debug("entering with conn = "+str(conn)+" size = "+str(int(size)))
    buf_p = libcastle.c_shared_buffer_create(conn, int(size))
    if not buf_p:
        raise Exception("Failed to create shared buffer of size "+str(int(size)))
    pycastle_log.debug("returning a "+str(type(buf_p)))
    return buf_p

def castle_shared_buffer_destroy(conn, buf_p, size):
    pycastle_log.debug("entering with conn = "+str(conn)+
        " a buf_p of type = "+str(type(buf_p))+
        " size = "+str(int(size)))
    libcastle.c_shared_buffer_destroy(conn, buf_p, size)

def make_key(key, buf, buf_len):
    pycastle_log.debug("entering with key = "+str(key))


    key_dims = list()
    number_of_keys = None

    #key could be a list, a tuple, or a single entry... but we don't check against
    #collections.Iterable type because that would include strings, and it seems more
    #natural to treat key["foo"] as a single dimension key.
    if isinstance(key, tuple) or isinstance(key, list):
        number_of_dims = len(key)
        for dim in key:
            key_dims.append(dim)
    else:
        number_of_dims = 1
        key_dims.append(key)

    builder = libcastle.make_key_builder(number_of_dims, buf, buf_len)
    if not builder:
        raise Exception("Failed to allocate key builder.")

    for dim_num in range(0, number_of_dims):
        dim = key_dims[dim_num]
        pycastle_log.debug("Making dimension "+str(dim_num)+", "+str(dim))
        if isinstance(dim, str):
            ret = libcastle.build_key(builder, dim_num, str(dim), len(str(dim)))
            if ret != 0:
                raise Exception("libcastle.build_key returned "+str(ret))
        elif isinstance(dim, int):
            raise Exception("Handling ints in keys... not implemented yet!")
        else:
            raise Exception("Can only make keys out of ints and/or strs.")

    ck = libcastle.castle_key_ptr(builder)
    ck_size = libcastle.finalize_key(builder)
    return ck, ck_size

def dump(obj):
    for attr in dir(obj):
        print "obj "+str(attr)+" = "+str(getattr(obj, attr))

def castle_get_blocking(castle_key, castle_key_len, conn, coll, val_buf, val_len, get_timestamp=False):
    pycastle_log.debug("entering ")
    req_p = libcastle.malloc_castle_request()
    if isinstance(coll, int):
        coll_id = coll
    else:
        coll_id = coll.value()
    if get_timestamp:
        flag = libcastle.CASTLE_RING_FLAG_RET_TIMESTAMP
    else:
        flag = libcastle.CASTLE_RING_FLAG_NONE

    pycastle_log.debug("preparing request ")
    libcastle.c_get_prepare(req_p,
                            coll_id,
                            castle_key,
                            castle_key_len,
                            val_buf,
                            val_len,
                            flag)

    call_p = libcastle.malloc_castle_blocking_call_t()
    ret = libcastle.castle_request_do_blocking(conn, req_p, call_p)
    if ret == -errno.ENOENT:
        return None
    elif ret != 0:
        raise Exception("returned "+str(ret))
    val_len = call_p.length
    libcastle.free_castle_request(req_p)
    libcastle.free_castle_blocking_call_t(call_p)
    pycastle_log.debug("returning val_len = "+str(val_len))
    return val_len

def castle_replace_blocking(castle_key, castle_key_len, conn, coll, val_buf, val_len):
    pycastle_log.debug("entering ")
    req_p = libcastle.malloc_castle_request()
    if isinstance(coll, int):
        coll_id = coll
    else:
        coll_id = coll.value()

    pycastle_log.debug("preparing request ")
    libcastle.c_replace_prepare(req_p,
                                coll_id,
                                castle_key,
                                castle_key_len,
                                val_buf,
                                val_len,
                                libcastle.CASTLE_RING_FLAG_NONE)

    call_p = libcastle.malloc_castle_blocking_call_t()
    libcastle.castle_request_do_blocking(conn, req_p, call_p)
    libcastle.free_castle_request(req_p)
    libcastle.free_castle_blocking_call_t(call_p)
    pycastle_log.debug("returning ")


class Castle:
    conn = None

    #todo: we need a mutex to protect this. Also, having just a single buffer doesn't seem
    #like a great idea... maybe we can make one set of buffers per-collection? Probably
    #okay to force ops to serialise per-collection.
    key_buf_size = None
    key_buf = None
    val_buf_size = None
    val_buf = None

    #because we are using generators for range queries, it might lead to users not giving up
    #stateful ops as often as they should; lets at least help them keep track of it.
    current_stateful_op_count = 0

    #note: Current limitation is each Castle object may only have a single collection; this
    #      limitation should be removed in the future, and what we ought to have here is a
    #      set of collections.
    current_coll = None

    #note: current_version is not quite the current_version when using collection_snapshot
    #      because that method (in libcastle) currently only returns the old version and we
    #      would need to do some sysfs shenanigans to get the actual current version after
    #      a snapshot. I've filed a trac ticket (#3580) suggesting a change in libcastle
    #      that would make castle_collection_snapshot return the new_v in addition to old_v.
    current_version = None

    def __init__(self, opts={}):
        if sys.version_info < (2,6):
            pycastle_log.info("recommend using version 2.6 or later")


        self.conn = castle_connect()

        if opts:
            pycastle_log.info("opts: "+str(opts))

        #default shared buffer sizing
        self.key_buf_size = 4096
        self.val_buf_size = 4096*256
        #overide with opts
        if "key_buf_size" in opts:
            self.key_buf_size = opts["key_buf_size"]
        if "val_buf_size" in opts:
            self.val_buf_size = opts["key_buf_size"]

        self.key_buf = castle_shared_buffer_create(self.conn, self.key_buf_size)
        self.val_buf = castle_shared_buffer_create(self.conn, self.val_buf_size)

    def __del__(self):
        pycastle_log.debug(str(self)+" start ")
        if self.current_coll:
            self.collection_detach()
        castle_disconnect(self.conn)
        pycastle_log.debug(str(self)+" end ")

    def collection_detach(self):
        pycastle_log.debug(str(self)+" start ")
        if not self.current_coll:
            raise Exception("not attached!")
        castle_collection_detach(self.conn, self.current_coll)
        self.current_coll = None
        self.current_version = None
        pycastle_log.debug(str(self)+" end ")

    #we assume _v is an integer or a Swig Object of type 'castle_version_p *'
    def collection_attach(self, _v, coll_name):
        """
        Attach to an existing collection at version number _v, and give the attachment name coll_name.
        """
        pycastle_log.debug(str(self)+" start ")
        #drop current attachment if one exists
        if self.current_coll:
            self.collection_detach()

        #note: castle_collection_attach doesn't actually require a c_ver_t *; it dereferences it
        #      right away, so an int should be good enough. In conventional usage, it may take
        #      the outcome of castle_collection_create directly, but perhaps we shouldn't be
        #      allowing that... for now, we will continue building a c_ver_t * to be passed to
        #      castle_collection_attach. TODO: clean this up.

        #set up the correct types for the c_ver_t
        if isinstance(_v, int):
            v = _v
        else:
            v = int(_v.value())
        vp = libcastle.castle_version_p()
        vp.assign(v)

        #make new attachment
        new_coll = castle_collection_attach(self.conn, vp, coll_name)
        self.current_coll = new_coll
        self.current_version = v
        pycastle_log.debug(str(self)+" end ")

    def new_collection_attach(self, coll_name):
        """
        Create a new collection and attach to it (the equivalent of doing castle_create then
        castle_collection_attach). Takes an attachment name as an argument (coll_name).
        """
        pycastle_log.debug(str(self)+" start ")
        new_v = castle_collection_create(self.conn)
        self.collection_attach(new_v, coll_name)
        pycastle_log.debug(str(self)+" end ")

    def collection_snapshot(self):
        pycastle_log.debug(str(self)+" start ")
        old_v = castle_collection_snapshot(self.conn, self.current_coll)

        #once we are capable of updating the current_version after a snapshot, the following assertion will apply
        #if int(old_v.value()) != self.current_version:
        #    raise Exception("old_v.value = "+str(int(old_v.value()))+",!= current version = "+str(self.current_version))
        self.current_version = int(old_v.value()) #this is technically wrong, but is less wrong than not updating it at all
        pycastle_log.debug(str(self)+" end ")

    # point get
    def __getitem__(self, key):
        pycastle_log.info("Doing point get of key "+str(key))

        #prepare key
        ck, ck_size = make_key(key, self.key_buf, self.key_buf_size)

        #do it
        val_len = castle_get_blocking(ck, ck_size, self.conn, self.current_coll, self.val_buf, self.val_buf_size)
        if not val_len:
            pycastle_log.info("got tombstone ")
            return None

        val = libcastle.cdata(self.val_buf, val_len)
        return val

    # replace
    def __setitem__(self, key, val):
        pycastle_log.info("Replacing key "+str(key)+" with val "+str(val))

        #prepare key
        ck, ck_size = make_key(key, self.key_buf, self.key_buf_size)

        #prepare value
        if isinstance(val, str):
            _val = str(val)
            libcastle.memmove(self.val_buf, _val)
            val_len = len(_val)
        else:
            raise Exception("Currently only str values supported")

        #do it
        castle_replace_blocking(ck, ck_size, self.conn, self.current_coll, self.val_buf, val_len)


    # tombstone
    def __delitem__(self, key):
        pycastle_log.info("Inserting tombstone on key "+str(key))

    #todo: actually implement this!
    def range_query(self, start_key, end_key):
        """
        NOT YET IMPLEMENTED... but the idea is to implement range queries as python generators.
        They will take a start key and an end key, then users can rely on standard generator
        semantics to drive the range query (e.g. using the next method, or putting it directly
        in a for loop).
        """
        print "THIS IS FAKE"
        self.current_stateful_op_count += 1
        pycastle_log.debug(str(self)+" start, current_stateful_op_count now "+str(self.current_stateful_op_count))
        pycastle_log.info("Doing range query from key "+str(start_key)+" to key "+str(end_key))
        try:
            i = 0
            while i < 10:
                yield i
                i+=1
                if i % 5 == 0:
                    pycastle_log.info("Getting next batch")
        except GeneratorExit:
            self.current_stateful_op_count -= 1
            pycastle_log.info("User requested stop of range query from key "+str(start_key)+" to key "+str(end_key))
            pycastle_log.debug(str(self)+" end, current_stateful_op_count now "+str(self.current_stateful_op_count))

        self.current_stateful_op_count -= 1
        pycastle_log.debug(str(self)+" end, current_stateful_op_count now "+str(self.current_stateful_op_count))
        raise StopIteration


