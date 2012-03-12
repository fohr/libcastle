#!/usr/bin/python2.6
"""
Front-end interfaces to Acunu Castle. This is a convenience and compatibility layer mainly on top
of SWIG generated bindings to libcastle, though some abstractions over sysfs are also provided.
"""
import libcastle
import errno
import logging, sys
import struct
import abc
import re

pycastle_log = logging.getLogger('test')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(module)s: %(funcName)s: %(message)s')
ch.setFormatter(formatter)
pycastle_log.addHandler(ch)

###################################################################################################
#for performance tests and such we may want to totally disable the logging mechanism, and AFAICT
#there isn't anything much more elegant than this that actually disables everything from the
#perspective of performance.
def dummy_debug_log(*ignore):
    pass
def dummy_info_log(*ignore):
    pass
def disable_logging():
    """
    Completely disable client-side castle.py module logging. You may want to do this for performance
    testing and such (impact of the logging module is non-trivial, given how frequently it is called
    by this module). There is no supported way to re-enable logging after disabling it; the only
    way is to reload the castle.py module.
    """
    print "Permanently disabled logging; to re-enable logging, module must be reloaded"
    pycastle_log.debug=dummy_debug_log
    pycastle_log.info=dummy_info_log
###################################################################################################

###################################################################################################
#The following are a layer of convenience on top of SWIG generated libcastle bindings; users are
#not expected to use these (TODO: move these to a seperate file!)
def castle_connect():
    conn = libcastle.c_connect()
    if not conn:
        raise CastleConnectionException("no connection! is castle alive?")
    pycastle_log.debug("Established connection")
    pycastle_log.debug("returning conn = "+str(conn))
    return conn

def castle_disconnect(conn):
    pycastle_log.debug("entering with conn = "+str(conn))
    libcastle.castle_disconnect(conn)
    pycastle_log.debug("Disconnected")

def castle_delete_version(conn, v):
    pycastle_log.debug("entering with conn = "+str(conn)+" v = "+str(v))
    ret = libcastle.castle_delete_version(conn, v)
    if ret != 0:
        raise Exception("returned "+str(ret))
    pycastle_log.info("Deleted version "+str(v))

def castle_collection_create(conn):
    """ Convenience method to access something SWIG generated from libcastle. """
    pycastle_log.debug("entering with conn = "+str(conn))
    v = libcastle.castle_version_p()
    ret = libcastle.castle_create(conn, 0, v.cast())
    if ret != 0:
        raise CastleCollectionCreateException(ret)
    pycastle_log.debug("returning v = "+str(v)+", v.value() = "+str(v.value()))
    pycastle_log.debug("Created collection with version number "+str(v.value()))
    return v.value()

def castle_collection_attach(conn, _v, coll_name):
    """ Convenience method to access something SWIG generated from libcastle. """
    v = libcastle.castle_version_p()
    v.assign(_v)
    pycastle_log.debug("entering with conn = "+str(conn)+" v.value() = "+str(v.value())+" coll_name = "+str(coll_name))
    #make a string that's guaranteed to be null-terminated
    nt_coll_name = str(coll_name) + "\0"
    coll = libcastle.c_collection_id_t_p()
    ret = libcastle.castle_collection_attach(conn, v.value(), nt_coll_name, len(nt_coll_name), coll.cast())
    if ret != 0:
        raise CastleCollectionVersionNotAttachableException(ret)
    pycastle_log.debug("returning coll = "+str(coll))
    pycastle_log.debug("Attached to collection with version number "+str(v.value())+" with coll name "+str(nt_coll_name)+" with coll number "+str(coll.value()))
    return coll.value()

def castle_collection_snapshot(conn, coll):
    """ Convenience method to access something SWIG generated from libcastle. """
    pycastle_log.debug("entering with conn = "+str(conn)+" coll = "+str(coll))
    old_v = libcastle.castle_version_p()
    ret = libcastle.castle_collection_snapshot(conn, coll, old_v.cast())
    if ret != 0:
        raise CastleCollectionSnapshotException(ret)
    pycastle_log.debug("returning old_v = "+str(old_v)+", old_v.value() = "+str(old_v.value()))
    pycastle_log.debug("Created snapshot of version "+str(old_v.value()))
    return old_v.value()

def castle_collection_detach(conn, coll):
    """ Convenience method to access something SWIG generated from libcastle. """
    pycastle_log.debug("entering with conn = "+str(conn)+" coll = "+str(coll))
    ret = libcastle.castle_collection_detach(conn, coll)
    if ret != 0:
        raise CastleCollectionDetachException(ret)
    pycastle_log.debug("Detached from coll number "+str(coll))

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
            raise CastleKeyException("Handling ints in keys... not implemented yet!")
        else:
            raise CastleKeyException("Can only make keys out of ints and/or strs.")

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
    libcastle.c_get_prep(req_p, coll_id, castle_key, castle_key_len, val_buf, val_len, flag)

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

def castle_replace_blocking(castle_key, castle_key_len, conn, coll, val_buf=None, val_len=None, counter=None):
    """
    Do a replace (a.k.a. insert); block until successful. If no val_buf provided, we will insert
    tombstone (i.e. rm key). If inserting a counter, pass a counter type to argument 'counter',
    but note that the value must still be prepared in val_buf. This is not nice, if you are reading
    this, you should hassle someone to improve it.
    """
    pycastle_log.debug("entering ")

    req_p = libcastle.malloc_castle_request()
    if isinstance(coll, int):
        coll_id = coll
    else:
        coll_id = coll.value()

    pycastle_log.debug("preparing request ")

    if not val_buf: #must be tombstone
        libcastle.c_rm_prep(req_p, coll_id, castle_key, castle_key_len, libcastle.CASTLE_RING_FLAG_NONE)
    else: #could be counter_{ADD|SET} or regular insert
        if isinstance(counter, CastleCounterAdd):
            libcastle.c_add_prep(req_p, coll_id, castle_key, castle_key_len, val_buf, val_len, libcastle.CASTLE_RING_FLAG_NONE)
        elif isinstance(counter, CastleCounterSet):
            libcastle.c_set_prep(req_p, coll_id, castle_key, castle_key_len, val_buf, val_len, libcastle.CASTLE_RING_FLAG_NONE)
        else:
            if counter:
                raise CastleValueException("unexpected counter parameter of type "+str(type(counter)))
            libcastle.c_replace_prep(req_p, coll_id, castle_key, castle_key_len, val_buf, val_len, libcastle.CASTLE_RING_FLAG_NONE)

    call_p = libcastle.malloc_castle_blocking_call_t()
    ret = libcastle.castle_request_do_blocking(conn, req_p, call_p)
    if ret != 0:
        raise CastleReplaceException(ret)
    libcastle.free_castle_request(req_p)
    libcastle.free_castle_blocking_call_t(call_p)
    pycastle_log.debug("returning ")
###################################################################################################


###################################################################################################
# These are the means by which users are primarily expected to interact with Castle
def HACK_get_current_version_from_sysfs(coll_number):
    """
    At the moment after doing a snapshot the only way to know the version_id of a collection
    is to look it up in sysfs; this sucks, but for now, it's necessary, so if you need to know
    this information, use this method. In future, the version_id will be updated correctly
    within a CastleCollection object whenever a snapshot is done.
    """
    filename = "/sys/fs/castle-fs/collections/{0}/version".format(coll_number)
    try:
        with open(filename, 'r') as fd:
            for text in fd:
                return int(text, 16)
    except Exception, e:
        pycastle_log.error("Failed while trying to open {0} with exception {1}:{2}".format(filename, type(e), e))
        raise

class CastleException(Exception): pass
class CastleKeyException(CastleException): pass
class CastleValueException(CastleException): pass
class CastleReplaceException(CastleException): pass

class CastleConnectionException(CastleException): pass

class CastleConnection(object):
    """ castle_connection (and castle_shared_buffer) management. """
    conn = None
    key_buffer = None
    val_buffer = None

    def __init__(self):
        """ Make a castle_connection. """
        pycastle_log.debug(str(self)+" start")
        try:
            self.conn = castle_connect()
            if not self.conn:
                raise CastleConnectionException("Failed to make connection; is Castle alive?")
            pycastle_log.info(str(self)+" Established connection")
            key_buffer = CastleSharedBuffer(size=4*1024, connection=self)
            val_buffer = CastleSharedBuffer(size=16*1024, connection=self)
            self.key_buffer = key_buffer
            self.val_buffer = val_buffer
        except Exception, e:
            pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
            raise
        except:
            if self.key_buffer:
                del self.key_buffer
            if self.val_buffer:
                del self.val_buffer
            raise
        finally:
            pycastle_log.debug(str(self)+" stop")

    def buffers_destroy(self):
        """
        With the current implementation there is a circular dependency between CastleConnection and
        CastleSharedBuffer objects which prevents automatic garbage collection; so to properly
        clean up, the links must be removed manually. Call this method when the intent is to stop
        communicating with Castle (i.e. when you expect to destroy the CastleConnection object).

        TODO: get rid of this circular dependency so that gc will do the right thing and this method
        won't be necessary!
        """
        my_shared_buffers = [self.key_buffer, self.val_buffer]
        pycastle_log.debug(str(self)+" Calling buffer_destroy method for shared buffers: {0}".format(my_shared_buffers))
        for shared_buffer in my_shared_buffers:
            shared_buffer.buffer_destroy()

    def __del__(self):
        """ Destroy a castle_connection and associated shared_buffers. """
        if self.key_buffer:
            del self.key_buffer
        if self.val_buffer:
            del self.val_buffer
        castle_disconnect(self.conn)
        pycastle_log.info(str(self)+" Destroyed connection")

class CastleSharedBuffer(object):
    """ Castle/user shared_buffer management. """
    buf = None
    size = None
    connection = None

    def __init__(self, size, connection):
        """ Set up a shared buffer for data exchange with Castle. """
        pycastle_log.debug(str(self)+" start")
        try:
            assert isinstance(connection, CastleConnection), "wtf"
            self.buf = castle_shared_buffer_create(connection.conn, size)
            self.size = size
            self.connection = connection
            pycastle_log.info("Made buffer {0} of size {1} with connection {2}".format(self.buf, self.size, self.connection.conn))
        except Exception, e:
            pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
            raise
        finally:
            pycastle_log.debug(str(self)+" stop")

    def buffer_destroy(self):
        """ Destroy the shared buffer with Castle. """
        try:
            assert self.buf is not None, "wtf"
            assert self.size is not None, "wtf"
            assert self.connection is not None, "wtf"
            castle_shared_buffer_destroy(self.connection.conn, self.buf, self.size)
            pycastle_log.info(str(self)+" Destroyed buffer {0} of size {1} with connection {2}".format(self.buf, self.size, self.connection.conn))
            self.buf = None
            self.size = None
            self.connection = None
        except Exception, e:
            pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
            raise

    def __del__(self):
        pycastle_log.debug(str(self)+" start")
        if self.buf:
            self.destroy_buffer()
        else:
            pycastle_log.debug(str(self)+" Nothing to destroy")
        pycastle_log.debug(str(self)+" stop")

class CastleCollectionException(CastleException): pass
class CastleCollectionVersionNotAttachableException(CastleCollectionException): pass
class CastleCollectionNameNotFoundException(CastleCollectionException): pass
class CastleCollectionCreateException(CastleCollectionException): pass
class CastleCollectionSnapshotException(CastleCollectionException): pass
class CastleCollectionDetachException(CastleCollectionException): pass
class CastleCollectionNotAttachedException(CastleCollectionException): pass

class Castle(CastleConnection):
    """
    General-purpose management interface for Castle; use this to establish a connection and shared
    buffers, and then to instantiate CastleCollection objects which will be used to exchange data
    with Castle.
    """

    def __init__(self):
        """ Call CastleConnection constructor. """
        pycastle_log.debug(str(self)+" start")
        super(Castle, self).__init__()
        pycastle_log.debug(str(self)+" stop")

    def __del__(self):
        """ Call CastleConnection destructor. """
        pycastle_log.debug(str(self)+" start")
        super(Castle, self).__del__()
        pycastle_log.debug(str(self)+" stop")

    def collection_create(self, name):
        """ Make a CastleCollection on a new vertree, attached to the root version. """
        try:
            return CastleCollection(name, self)
        except:
            raise

    def collection_find_by_name(self, name):
        """ Make a CastleCollection on an existing attachment by name. """
        raise Exception("NEVER BEEN TESTED!")
        try:
            return CastleCollection(name, self, look_for_name=True)
        except:
            raise

    def collection_attach(self, name, version_id):
        """ Make a CastleCollection by attaching to the given version. """
        try:
            return CastleCollection(name, self, version_id)
        except:
            raise

    def version_delete(self, version_id):
        """ Delete a version (without attaching a CastleCollection to it). """
        try:
            castle_delete_version(self.conn, version_id)
            pycastle_log.info("Deleted version {0}".format(version_id))
        except Exception, e:
            pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
            raise

    def version_clone(self, version_id):
        """ Clone a version, returning the new (clone) version_id. """
        raise Exception("TODO")

    def checkpoint_period_get(self):
        """ Return the current checkpoint period. """
        raise Exception("TODO")

    def checkpoint_period_set(self):
        """ Set the checkpoint period. """
        raise Exception("TODO")

class CastleCollection(object):
    """ Castle collection management. """
    coll_id = None
    version_id = None
    name = None
    castle = None
    attached = None

    def __init__(self, name, castle, version_id=None, look_for_name=False):
        """
        Attach to a collection; if version_id provided, attach to that version (and if this fails,
        raise CastleCollectionVersionNotAttachableException), else, if look_for_name, connect to an
        existing attachment by that name (if this fails, raise CastleCollectionNameNotFoundException),
        else make a new vertree with a new root version_id and attach to the root version.
        """
        try:
            assert not(version_id and look_for_name), "invalid opts"
            assert isinstance(castle, Castle), "castle is of type {0}, expecting type {1}".format(type(castle), Castle)
            coll_id = None
            attached_v_id = None
            self.attached = False

            if look_for_name:
                #reattach to an existing attachment, by name
                raise Exception("TODO")
            elif version_id:
                #attach to the specified version_id
                coll_id = castle_collection_attach(castle.conn, int(version_id), name)
                pycastle_log.info(str(self)+" Attached to version {0}, with collection {1} of id {2} on connection {3}".format(version_id, name, coll_id, castle.conn))
                attached_v_id = version_id
            else:
                #make a new vertree and attach to the root version
                new_v_id = castle_collection_create(castle.conn)
                coll_id = castle_collection_attach(castle.conn, new_v_id, name)
                pycastle_log.info(str(self)+" Created a new vertree with root version {0}, attached to collection {1} with id {2} on connection {3}".format(new_v_id, name, coll_id, castle.conn))
                attached_v_id = new_v_id
            self.name = name
            self.coll_id = coll_id
            self.version_id = attached_v_id
            self.castle = castle
            self.attached = True
        except Exception, e:
            pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
            raise

    def detach(self):
        """ Detach the collection. """
        try:
            if self.attached:
                assert self.coll_id is not None, "wtf"
                assert self.castle is not None, "wtf"
                assert self.name is not None, "wtf"
                assert self.version_id is not None, "wtf"
                castle_collection_detach(self.castle.conn, self.coll_id)
                pycastle_log.info(str(self)+" Detached collection {2} with id {0} of version id {1}".format(self.coll_id, self.version_id, self.name))
                self.coll_id = None
                self.version_id = None
                self.attached = False
                self.castle = None
            else:
                pycastle_log.warn(str(self)+" Nothing to do (looks like Collection was not successfully attached?)")
        except Exception, e:
            pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
            raise

    def __del__(self):
        """
        By default, detach from the collection; if keepalive, then leave the attachment in place.
        """
        if self.attached:
            pycastle_log.warn(str(self)+" Leaving collection {0} with coll_id {1} of version id {2} attached".format(self.name, self.coll_id, self.version_id))
        else:
            pycastle_log.debug(str(self)+" No remaining attachment on collection named {0}".format(self.name))

    def range_query(self, start_key, end_key):
        """
        NOT YET IMPLEMENTED... but the idea is to implement range queries as python generators.
        They will take a start key and an end key, then users can rely on standard generator
        semantics to drive the range query (e.g. using the next method, or putting it directly
        in a for loop).
        """
        if not self.attached:
            raise CastleCollectionNotAttachedException()

        print "THIS IS FAKE"
        pycastle_log.info("Doing range query from key "+str(start_key)+" to key "+str(end_key))
        try:
            i = 0
            while i < 10:
                yield i
                i+=1
                if i % 5 == 0:
                    pycastle_log.info("Getting next batch")
        except GeneratorExit:
            pycastle_log.info("User requested stop of range query from key "+str(start_key)+" to key "+str(end_key))

    def __getitem__(self, key):
        """
        Queries (point gets and range queries). If key is a slice, returns a range_query generator object.
        """
        if not self.attached:
            raise CastleCollectionNotAttachedException()
        if isinstance(key, slice):
            try:
                start_key = key.start
                end_key = key.stop
                pycastle_log.debug("Making generator for range query from key {0} to key {1}".format(start_key, end_key))
                return self.range_query(start_key, end_key)
            except Exception, e:
                pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
                raise
        else:
            try:
                pycastle_log.debug("Doing point get of key "+str(key))
                ck, ck_size = make_key(key, self.castle.key_buffer.buf, self.castle.key_buffer.size)
                val_len = castle_get_blocking(ck, ck_size, self.castle.conn, self.coll_id, self.castle.val_buffer.buf, self.castle.val_buffer.size)
                if not val_len:
                    return None
                val = libcastle.cdata(self.castle.val_buffer.buf, val_len)
                return val
            except Exception, e:
                pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
                raise

    def __setitem__(self, key, val):
        """ Value insert. """
        if not self.attached:
            raise CastleCollectionNotAttachedException()
        pycastle_log.debug("Replacing key "+str(key)+" with val "+str(val))

        ck, ck_size = make_key(key, self.castle.key_buffer.buf, self.castle.key_buffer.size)
        counter = None
        if isinstance(val, _CastleCounter):
            vstr = struct.pack('q', int(val.value))
            libcastle.memmove(self.castle.val_buffer.buf, vstr)
            val_len = 8
            counter = val
        elif isinstance(val, str):
            vstr = str(val)
            libcastle.memmove(self.castle.val_buffer.buf, vstr)
            val_len = len(vstr)
        else:
            raise CastleKeyException("Currently only str values supported")
        castle_replace_blocking(ck, ck_size, self.castle.conn, self.coll_id, self.castle.val_buffer.buf, val_len, counter)

    def __delitem__(self, key):
        """ Tombstone insert. """
        if not self.attached:
            raise CastleCollectionNotAttachedException()
        pycastle_log.debug("Inserting tombstone on key "+str(key))
        ck, ck_size = make_key(key, self.castle.key_buffer.buf, self.castle.key_buffer.size)
        #val_buf=None tells castle_replace_blocking() to insert a tombstone
        castle_replace_blocking(ck, ck_size, self.castle.conn, self.coll_id, val_buf=None)

    def snapshot(self):
        """ Snapshot the collection's current version without removing the attachment. """
        if not self.attached:
            raise CastleCollectionNotAttachedException()
        try:
            old_v = castle_collection_snapshot(self.castle.conn, self.coll_id)
            #once we are capable of updating the current_version after a snapshot, the following assertion will apply
            #if old_v != self.version_id:
            #    raise Exception
            self.version_id = old_v #this is technically wrong, but is less wrong than not updating it at all
            pycastle_log.info("Snapshotting collection {0} (coll_id={1}, version_id={2})".format(self.name, self.coll_id, self.version_id))
        except Exception, e:
            pycastle_log.error(str(self)+" got exception {0}:{1}".format(type(e), e))
            raise

    def delete(self):
        """ Detach from and delete the version. """
        if not self.attached:
            raise CastleCollectionNotAttachedException()
        raise Exception("TODO")

    def vertree_delete(self):
        """ Detach from and delete the entire vertree. """
        if not self.attached:
            raise CastleCollectionNotAttachedException()
        raise Exception("TODO")

    def vertree_compact(self):
        """ Initiate a bigmerge on the vertree. """
        if not self.attached:
            raise CastleCollectionNotAttachedException()
        raise Exception("TODO")

class CastleInterface(Castle):
    """
    Encapculates the Castle object (which provides CastleCollection object, CastleSharedBuffer
    objects, and a factory to provide CastleCollection objects) and adds a Sysfs interface.

    TODO TODO TODO TODO TODO TODO
    """
    def __init__():
        raise Exception("TODO")

class CastleCounterException(CastleException): pass
class _CastleCounter(object):
    __metaclass__ = abc.ABCMeta
    value = None

    @abc.abstractmethod
    def __init__(self, val):
        if isinstance(val, _CastleCounter):
            self.value=val.value
        elif isinstance(val, int):
            self.value=val
        elif isinstance(val, str):
            self.value=int(struct.unpack('q', val)[0])
        else:
            raise CastleCounterException("Dunno what to do with val of type "+str(type(val)))

class CastleCounterSet(_CastleCounter):
    """ A Castle counter SET object. """
    def __init__(self, val):
        super(CastleCounterSet, self).__init__(val)

class CastleCounterAdd(_CastleCounter):
    """ A Castle counter ADD object. """
    def __init__(self, val):
        super(CastleCounterAdd, self).__init__(val)

def castle_counter_to_int(castle_counter_val):
    """ A convenience function to unpack a counter value returned by Castle. """
    return int(struct.unpack('q', castle_counter_val)[0])

###################################################################################################


###################################################################################################
# Work-in-progress: some stuff to help with sysfs...
class CastleSysfsException(CastleException): pass
class CastleSysfsSyntaxException(CastleSysfsException): pass

class CastleSysfsComponentTreeState:
    """
    State of a component tree (this is based on
    /sys/fs/castle-fs/vertrees/(vertree_id)/component_trees).
    """
    entries = None
    _raw_sysfs_string = None

    def __init__(self, sysfs_string):
        """
        The sysfs_string will be parsed (splitted) and results stored (right now just the
        number of entries in entries.
        """
        self.entries = None
        self._raw_sysfs_string = None
        if sysfs_string:
            self._raw_sysfs_string = sysfs_string
            self._parse_sysfs()

    def _parse_sysfs(self, sysfs_string = None):
        if sysfs_string:
            self._raw_sysfs_string = sysfs_string
        ct_attributes = self._raw_sysfs_string.split()
        self.entries = int(ct_attributes[0])

class CastleSysfsPerVertreeComponentTreeState:
    """
    Sysfs-derived list of all component trees in a given vertree (this is based on
    /sys/fs/castle-fs/vertrees/(vertree_id)/component_trees).
    """
    levels = None

    _raw_sysfs_data = None
    _filepath = None

    def __init__(self, vertree_id = None, filepath = None):
        """
        Can init with vertree_id or filepath, MUST provide at least one, if
        both provided, will ignore vertree_id.
        """
        if not filepath:
            if not vertree_id:
                raise CastleSysfsSyntaxException("Must init with either vertree_id or a filepath")
            assert isinstance(vertree_id, int), "vertree_id needs to be an int"
            vertree_id = hex(vertree_id)[2:] #we convert it to non-prefixed hex
            filepath = "/sys/fs/castle-fs/vertrees/{0}/component_trees".format(vertree_id)
        self._filepath = filepath
        pycastle_log.info("Generating sysfs_component_trees data from {0}".format(filepath))
        self.refresh()

    def refresh(self):
        """ Update state by reading sysfs again. """
        f = open(self._filepath, 'r')
        self._raw_sysfs_data = f.read()
        f.close()
        self._process_raw_data()

    def _process_raw_data(self):
        """ Process the text block (called automatically by refresh(). """
        sysfs_per_line = self._raw_sysfs_data.splitlines()
        del sysfs_per_line[0] #throw away the first line, it's just the counts, we don't care...

        self.levels = dict()

        for level in range(0, len(sysfs_per_line)):
            line = sysfs_per_line[level]
            cts = list()
            cts_strings = re.findall('\[(.+?)\]', line)
            for ct_string in cts_strings:
                new_ct = CastleSysfsComponentTreeState(ct_string)
                new_ct.raw_sysfs_string = ct_string
                cts.append(new_ct)
            self.levels[level]=cts

