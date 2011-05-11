CFLAGS=-fPIC -DPIC -std=gnu99 -ggdb -Wmissing-prototypes -Wmissing-declarations -Wstrict-prototypes -Wall -Wextra -Wshadow -Werror -O2
LIB_DESTDIR=$(DESTDIR)/usr/lib64
INC_DESTDIR=$(DESTDIR)/usr/include/castle

SONAME=libcastle.so.1

all: $(SONAME)

%.o: %.c *.h
	gcc -pthread -c -o $@ $< $(CFLAGS)

$(SONAME): castle_front.o castle_ioctl.o castle_convenience.o castle_print.o castle_utils.o
	gcc -pthread -shared -Wl,-Bsymbolic -Wl,-soname,$(SONAME) -Wl,--warn-common -Wl,--fatal-warnings -Wl,--version-script=versions -o $@ $^ $(CFLAGS)

install: $(SONAME)
	mkdir -p $(LIB_DESTDIR)
	install $(SONAME) $(LIB_DESTDIR)
	ln -sf $(SONAME) $(LIB_DESTDIR)/libcastle.so 
	if [ -z "$(DONT_RUN_LDCONFIG)" ]; then \
	    /sbin/ldconfig; \
	fi

	mkdir -p $(INC_DESTDIR)
	install castle_public.h $(INC_DESTDIR)
	install castle.h $(INC_DESTDIR)

clean:
	rm -rf *.o *.so*
