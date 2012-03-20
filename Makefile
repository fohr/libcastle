CFLAGS=-fPIC -DPIC -std=gnu99 -ggdb -Wmissing-prototypes -Wmissing-declarations -Wstrict-prototypes -Wall -Wextra -Wshadow -Werror -O2 -D_FORTIFY_SOURCE=2
LIB_DESTDIR=$(DESTDIR)/usr/lib64
INC_DESTDIR=$(DESTDIR)/usr/include/castle
EXE_DESTDIR=$(DESTDIR)/usr/sbin

SONAME=libcastle.so.1
CASTLE_IOCTLS_EXENAME=castle_ioctl_cmd

all: $(SONAME) $(CASTLE_IOCTLS_EXENAME)

%.o: %.c *.h
	gcc -pthread -c -o $@ $< $(CFLAGS)

$(SONAME): castle_front.o castle_ioctl.o castle_convenience.o castle_print.o castle_utils.o
	gcc -pthread -shared -Wl,-Bsymbolic -Wl,-soname,$(SONAME) -Wl,--warn-common -Wl,--fatal-warnings -Wl,--version-script=versions -o $@ $^ $(CFLAGS)

$(CASTLE_IOCTLS_EXENAME): castle_ioctl.c *.h
	gcc -lcastle -o $@ castle_ioctl.c $(CFLAGS)

install: $(SONAME) $(CASTLE_IOCTLS_EXENAME)
	mkdir -p $(LIB_DESTDIR)
	install $(SONAME) $(LIB_DESTDIR)
	ln -sf $(SONAME) $(LIB_DESTDIR)/libcastle.so 
	if [ -z "$(DONT_RUN_LDCONFIG)" ]; then \
	    /sbin/ldconfig; \
	fi

	mkdir -p $(INC_DESTDIR)
	install castle_public.h $(INC_DESTDIR)
	install castle.h $(INC_DESTDIR)

	mkdir -p $(EXE_DESTDIR)
	install $(CASTLE_IOCTLS_EXENAME) $(EXE_DESTDIR)

.PHONY: tags
tags:
	ctags *.c *.h

.PHONY: cscope

cscope:
	cscope -b -q *.c *.h

clean:
	rm -rf *.o *.so* cscope*
