#!/bin/bash

rm libcastle.py *.c *.pyc *.o *.so
set -e

echo running swig - generating libcastle_wrap.c
swig -verbose -debug-csymbols -debug-lsymbols -I/usr/include -builtin -python libcastle.i

echo compiling wrapper
gcc -fPIC -DPIC -std=gnu99 -ggdb -O2 -D_FORTIFY_SOURCE=2 -o libcastle_wrap.o -c libcastle_wrap.c -I/usr/include/python2.6

echo making so
ld -shared libcastle_wrap.o -lcastle -o _libcastle.so
