#!/bin/bash

rm libcastle.py *.c *.pyc *.o *.so
set -e

echo running swig - generating libcastle_wrap.c
swig -verbose -debug-csymbols -debug-lsymbols -I/usr/include -builtin -python libcastle.i

echo massaging libcastle_wrap.c
tmpfilesuffix=$$.`date +%s`
tmpfile=libcastle_wrap.c.tmp.$tmpfilesuffix
cp libcastle_wrap.c $tmpfile.1

sed 's/\->swig_ioctl_u//' $tmpfile.1 > $tmpfile.2
sed 's/\->swig_req_u//' $tmpfile.2 > $tmpfile.3
sed 's/\->swig_val_src_u//' $tmpfile.3 > $tmpfile.4

cp $tmpfile.4 libcastle_wrap.c
rm $tmpfile.*

echo compiling wrapper
gcc -fPIC -DPIC -std=gnu99 -ggdb -O2 -D_FORTIFY_SOURCE=2 -o libcastle_wrap.o -c libcastle_wrap.c -I/usr/include/python2.6

echo making so
ld -shared libcastle_wrap.o -lcastle -o _libcastle.so
