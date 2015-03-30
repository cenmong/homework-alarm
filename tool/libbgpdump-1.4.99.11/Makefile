# $Id: Makefile.in,v 1.4 2009/07/22 12:09:54 ruben Exp $

CC	= gcc
CFLAGS	=  -Wall -Wcast-align -Wsystem-headers -Wno-format-y2k -W -Wno-unused-parameter -Wstrict-prototypes -Wmissing-prototypes -Wpointer-arith -Wreturn-type -Wcast-qual -Wwrite-strings -Wswitch -Wshadow -Wcast-align -Wunused-parameter -O3
INCLUDES = -I./
COMPILE  = $(CC) $(CFLAGS) $(INCLUDES)

LD	= gcc
LDFLAGS	= -shared
AR	= /usr/bin/ar
RANLIB	= ranlib

SYS_LIBS= -lbz2 -lz 

LIB_H	 = bgpdump.h bgpdump_attr.h bgpdump_formats.h bgpdump_lib.h bgpdump_mstream.h cfile_tools.h
LIB_C    = bgpdump_lib.c bgpdump_mstream.c cfile_tools.c
LIB_O	 = bgpdump_lib.o bgpdump_mstream.o cfile_tools.o

all: libbgpdump.so libbgpdump.a testbgpdump bgpdump 

libbgpdump.so: $(LIB_H) $(LIB_C) Makefile
	$(COMPILE) -fPIC -c $(LIB_C)
	$(COMPILE) $(LDFLAGS) -o libbgpdump.so $(LIB_O) $(SYS_LIBS)

libbgpdump.a: $(LIB_H) $(LIB_C) $(LIB_O) Makefile
	$(AR) r libbgpdump.a $(LIB_O)
	$(RANLIB) libbgpdump.a

testbgpdump: test.c $(LIB_H) $(LIB_C) Makefile
	$(COMPILE) -o testbgpdump test.c libbgpdump.a $(SYS_LIBS)

bgpdump: bgpdump.c $(LIB_H) $(LIB_C) Makefile
	$(COMPILE) -o bgpdump bgpdump.c libbgpdump.a $(SYS_LIBS)

clean:
	rm -f libbgpdump.so libbgpdump.a testbgpdump bgpdump *.o 

distclean: clean
	rm -rf config.log config.status *.dSYM core *.core autom4te.cache bgpdump-config.h Makefile
