# contrib/pg_prewarm/Makefile

MODULE_big = meta_sync
OBJS = src/gmeta.o src/comm/setup.o src/comm/bgworker.o \
		src/hook/heapam_hook.o src/fdbcli/fdbutil.o $(WIN32RES)
PG_CPPFLAGS += -I./include -I./src 
SHLIB_LINK += -L./lib -lfdb_c -lpthread

EXTENSION = gmeta
DATA = gmeta--1.0.sql 
PGFILEDESC = "gmeta - sync metadata to global storage fdb"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gmeta
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

