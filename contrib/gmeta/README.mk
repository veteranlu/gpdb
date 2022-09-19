Global Metadata Mangement Service GP EXTENSION

# 1. Description
gmeta(global metadata) is the global metdata management extension, the feature as following:
1. Trace the system table's operation, and sync the difference to FDB, which implement by heapam hook
2. Provide column storage's visibility metadata access interface, and sync the metadata to FBD 

# 2. Source code preview
gmeta contain following modules:
- src 
-- fdbcli:  provide fdbcli's utility 
-- hook: hooker implement, contains heapam' hook implement, xact's hook implement, etc.
-- comm: communication service, which setup background communication worker, accept 
	front backend's metadata request, and R/W metadata to fdb, the front and bgworker
	communication with PG's shared-memory message queue.
-- mvcc: implement the mvcc

# 3. How to contribute

1. setup your develop enviroment.
gmeta dependence fdb, for development enviroment, you can setup the fdb env by docker,
```
docker load -i fdbdev.tar 
# suppose your source code directory is /Users/kaka/Workspace/
docker run -v/Users/kaka/Workspace/:/workspace --privileged --cap-add SYS_ADMIN -e container=docker -it --name=gp -d --restart=always gpdb_dev:latest /usr/sbin/init
docker exec -it fdbdev bash

```

2. build, test
```
make 
make install
```

3. use the extension
add extension to preload libraray
```
gpconfig -c shared_preload_libraries -v "gmeta"
```

create extension
```
CREATE EXTENSION gmeta;
```

create table from db1 and select pg_class and pg_attribute info from db2
```
create database db1;
create database db2;

\c db1
create extension gmeta; 
-- gmeta's hook would trace system table, and write to FDB
create table t1(a int, b int, c varchar);

\c db2
-- get system table from FDB 
select pg_show_gmetadata('db1'::regclass, 't1'::regclass);
``` 

4. code style
