VoltDB Hive Storage Handler
===========================

This module contains the implementation of a VoltDB Hive storage handler. This handler allows you to define 
_insert_ only Hive table that map to coressponding VoltDB tables, so when you insert a row in to a table backed 
by `org.voltdb.hive.VoltStorageHandler` the row is written to the configured destination VoltDB table.

It suports the following Hive column types:

* `tinyint`
* `smallint`
* `int`
* `bigint`
* `double`
* `timeststamp`
* `decimal(26,12)`
* `string`
* `binary`

and it accepts the following configuration properties

* `voltdb.servers` _(required)_ comma separated list of VoltDB servers that comprise a VoltDB cluster
* `voltdb.table` _(required)_ destination VoltDB table
* `voltdb.user` _(optional)_ VoltDB user name
* `voltdb.password` _(optional)_ VoltDB user password

##Usage
```sql
ADD JAR voltdbclient-4.9.jar;
ADD JAR voltdb-hadoop-1.0-SNAPSHOT.jar;
ADD JAR voltdb-hive-1.0-SNAPSHOT.jar;

CREATE TABLE VOLTSINK (
   IFIELD INT,
   LFIELD BIGINT,
   FFIELD DOUBLE,
   SFIELD STRING,
   TFIELD TIMESTAMP,
   BFIELD BINARY
) STORED BY 'org.voltdb.hive.VoltStorageHandler' 
  WITH SERDEPROPERTIES(
      'voltdb.table'='LOADME',
      'voltdb.servers'='stefanows'
);

INSERT INTO TABLE VOLTSINK SELECT * FROM SOURCE_TABLE;
```

The VOLTSINK column types match the number, order, and types of the VoltDB table LOADME columns.
