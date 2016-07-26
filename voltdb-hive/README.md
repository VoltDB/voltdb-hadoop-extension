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
ADD JAR voltdbclient-6.4.jar;
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

##Beeline CLI

Hive CLI has been deprecated and replaced with [Beeline](https://cwiki.apache.org/confluence/display/Hive/Replacing+the+Implementation+of+Hive+CLI+Using+Beeline). 
Beeline CLI uses [HiveServer2 clients](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients). There is no fundamental difference between Hive CLI and Beeline CLI regarding the usage of
*VoltDB Hive Storage Handler*  

With Beeline, connect Hive2:
```
$beeline
beeline>!connect jdbc:hive2://<url>:<port #> <user>
example:
!connect jdbc:hive2://localhost:10000 cloudera 
```
For complete Beeline CLI reference, check out [Beeline Commands](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-BeelineHiveCommands)

Then execute:
```
ADD JAR voltdbclient-6.4.jar;
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

Make sure that the connected user has the permission to have access to the JAR files and other resources on Hive. 
