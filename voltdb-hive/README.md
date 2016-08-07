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
* `voltdb.batchSize` _(optional)_ VoltDB BulkLoader batch size
* `voltdb.clientTimeout` _(optional)_ VoltDB client time out in milliseconds
* `voltdb.maxErrors` _(optional)_ VoltDB BulkLoader maximal number of errors allowed
* `voltdb.upsert` _(optional)_ VoltDB BulkLoader in upsert mode. Value 'true' or 'false'

##Usage

Hive CLI has been deprecated and replaced with [Beeline](https://cwiki.apache.org/confluence/display/Hive/Replacing+the+Implementation+of+Hive+CLI+Using+Beeline). 
Beeline CLI uses [HiveServer2 clients](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients). There is no fundamental difference between Hive CLI and Beeline CLI regarding the usage of
*VoltDB Hive Storage Handler*  

$beeline

beeline>!connect jdbc:hive2://<url>:<port #> <user>
example:
```
!connect jdbc:hive2://localhost:10000 cloudera 
```

For complete Beeline CLI reference, check out [Beeline Commands](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-BeelineHiveCommands)


Then add voltdbclient-6.4.jar, voltdb-hadoop-1.0-SNAPSHOT.jar and voltdb-hive-1.0-SNAPSHOT.jar in order;

example:
```
(1) jdbc:hive2://localhost:10000> ADD JAR /home/cloudera/voltdb-ent-6.4/voltdb/voltdbclient-6.4.jar;
(2) jdbc:hive2://localhost:10000> ADD JAR /home/cloudera/voltdb-hadoop-1.1-SNAPSHOT.jar;
(3) jdbc:hive2://localhost:10000> ADD JAR /home/cloudera/voltdb-hive-1.1-SNAPSHOT.jar;
```

then create table on VoltDB and create table on Hive:
```
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
      'voltdb.servers'='stefanows',
      'voltdb.batchSize'='50', 
      'voltdb.clientTimeout'='600000',
      'voltdb.maxErrors'='100'); 
);

The VOLTSINK column types match the number, order, and types of the VoltDB table LOADME columns.
```

create a source table on Hive and load data into the source table  and then execute the following query:

```
INSERT INTO TABLE VOLTSINK SELECT * FROM SOURCE_TABLE;
```

*VoltDB Hive Storage Handler*  will continuously move data from Hive to VoltDB. 

