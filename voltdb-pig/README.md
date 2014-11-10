VoltDB Pig Connector
====================
This module contains the inplementation of a Pig store function for VoltDB. The store funtion takes a Pig data stream, 
and loads it into a VoltDB table

```pig
REGISTER 'voltdb-pig-*.jar'
REGISTER 'voltdb-hadoop-*.jar'
REGISTER 'voltdbclient*.jar'

STORE data_stream INTO 'VOLT_TABLE' 
  using org.voldb.pig.VoltStorer('{"servers":["host1","host2"]}');
```

The VoltStorer constructor takes the a JSON document with the following fields
* servers: an array host names where a VoltDB cluster is running
* user: a volt user name
* password: a volt user password
