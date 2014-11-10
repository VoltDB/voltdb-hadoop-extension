Hadoop OutputFormat for VoltDB
==============================
This module contains both `org.apache.hadoop.mapred` and `org.apache.hadoop.mapreduce` `OutputFormat` 
implementations for VoltDB. 

There is also a Hadoop impelemtation for a VoltDB table loader that reads a TSV file from HDFS, and stores its
content on a VoltDB table

To comand line options for the loader are:
```
usage: org.voltdb.hadoop.mapred.VoltLoader [OPTION]... FILE TABLE
        -p,--password <password>            user password
        -s,--servers <HOST[:PORT][,]...>    List of VoltDB servers to connect to
                                            (default: localhost)
        -u,--user <username>                database user
```
To execute the job enter the following commands
```bash
# VOLDB_HOME is set to the location where VoltDB is installed
$ export HADOOP_CLASSPATH="voltdb-hadoop-1.0-SNAPSHOT.jar:${VOLTDB_HOME}/voltdb/voltdbclient-4.9.jar"
$ hadoop jar org.voltdb.mapred.VoltLoader /hdfs/file/loadfrom.tsv LOAD_TO_TABLE --servers HOST1,HOST2
```
