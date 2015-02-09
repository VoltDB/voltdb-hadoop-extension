VoltDB Hadoop Extension
=======================
This repository contains implementations of the VoltDB Hadoop OutputFormat, along side two of its 
implementations:

* A VoltLoader hadoop job that loads a TSV file in HDFS into a VoltDB table
* A Pig `StoreFunc` implementation that can be used to load a Pig data flow into a VoltDB table
* A Hive `StorageHandler` implementation that allows Hive insert operations to write to VoltDB tables

## Build Instuctions

Build by invoking the gradlew script
```bash
$ ./gradlew jar
```
this builds jar artifacts in `voltb-{hadoop,hive,pig}/build/libs/`

To setup eclipse projects to play around with the sources and tests, invoke the
eclipse task as follows:
```bash
$ ./gradlew eclipse
```
Invoke gradlew as follows for more information on what other build tasks are available
```bash
$ ./gradlew tasks --all
```
