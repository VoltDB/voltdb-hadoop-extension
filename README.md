VoltDB Hadoop Extension
=======================
This repository contains implementations of the VoltDB Hadoop OutputFormat, along side two its implementations:

* A VoltLoader hadoop job that loads a TSV file in HDFS into a VoltDB table
* A Pig StoreFunc implementation that can be used to load a Pig data flow into a VoltDB table

## Build Instuctions
Copy the gradle.properties-template file in voltdb-hadoop to voltdb-hadoop/gradle.properties
```bash
$ cp voltdb-hadoop/gradle.properties-template voltdb-hadoop/gradle.properties
```
Edit `voltdb-hadoop/gradle.properties`, and set the `voltdbHome` property to the location where your VoltDB distribution resides.

Build by invoking the gradlew script
```bash
$ ./gradlew jar
```
this builds jar artifacts in `voltb-{hadoop,pig}/build/libs/`

Invoke gradlew as follows for more information on what other build tasks are available
```bash
$ ./gradlew tasks --all
```
