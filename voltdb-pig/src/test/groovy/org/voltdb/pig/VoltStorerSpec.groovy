/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.pig

import static org.voltdb.VoltType.*

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.pig.ResourceSchema
import org.apache.pig.data.DataByteArray
import org.apache.pig.data.DataType
import org.apache.pig.data.Tuple
import org.apache.pig.data.TupleFactory
import org.apache.pig.impl.util.UDFContext
import org.joda.time.DateTime
import org.voltdb.VoltType
import org.voltdb.hadoop.DataAdapters
import org.voltdb.hadoop.VoltConfiguration
import org.voltdb.hadoop.VoltRecord

import spock.lang.Specification

class VoltStorerSpec extends Specification {

    static String THINGS = "THINGS"
    static VoltType [] COLUMNTYPES = [INTEGER,BIGINT,FLOAT,STRING,TIMESTAMP,VARBINARY] as VoltType[]
    static TupleFactory tFactory = TupleFactory.instance

    def setupSpec() {
        VoltConfiguration.typesFor(THINGS, COLUMNTYPES)
        DataAdapters.adaptersFor(THINGS, COLUMNTYPES)
    }

    ResourceSchema schema = schemaBuilder {
        field name: 'ifield', type: DataType.INTEGER
        field name: 'lfield', type: DataType.LONG
        field name: 'dfield', type: DataType.DOUBLE
        field name: 'sfield', type: DataType.CHARARRAY
        field name: 'tfield', type: DataType.DATETIME
        field name: 'yfield', type: DataType.BYTEARRAY
    }

    Properties udfProperties = new Properties()

    Configuration conf = Mock()
    Job job = Mock()
    RecordWriter writer = Mock()
    UDFContext udfContext = Mock()
    VoltStorer voltStorer = Spy(VoltStorer.class, constructorArgs: ['{"servers":["uno","due"]}'])

    def setup() {
        conf.get(VoltConfiguration.TABLENAME_PROP) >> THINGS
        conf.getStrings(VoltConfiguration.HOSTNAMES_PROP, _ as String[]) >> {['uno','due'] as String[]}
        job.getConfiguration() >> conf
        udfContext.getUDFProperties(*_) >> udfProperties
        voltStorer.getUDFContext() >> udfContext
        voltStorer.checkSchema(schema)
    }

    def "make sure that the configuration mock works"() {
        given:
            def vc = new VoltConfiguration(conf)
        expect:
            vc.tableName == THINGS
            vc.hosts == ['uno','due'] as String[]
            vc.minimallyConfigured
    }

    def "setStoreLocation interacts with the job configuration"() {
        when:
            voltStorer.setStoreLocation('THINGS', job)
        then:
            1 * conf.set(VoltConfiguration.TABLENAME_PROP, THINGS)
            1 * conf.setStrings(VoltConfiguration.HOSTNAMES_PROP, ['uno','due'] as String[])
    }

    def "sets up the tuple adapter as expected"() {
        when:
            voltStorer.setStoreLocation('THINGS', job)
        and:
            voltStorer.prepareToWrite(writer)
        then:
            notThrown(IOException)
    }

    def "writes volt records from adapted pig tuples"() {
        given:
            voltStorer.setStoreLocation('THINGS', job)
            voltStorer.prepareToWrite(writer)
        when:
            voltStorer.putNext(tuple(pigTuple))
        then:
            1 * writer.write(_ as Text, {VoltRecord vr ->
                vr.eachWithIndex {v,i -> assert v == expected[i]}
            })
        where:
            pigTuple                                                      | expected
            [1,2L,3D,"4",new DateTime(5),new DataByteArray("6".bytes)]    | [1,2L,3D,"4",new Date(5),"6".bytes]
            [7,8L,9D,"10",new DateTime(11),new DataByteArray("12".bytes)] | [7,8L,9D,"10",new Date(11),"12".bytes]
    }

    ResourceSchema schemaBuilder(Closure fields) {
        new ResourceSchemaBuilder().make(fields);
    }

    Tuple tuple(List values) {
        tFactory.newTupleNoCopy(values)
    }
}
