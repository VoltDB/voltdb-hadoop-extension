/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.voltdb.hive

import static org.voltdb.VoltType.*

import java.sql.Timestamp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde.serdeConstants;
import org.voltdb.VoltType
import org.voltdb.hadoop.DataAdapters
import org.voltdb.hadoop.VoltConfiguration
import org.voltdb.hadoop.VoltRecord

import spock.lang.Specification
import spock.lang.Subject;

class VoltSerDeSpec extends Specification {

    static String THINGS = "THINGS"
    static VoltType [] COLUMNTYPES = [INTEGER,BIGINT,FLOAT,STRING,TIMESTAMP,VARBINARY] as VoltType[]

    static Properties prop = [
        "${serdeConstants.LIST_COLUMNS}" : 'fi,fl,fd,fs,ft,fb',
        "${serdeConstants.LIST_COLUMN_TYPES}" : 'int,bigint,double,string,timestamp,binary',
        "${VoltSerDe.SERVERS_PROP}" : 'uno,due',
        "${VoltSerDe.TABLE_PROP}" : THINGS
    ] as Properties

    def setupSpec() {
        VoltConfiguration.typesFor(THINGS, COLUMNTYPES)
        DataAdapters.adaptersFor(THINGS, COLUMNTYPES)
    }

    Configuration conf = Mock()

    @Subject
    VoltSerDe serde = new VoltSerDe();

    def setup() {
        conf.get(VoltConfiguration.TABLENAME_PROP) >> THINGS
        conf.getStrings(VoltConfiguration.HOSTNAMES_PROP, _ as String[]) >> {['uno','due'] as String[]}
    }

    def "interacts with configuration as expected"() {
        when:
            serde.initialize(conf, prop)
        then:
            1 * conf.set(VoltConfiguration.TABLENAME_PROP, THINGS)
            1 * conf.setStrings(VoltConfiguration.HOSTNAMES_PROP, ['uno','due'] as String[])
    }

    def "serde serializes to a volt record"() {
        given:
            serde.initialize(conf, prop)
            def oi = serde.objectInspector
        when:
            def vr = serde.serialize(bees, oi)
        then:
           vr.eachWithIndex {v,i -> assert v == expected[i]}
        where:
            bees                                          | expected
            [1,2L,3D,"fourth",new Timestamp(5),"6".bytes] | [1,2L,3D,"fourth",new Date(5),"6".bytes]
    }

    def "does not accept unsupported or unmatched order of column types"() {
        given:
            def tprop = new Properties() << prop
            tprop[serdeConstants.LIST_COLUMN_TYPES] = columnTypes
            tprop[serdeConstants.LIST_COLUMNS] = columnNames
        when:
            serde.initialize(conf,tprop)
        then:
            thrown(exception)
        where:
            columnNames           | columnTypes                                          | exception
            'c1_f1,f2'            | 'int,double'                                         | VoltSerdeException
            'c2_f1,f2,f3,f4,f5,f6'| 'bigint,int,double,string,timestamp,binary'          | VoltSerdeException
            'c3_f1,f2,f3,f4,f5,f6'| 'int,bigint,double,string,timestamp'                 | VoltSerdeException
            'c4_f1,f2,f3,f4,f5,f6'| 'int,bigint,float,string,timestamp,binary'           | VoltSerdeException
            'c5_f1,f2,f3,f4,f5,f6'| 'int,bigint,double,string,timestamp,map<int,string>' | VoltSerdeException
    }
}
