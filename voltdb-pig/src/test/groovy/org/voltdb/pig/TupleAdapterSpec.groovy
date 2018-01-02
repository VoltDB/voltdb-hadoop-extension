/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import groovy.lang.Closure;

import org.apache.pig.ResourceSchema
import org.apache.pig.ResourceSchema.ResourceFieldSchema
import org.apache.pig.data.DataType
import org.apache.pig.data.TupleFactory
import org.apache.pig.data.Tuple
import org.apache.pig.data.DataByteArray
import org.joda.time.DateTime
import org.voltdb.VoltType;
import org.voltdb.hadoop.VoltRecord

import spock.lang.Specification

class TupleAdapterSpec extends Specification {

    static TupleFactory tFactory = TupleFactory.instance

    def "resource schema builder makes schemas"() {
        given:
            def schema = schemaBuilder {
                field name: 'lfield', type: DataType.LONG
                field name: 'ifield', type: DataType.INTEGER
            }
        expect:
            schema instanceof ResourceSchema
            with(schema.fields[i]) {
                type == dataType
                name == fieldName
            }
        where:
            i | dataType         | fieldName
            0 | DataType.LONG    | 'lfield'
            1 | DataType.INTEGER | 'ifield'
    }

    def "straight forward tuple adaptation"() {
        given:
            def schema = schemaBuilder {
                field name: 'ifield', type: DataType.INTEGER
                field name: 'lfield', type: DataType.LONG
                field name: 'ffield', type: DataType.FLOAT
                field name: 'dfield', type: DataType.DOUBLE
                field name: 'bfield', type: DataType.BOOLEAN
                field name: 'cfield', type: DataType.BIGDECIMAL
                field name: 'nfield', type: DataType.BIGINTEGER
                field name: 'sfield', type: DataType.CHARARRAY
                field name: 'yfield', type: DataType.BYTEARRAY
                field name: 'tfield', type: DataType.DATETIME
            }
            def volttypes = [VoltType.INTEGER, VoltType.BIGINT, VoltType.FLOAT,
                VoltType.FLOAT, VoltType.TINYINT, VoltType.DECIMAL, VoltType.DECIMAL,
                VoltType.STRING, VoltType.VARBINARY, VoltType.TIMESTAMP] as VoltType[]
        and:
            def adapter = new TupleAdapter(schema,volttypes)
        when:
            def voltrecord = adapter.adapt(tuple(pigTuple), new VoltRecord("YOLANDA"))
        then:
            voltrecord.eachWithIndex { v,i -> assert v == expected[i] }
        where:
            pigTuple                                                                                                | expected
            [1,2L,3F,4D,true,new BigDecimal(6),new BigInteger(7),"8",new DataByteArray("9".bytes),new DateTime(10)] | [1,2L,3D,4D,(byte)1,new BigDecimal(6),new BigDecimal(7),"8","9".bytes,new Date(10)]
            [null,null,null,null,null,null,null,null,null,null]                                                     | [null,null,null,null,null,null,null,null,null,null]
    }

    def "tuple adaptation requiring a few type adjustments"() {
        given:
            def schema = schemaBuilder {
                field name: 'ifield', type: DataType.INTEGER
                field name: 'lfield', type: DataType.LONG
                field name: 'dfield', type: DataType.DOUBLE
                field name: 'sfield', type: DataType.CHARARRAY
            }
            def volttypes = [VoltType.BIGINT, VoltType.TIMESTAMP, VoltType.DECIMAL,
                VoltType.VARBINARY] as VoltType[]
        and:
            def adapter = new TupleAdapter(schema,volttypes)
        when:
            def voltrecord = adapter.adapt(tuple(pigTuple), new VoltRecord("YOLANDA"))
        then:
            voltrecord.eachWithIndex { v,i -> assert v == expected[i] }
        where:
            pigTuple      | expected
            [1,2L,3D,"4"] | [1L,new Date(2),new BigDecimal(3),"4".getBytes("UTF-8")]
    }

    ResourceSchema schemaBuilder(Closure fields) {
        new ResourceSchemaBuilder().make(fields);
    }

    Tuple tuple(List values) {
        tFactory.newTupleNoCopy(values)
    }
}
