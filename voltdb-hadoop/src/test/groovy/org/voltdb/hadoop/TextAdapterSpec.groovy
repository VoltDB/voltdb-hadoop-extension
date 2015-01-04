/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.hadoop

import static org.voltdb.VoltType.*

import org.apache.hadoop.io.Text
import org.voltdb.VoltType
import org.voltdb.common.Constants

import spock.lang.Specification

class TextAdapterSpec extends Specification {

    static String      THINGS = "THINGS"
    static VoltType [] COLUMNTYPES = [INTEGER,BIGINT,FLOAT,STRING,TIMESTAMP,VARBINARY] as VoltType[]

    static Date   BINDATE = new Date()
    static String STRDATE = BINDATE.format(Constants.ODBC_DATE_FORMAT_STRING)
    static String NULL = "\\N"
    static byte[] BYTES = "bytes".bytes

    def setupSpec() {
        VoltConfiguration.typesFor(THINGS, COLUMNTYPES)
        DataAdapters.adaptersFor(THINGS, COLUMNTYPES)
    }

    def "text instance adapts to volt record"() {
        given:
            def adptr = new TextInputAdapter(COLUMNTYPES)
            def rec   = adptr.adapt(new Text(input.toString()), new VoltRecord(THINGS))
        expect:
            rec.eachWithIndex { f,i -> assert f == vals[i] }
        where:
            input                                                         | vals
            "1\t23\t33.4\thello kitty\t${STRDATE}\t${BYTES.encodeHex()}"  | [1,23L,33.4D,"hello kitty",BINDATE,BYTES]
            "2\t23\t33.4\t\t${STRDATE}\t${BYTES.encodeHex()}"             | [2,23L,33.4D,"",BINDATE,BYTES]
            "3\t23\t33.4\t${NULL}\t${STRDATE}\t${BYTES.encodeHex()}"      | [3,23L,33.4D,null,BINDATE,BYTES]
            "4\t${NULL}\t33.4\t${NULL}\t${STRDATE}\t${BYTES.encodeHex()}" | [4,null,33.4D,null,BINDATE,BYTES]
            "5\t\t46.4\t${NULL}\t${STRDATE}\t}"                           | [5,null,46.4D,null,BINDATE, new byte[0]]
            "6\t\t46.4\t${NULL}\t${STRDATE}\t${NULL}"                     | [6,null,46.4D,null,BINDATE, null]
    }

    def "throws on non conformant text instances"() {
        given:
            def adptr = new TextInputAdapter(COLUMNTYPES)
        when:
            adptr.adapt(new Text(input.toString()), new VoltRecord(THINGS))
        then:
            thrown(exception)
        where:
            input                                                                  | exception
            "1\t23\t33.4\thello kitty\t${STRDATE}\t${BYTES.encodeHex()}\tanother"  | IllegalArgumentException
            "2\t23\t33.4\thello kitty\t${STRDATE}\t${BYTES.encodeHex()}\t${NULL}"  | IllegalArgumentException
            "3\t23\t33.4\thello kitty\t${STRDATE}\t${BYTES.encodeHex()}\t"         | IllegalArgumentException
            "1\t23\thello kitty\t33.4\t${STRDATE}\t${BYTES.encodeHex()}"           | NumberFormatException
            "1\t23\t33.4\thello kitty\tnot-a-date\t${BYTES.encodeHex()}"           | RuntimeException
            "\t" * 1024                                                            | IllegalArgumentException
            "1\t23"                                                                | IllegalArgumentException
    }

    def "record formats correctly into text instance"() {
        given:
            def adptr = new TextOutputAdapter(COLUMNTYPES)
        expect:
            adptr.adapt(null, new VoltRecord(THINGS,fields)).toString() == output
        where:
            fields                                    | output
            [1,23L,33.4D,"hello kitty",BINDATE,BYTES] | "1\t23\t33.4\thello kitty\t${STRDATE}\t${BYTES.encodeHex()}"
            [2,23L,33.4D,"",BINDATE,BYTES]            | "2\t23\t33.4\t\t${STRDATE}\t${BYTES.encodeHex()}"
            [3,23L,33.4D,null,BINDATE,BYTES]          | "3\t23\t33.4\t${NULL}\t${STRDATE}\t${BYTES.encodeHex()}"
            [4,null,33.4D,null,BINDATE,BYTES]         | "4\t\t33.4\t${NULL}\t${STRDATE}\t${BYTES.encodeHex()}"
            [5,null,46.4D,null,BINDATE, new byte[0]]  | "5\t\t46.4\t${NULL}\t${STRDATE}\t"
            [6,null,46.4D,null,BINDATE, null]         | "6\t\t46.4\t${NULL}\t${STRDATE}\t${NULL}"
    }

}
