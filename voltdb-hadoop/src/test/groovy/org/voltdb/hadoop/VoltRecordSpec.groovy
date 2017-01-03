/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.voltdb.VoltType

import spock.lang.Specification

class VoltRecordSpec extends Specification {

    static String THINGS = "THINGS"
    static VoltType [] COLUMNTYPES = [INTEGER,BIGINT,FLOAT,STRING,TIMESTAMP,VARBINARY] as VoltType[]

    def setupSpec() {
        VoltConfiguration.typesFor(THINGS, COLUMNTYPES)
        DataAdapters.adaptersFor(THINGS, COLUMNTYPES)
    }

    def "caches are setup properly"() {
        when:
            def types = VoltConfiguration.typesFor(THINGS, null)
            def signature = DataAdapters.adaptersFor(THINGS, null).getSignature()
        then:
            types == COLUMNTYPES
            signature == UUID.fromString('08133dba-9305-9f91-36fd-46970bbc51e4')
    }

    def "valid records are serialized properly"() {
        given: "output conduits are setup"
            def baos = new ByteArrayOutputStream(1024)
            def dos = new DataOutputStream(baos)
        and:
            vro.write(dos)
            vri.readFields(getDataInputStreamFrom(baos))
        expect:
            vri.eachWithIndex { f,i -> assert f == vro.get(i) }
        where:
            vro                                                                          | vri
            new VoltRecord(THINGS, 1,2L, 3.33D, "voltrecord", new Date(), "bytes".bytes) | new VoltRecord()
            new VoltRecord(THINGS, 2,2L, null,  "voltrecord", null,       "bytes".bytes) | new VoltRecord()
    }

    def "throws on erroneous conditions"() {
        given:
            def baos = new ByteArrayOutputStream(1024)
            def dos = new DataOutputStream(baos)
        when:
            vro.write(dos)
        then:
            thrown(exception)
        where:
            vro                                                                               | exception
            new VoltRecord(THINGS,  1,2L, 3.33D, "voltrecord", new Date())                    | IOException
            new VoltRecord(THINGS,  2,2L, 3.33D, "voltrecord", new Date(), "bytes".bytes, 23) | IOException
            new VoltRecord("OTHER", 3,2L, 3.33D, "voltrecord", new Date(), "bytes".bytes)     | IOException
            new VoltRecord(THINGS,  4,2L, "voltrecord", 3.33D, new Date(), "bytes".bytes)     | ClassCastException
    }

    def getDataInputStreamFrom(ByteArrayOutputStream baos) {
        new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))
    }
}
