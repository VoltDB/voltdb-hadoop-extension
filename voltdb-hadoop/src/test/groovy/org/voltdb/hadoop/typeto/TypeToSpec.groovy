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

package org.voltdb.hadoop.typeto

import static java.nio.ByteBuffer.allocate as newbb
import static org.voltdb.hadoop.TypeAide.*
import static org.voltdb.types.VoltDecimalHelper.serializeBigDecimal

import org.voltdb.hadoop.TypeAide

import spock.lang.Specification

class TypeToSpec extends Specification {

    def "byte type_to adjusts correctly"() {
        given:
            def typeto = new ByteTypeTo(TINYINT)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data      | expected
            TINYINT   | (byte)120 | (byte)120
            SMALLINT  | (byte)121 | (short)121
            INTEGER   | (byte)122 | 122
            BIGINT    | (byte)123 | 123L
            FLOAT     | (byte)124 | 124D
            DECIMAL   | (byte)125 | new BigDecimal(125)
            STRING    | (byte)126 | "126"
            VARBINARY | (byte)119 | [(byte)119] as byte[]
            TINYINT   | null      | null
            SMALLINT  | null      | null
            INTEGER   | null      | null
            BIGINT    | null      | null
            FLOAT     | null      | null
            DECIMAL   | null      | null
            STRING    | null      | null
            VARBINARY | null      | null
    }

    def "byte type_to throws for incompatible types"() {
        when:
            def typeto = new ByteTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TIMESTAMP | _
    }

    def "smallint type_to adjusts correctly"() {
        given:
            def typeto = new ShortTypeTo(SMALLINT)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data       | expected
            SMALLINT  | (short)501 | (short)501
            INTEGER   | (short)502 | 502
            BIGINT    | (short)503 | 503L
            FLOAT     | (short)504 | 504D
            DECIMAL   | (short)505 | new BigDecimal(505)
            STRING    | (short)506 | "506"
            VARBINARY | (short)507 | newbb(2).putShort((short)507).array()
            SMALLINT  | null       | null
            INTEGER   | null       | null
            BIGINT    | null       | null
            FLOAT     | null       | null
            DECIMAL   | null       | null
            STRING    | null       | null
            VARBINARY | null       | null
    }

    def "smallint type_to throws for incompatible types"() {
        when:
            def typeto = new ShortTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TINYINT   | _
            TIMESTAMP | _
    }

    def "integer type_to adjusts correctly"() {
        given:
            def typeto = new IntegerTypeTo(INTEGER)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data   | expected
            INTEGER   | 700001 | 700001
            BIGINT    | 700002 | 700002L
            DECIMAL   | 700003 | new BigDecimal(700003)
            STRING    | 700004 | "700004"
            VARBINARY | 700005 | newbb(4).putInt(700005).array()
            FLOAT     | 700006 | 700006D
            INTEGER   | null   | null
            BIGINT    | null   | null
            DECIMAL   | null   | null
            STRING    | null   | null
            VARBINARY | null   | null
            FLOAT     | null   | null
    }

    def "integer type_to throws for incompatible types"() {
        when:
            def typeto = new IntegerTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TINYINT   | _
            SMALLINT  | _
            TIMESTAMP | _
    }

    def "bigint type_to adjusts correctly"() {
        given:
            def typeto = new LongTypeTo(BIGINT)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data           | expected
            BIGINT    | 1415235600000L | 1415235600000L
            DECIMAL   | 1415235601000L | new BigDecimal(1415235601000L)
            STRING    | 1415235602000L | "1415235602000"
            VARBINARY | 1415235603000L | newbb(8).putLong(1415235603000L).array()
            FLOAT     | 1415235604000L | 1415235604000D
            TIMESTAMP | 1415235605000L | new Date(1415235605000L)
            BIGINT    | null           | null
            DECIMAL   | null           | null
            STRING    | null           | null
            VARBINARY | null           | null
            FLOAT     | null           | null
            TIMESTAMP | null           | null
    }

    def "bigint type_to throws for incompatible types"() {
        when:
            def typeto = new LongTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TINYINT   | _
            SMALLINT  | _
            INTEGER   | _
    }

    def "float type_to adjusts correctly"() {
        given:
            def typeto = new DoubleTypeTo(FLOAT)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data            | expected
            DECIMAL   | 1415235601.001D | new BigDecimal(1415235601.001D)
            STRING    | 1415235602.002D | "1.415235602002E9"
            VARBINARY | 1415235603.003D | newbb(8).putDouble(1415235603.003D).array()
            FLOAT     | 1415235604.004D | 1415235604.004D
            TIMESTAMP | 1415235605.005D | new Date(1415235605005L)
            DECIMAL   | null            | null
            STRING    | null            | null
            VARBINARY | null            | null
            FLOAT     | null            | null
            TIMESTAMP | null            | null
    }

    def "float type_to throws for incompatible types"() {
        when:
            def typeto = new DoubleTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TINYINT   | _
            SMALLINT  | _
            INTEGER   | _
            BIGINT    | _
    }

    def "timestamp type_to adjusts correctly"() {
        given:
            def typeto = new DateTypeTo(TIMESTAMP)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data                     | expected
            BIGINT    | new Date(1415235600000L) | 1415235600000L
            DECIMAL   | new Date(1415235601001L) | new BigDecimal(1415235601001L)
            STRING    | new Date(1415235602000L) | "2014-11-05 20:00:02.000"
            VARBINARY | new Date(1415235603000L) | newbb(8).putLong(1415235603000L).array()
            FLOAT     | new Date(1415235604004L) | 1415235604.004D
            TIMESTAMP | new Date(1415235605000L) | new Date(1415235605000L)
            BIGINT    | null                     | null
            DECIMAL   | null                     | null
            STRING    | null                     | null
            VARBINARY | null                     | null
            FLOAT     | null                     | null
            TIMESTAMP | null                     | null
    }

    def "timestamp type_to throws for incompatible types"() {
        when:
            def typeto = new DateTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TINYINT   | _
            SMALLINT  | _
            INTEGER   | _
    }

    def "decimal type_to adjusts correctly"() {
        given:
            def typeto = new BigDecimalTypeTo(DECIMAL)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data                                              | expected
            DECIMAL   | new BigDecimal("12345678901234567890123456.1234") | new BigDecimal("12345678901234567890123456.1234")
            STRING    | new BigDecimal("12345678901234567890123456.1234") | "12345678901234567890123456.1234"
            VARBINARY | new BigDecimal("12345678901234567890123456.1234") | serializeBigDecimal(new BigDecimal("12345678901234567890123456.1234"))
            DECIMAL   | null                                              | null
            STRING    | null                                              | null
            VARBINARY | null                                              | null
    }

    def "decimal type_to throws for incompatible types"() {
        when:
            def typeto = new BigDecimalTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TINYINT   | _
            SMALLINT  | _
            INTEGER   | _
            TIMESTAMP | _
            FLOAT     | _
            BIGINT    | _
    }

    def "string type_to adjusts correctly"() {
        given:
            def typeto = new StringTypeTo(STRING)
            def adjust = typeto.getAdjusterFor(target)
        when:
            def result = adjust.apply(data)
        then:
            result == expected
        where:
            target    | data                       | expected
            STRING    | "yolanda is partying at 1" | "yolanda is partying at 1"
            VARBINARY | "yolanda is partying at 2" | "yolanda is partying at 2".getBytes("UTF-8")
            STRING    | null                       | null
            VARBINARY | null                       | null
            STRING    | "\\N"                      | null
            VARBINARY | "\\N"                      | null
    }

    def "string type_to throws for incompatible types"() {
        when:
            def typeto = new StringTypeTo(target)
        then:
            thrown(IncompatibleException)
        where:
            target    | _
            TINYINT   | _
            SMALLINT  | _
            INTEGER   | _
            TIMESTAMP | _
            FLOAT     | _
            BIGINT    | _
            DECIMAL   | _
    }
}
