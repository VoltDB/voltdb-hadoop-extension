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

package org.voltdb.hadoop.typeto;

import static org.voltdb.hadoop.TypeAide.*;

import java.util.EnumSet;

import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Function;

public class ByteArrayTypeTo extends TypeTo<byte[]> {

    final static EnumSet<TypeAide> loose = EnumSet.of(STRING,VARBINARY);
    final static EnumSet<TypeAide> strict = EnumSet.of(VARBINARY);

    public ByteArrayTypeTo(TypeAide type, boolean strictCompatibility) {
        super(type, strictCompatibility);
    }

    public ByteArrayTypeTo(TypeAide type) {
        this(type,true);
    }

    @Override
    public Function<byte[], Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<byte[], Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor,null,null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private static final TypeAide.Visitor<Function<byte[], Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<byte[],Object>, Void, RuntimeException>() {
                @Override
                public Function<byte[], Object> visitVarBinary(Void p, Object v) {
                    return new Function<byte[], Object>() {
                        @Override
                        final public Object apply(byte[] v) {
                            return v;
                        }
                    };
                }
                @Override
                public Function<byte[], Object> visitTinyInt(Void p, Object v) {
                    throw new IncompatibleException("byte[] is not compatible with Byte");
                }
                @Override
                public Function<byte[], Object> visitTimestamp(Void p, Object v) {
                    throw new IncompatibleException("byte[] is not compatible with Date");
                }
                @Override
                public Function<byte[], Object> visitString(Void p, Object v) {
                    return new Function<byte[], Object>() {
                        @Override
                        final public Object apply(byte[] v) {
                            if (v == null) return null;
                            return new String(v, Charsets.UTF_8);
                        }
                    };
                }
                @Override
                public Function<byte[], Object> visitSmallInt(Void p, Object v) {
                    throw new IncompatibleException("byte[] is not compatible with Short");
                }
                @Override
                public Function<byte[], Object> visitInteger(Void p, Object v) {
                    throw new IncompatibleException("byte[] is not compatible with Integer");
                }
                @Override
                public Function<byte[], Object> visitFloat(Void p, Object v) {
                    throw new IncompatibleException("byte[] is not compatible with Double");
                }
                @Override
                public Function<byte[], Object> visitDecimal(Void p, Object v) {
                    throw new IncompatibleException("byte[] is not compatible with BigDecimal");
                }
                @Override
                public Function<byte[], Object> visitBigInt(Void p, Object v) {
                    throw new IncompatibleException("byte[] is not compatible with Long");
                }
            };
}
