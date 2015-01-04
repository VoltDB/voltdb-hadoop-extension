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

package org.voltdb.hadoop.typeto;

import static org.voltdb.hadoop.TypeAide.BIGINT;
import static org.voltdb.hadoop.TypeAide.DECIMAL;
import static org.voltdb.hadoop.TypeAide.FLOAT;
import static org.voltdb.hadoop.TypeAide.INTEGER;
import static org.voltdb.hadoop.TypeAide.STRING;
import static org.voltdb.hadoop.TypeAide.TIMESTAMP;
import static org.voltdb.hadoop.TypeAide.VARBINARY;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Function;

public class IntegerTypeTo extends TypeTo<Integer> {

    final static EnumSet<TypeAide> loose =
            EnumSet.complementOf(EnumSet.of(TIMESTAMP));
    final static EnumSet<TypeAide> strict =
            EnumSet.of(DECIMAL,STRING,INTEGER,BIGINT,VARBINARY,FLOAT);

    public IntegerTypeTo(TypeAide type, boolean strictCompatibility) {
        super(type, strictCompatibility);
    }

    public IntegerTypeTo(TypeAide type) {
        this(type,true);
    }

    @Override
    public Function<Integer, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<Integer, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private static final TypeAide.Visitor<Function<Integer, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<Integer,Object>, Void, RuntimeException>() {
                @Override
                public Function<Integer, Object> visitVarBinary(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE>>3);
                            return bb.putInt(v).array();
                        }
                    };
                }
                @Override
                public Function<Integer, Object> visitTinyInt(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            if (v <= Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                                throw new IncompatibleException("Integer(" +v+") to Byte");
                            }
                            return v.byteValue();
                        }
                    };
                }
                @Override
                public Function<Integer, Object> visitTimestamp(Void p, Object v) {
                    throw new IncompatibleException("Integer is not compatible with Date");
                }
                @Override
                public Function<Integer, Object> visitString(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            return v.toString();
                        }
                    };
                }
                @Override
                public Function<Integer, Object> visitSmallInt(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            if (v <= Short.MIN_VALUE || v > Short.MAX_VALUE) {
                                throw new IncompatibleException("Integer(" +v+") to Short");
                            }
                            return v.shortValue();
                        }
                    };
                }
                @Override
                public Function<Integer, Object> visitInteger(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            return v.intValue();
                        }
                    };
                }
                @Override
                public Function<Integer, Object> visitFloat(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            return v.doubleValue();
                        }
                    };
                }
                @Override
                public Function<Integer, Object> visitDecimal(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            return new BigDecimal(v);
                        }
                    };
                }
                @Override
                public Function<Integer, Object> visitBigInt(Void p, Object v) {
                    return new Function<Integer, Object>() {
                        @Override
                        final public Object apply(Integer v) {
                            if (v == null) return null;
                            return v.longValue();
                        }
                    };
                }
            };
}
