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

package org.voltdb.hadoop.typeto;

import static org.voltdb.hadoop.TypeAide.DECIMAL;
import static org.voltdb.hadoop.TypeAide.FLOAT;
import static org.voltdb.hadoop.TypeAide.STRING;
import static org.voltdb.hadoop.TypeAide.VARBINARY;
import static org.voltdb.hadoop.TypeAide.TIMESTAMP;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.EnumSet;

import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Function;

public class DoubleTypeTo extends TypeTo<Double> {

    final static EnumSet<TypeAide> loose = EnumSet.allOf(TypeAide.class);
    final static EnumSet<TypeAide> strict =
            EnumSet.of(DECIMAL,STRING,FLOAT,VARBINARY,TIMESTAMP);

    public DoubleTypeTo(TypeAide type, boolean strictCompatibility) {
        super(type,strictCompatibility);
    }

    public DoubleTypeTo(TypeAide type) {
        this(type,true);
    }

    @Override
    public Function<Double, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<Double, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private static final TypeAide.Visitor<Function<Double, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<Double,Object>, Void, RuntimeException>() {
                @Override
                public Function<Double, Object> visitVarBinary(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            ByteBuffer bb = ByteBuffer.allocate(Double.SIZE>>3);
                            return bb.putDouble(v).array();
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitTinyInt(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            if (v <= Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                                throw new IncompatibleException("Double("+v+") to Byte");
                            }
                            return v.byteValue();
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitTimestamp(Void p, Object v)
                        throws RuntimeException {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            long ts = (long)(v * 1000);
                            return new Date(ts);
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitString(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            return v.toString();
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitSmallInt(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            if (v <= Short.MIN_VALUE || v > Short.MAX_VALUE) {
                                throw new IncompatibleException("Double(" +v+") to Short");
                            }
                            return v.shortValue();
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitInteger(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            if (v <= Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                                throw new IncompatibleException("Double("+v+") to Integer");
                            }
                            return v.intValue();
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitFloat(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            return v.doubleValue();
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitDecimal(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            return new BigDecimal(v);
                        }
                    };
                }
                @Override
                public Function<Double, Object> visitBigInt(Void p, Object v) {
                    return new Function<Double, Object>() {
                        @Override
                        final public Object apply(Double v) {
                            if (v == null) return null;
                            if (v <= Long.MIN_VALUE || v > Long.MAX_VALUE) {
                                throw new IncompatibleException("Double("+v+") to Integer");
                            }
                            return v.longValue();
                        }
                    };
                }
            };

}
