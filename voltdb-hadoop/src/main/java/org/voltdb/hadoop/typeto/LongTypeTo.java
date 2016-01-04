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

import static org.voltdb.hadoop.TypeAide.BIGINT;
import static org.voltdb.hadoop.TypeAide.DECIMAL;
import static org.voltdb.hadoop.TypeAide.STRING;
import static org.voltdb.hadoop.TypeAide.TIMESTAMP;
import static org.voltdb.hadoop.TypeAide.VARBINARY;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.EnumSet;

import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Function;

public class LongTypeTo extends TypeTo<Long>{

    final static EnumSet<TypeAide> loose = EnumSet.allOf(TypeAide.class);
    final static EnumSet<TypeAide> strict =
            EnumSet.of(DECIMAL,STRING,BIGINT,VARBINARY,TIMESTAMP);

    public LongTypeTo(TypeAide type, boolean strictCompatibility) {
        super(type,strictCompatibility);
    }

    public LongTypeTo(TypeAide type) {
        this(type,true);
    }

    @Override
    public Function<Long, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<Long, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private static final TypeAide.Visitor<Function<Long, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<Long,Object>, Void, RuntimeException>() {
                @Override
                public Function<Long, Object> visitVarBinary(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            ByteBuffer bb = ByteBuffer.allocate(Long.SIZE>>3);
                            return bb.putLong(v).array();
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitTinyInt(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            if (v <= Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                                throw new IncompatibleException("Long("+v+") to Byte");
                            }
                            return v.byteValue();
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitTimestamp(Void p, Object v)
                        throws RuntimeException {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            return new Date(v);
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitString(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            return v.toString();
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitSmallInt(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            if (v <= Short.MIN_VALUE || v > Short.MAX_VALUE) {
                                throw new IncompatibleException("Long("+v+") to Short");
                            }
                            return v.shortValue();
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitInteger(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            if (v <= Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                                throw new IncompatibleException("Long("+v+") to Integer");
                            }
                            return v.intValue();
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitFloat(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            return v.doubleValue();
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitDecimal(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            return new BigDecimal(v);
                        }
                    };
                }
                @Override
                public Function<Long, Object> visitBigInt(Void p, Object v) {
                    return new Function<Long, Object>() {
                        @Override
                        final public Object apply(Long v) {
                            if (v == null) return null;
                            return v.longValue();
                        }
                    };
                }
            };
}
