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

import static org.voltdb.hadoop.TypeAide.TIMESTAMP;

import java.math.BigDecimal;
import java.util.EnumSet;

import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Function;

public class ByteTypeTo extends TypeTo<Byte> {
    final static EnumSet<TypeAide> loose = EnumSet.complementOf(EnumSet.of(TIMESTAMP));
    final static EnumSet<TypeAide> strict = loose;

    public ByteTypeTo(TypeAide type, boolean strictCompatibility) {
        super(type,strictCompatibility);
    }

    public ByteTypeTo(TypeAide type) {
        this(type,true);
    }

    @Override
    public Function<Byte, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<Byte, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private static final TypeAide.Visitor<Function<Byte, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<Byte,Object>, Void, RuntimeException>() {
                @Override
                public Function<Byte, Object> visitVarBinary(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return new byte[] {v.byteValue()};
                        }
                    };
                }
                @Override
                public Function<Byte, Object> visitTinyInt(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return v.byteValue();
                        }
                    };
                }
                @Override
                public Function<Byte, Object> visitTimestamp(Void p, Object v) {
                    throw new IncompatibleException("Byte is not compatible with Date");
                }
                @Override
                public Function<Byte, Object> visitString(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return v.toString();
                        }
                    };
                }
                @Override
                public Function<Byte, Object> visitSmallInt(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return v.shortValue();
                        }
                    };
                }
                @Override
                public Function<Byte, Object> visitInteger(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return v.intValue();
                        }
                    };
                }
                @Override
                public Function<Byte, Object> visitFloat(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return v.doubleValue();
                        }
                    };
                }
                @Override
                public Function<Byte, Object> visitDecimal(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return new BigDecimal(v.intValue());
                        }
                    };
                }
                @Override
                public Function<Byte, Object> visitBigInt(Void p, Object v) {
                    return new Function<Byte, Object>() {
                        @Override
                        final public Object apply(Byte v) {
                            if (v == null) return null;
                            return v.longValue();
                        }
                    };
                }
            };

}
