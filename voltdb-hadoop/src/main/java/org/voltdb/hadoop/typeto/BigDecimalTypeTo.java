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

import static org.voltdb.hadoop.TypeAide.DECIMAL;
import static org.voltdb.hadoop.TypeAide.STRING;

import java.math.BigDecimal;
import java.util.Date;
import java.util.EnumSet;

import org.voltdb.hadoop.TypeAide;
import org.voltdb.types.VoltDecimalHelper;

import com.google_voltpatches.common.base.Function;

public class BigDecimalTypeTo extends TypeTo<BigDecimal> {
    final static EnumSet<TypeAide> loose = EnumSet.allOf(TypeAide.class);
    final static EnumSet<TypeAide> strict = EnumSet.of(DECIMAL,STRING);

    public BigDecimalTypeTo(TypeAide type, boolean strictCompatibility) {
        super(type, strictCompatibility);
    }

    public BigDecimalTypeTo(TypeAide type) {
        this(type, true);
    }

    @Override
    public Function<BigDecimal, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<BigDecimal, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private final TypeAide.Visitor<Function<BigDecimal, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<BigDecimal,Object>, Void, RuntimeException>() {
                @Override
                public Function<BigDecimal, Object> visitVarBinary(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return VoltDecimalHelper.serializeBigDecimal(v);
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitTinyInt(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.byteValueExact();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitTimestamp(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return new Date(v.longValueExact());
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitString(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.toString();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitSmallInt(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.shortValueExact();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitInteger(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.intValueExact();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitFloat(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.doubleValue();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitDecimal(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v;
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitBigInt(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.longValueExact();
                        }
                    };
                }
            };
}
