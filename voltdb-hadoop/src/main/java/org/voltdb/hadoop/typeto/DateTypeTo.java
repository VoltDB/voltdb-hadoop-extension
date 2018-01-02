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

package org.voltdb.hadoop.typeto;

import static org.voltdb.hadoop.TypeAide.INTEGER;
import static org.voltdb.hadoop.TypeAide.SMALLINT;
import static org.voltdb.hadoop.TypeAide.TINYINT;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;

import org.voltdb.common.Constants;
import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Function;

public class DateTypeTo extends TypeTo<Date> {

    final static EnumSet<TypeAide> loose =
            EnumSet.complementOf(EnumSet.of(TINYINT,SMALLINT,INTEGER));
    final static EnumSet<TypeAide> strict = loose;

    private final String m_dateFormat;

    public DateTypeTo(TypeAide type, boolean strictlyCompatible, String dateFormat) {
        super(type,strictlyCompatible);
        if (dateFormat != null && !dateFormat.trim().isEmpty()) {
            m_dateFormat = dateFormat;
        } else {
            m_dateFormat = Constants.ODBC_DATE_FORMAT_STRING;
        }
    }

    public DateTypeTo(TypeAide type, boolean strictlyCompatible) {
        this(type, strictlyCompatible, Constants.ODBC_DATE_FORMAT_STRING);
    }

    public DateTypeTo(TypeAide type) {
        this(type, true, Constants.ODBC_DATE_FORMAT_STRING);
    }

    @Override
    public Function<Date, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<Date, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private final TypeAide.Visitor<Function<Date, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<Date,Object>, Void, RuntimeException>() {
                @Override
                public Function<Date, Object> visitVarBinary(Void p, Object v) {
                    return new Function<Date, Object>() {
                        @Override
                        final public Object apply(Date v) {
                            if (v == null) return null;
                            ByteBuffer bb = ByteBuffer.allocate(Long.SIZE>>3);
                            return bb.putLong(v.getTime()).array();
                        }
                    };
                }
                @Override
                public Function<Date, Object> visitTinyInt(Void p, Object v) {
                    throw new IncompatibleException("Date is not compatible with Byte");
                }
                @Override
                public Function<Date, Object> visitTimestamp(Void p, Object v) {
                    return new Function<Date, Object>() {
                        @Override
                        final public Object apply(Date v) {
                            return v;
                        }
                    };
                }
                @Override
                public Function<Date, Object> visitString(Void p, Object v) {
                    return new Function<Date, Object>() {
                        final SimpleDateFormat m_dfmt = new SimpleDateFormat(m_dateFormat);
                        @Override
                        final public Object apply(Date v) {
                            if (v == null) return null;
                            return m_dfmt.format(v);
                        }
                    };
                }
                @Override
                public Function<Date, Object> visitSmallInt(Void p, Object v) {
                    throw new IncompatibleException("Date is not compatible with Short");
                }
                @Override
                public Function<Date, Object> visitInteger(Void p, Object v) {
                    throw new IncompatibleException("Date is not compatible with Integer");
                }
                @Override
                public Function<Date, Object> visitFloat(Void p, Object v) {
                    return new Function<Date, Object>() {
                        @Override
                        final public Object apply(Date v) {
                            if (v == null) return null;
                            return v.getTime() / 1000D;
                        }
                    };
                }
                @Override
                public Function<Date, Object> visitDecimal(Void p, Object v) {
                    return new Function<Date, Object>() {
                        @Override
                        final public Object apply(Date v) {
                            if (v == null) return null;
                            return new BigDecimal(v.getTime());
                        }
                    };
                }
                @Override
                public Function<Date, Object> visitBigInt(Void p, Object v) {
                    return new Function<Date, Object>() {
                        @Override
                        final public Object apply(Date v) {
                            if (v == null) return null;
                            return v.getTime();
                        }
                    };
                }
            };
}
