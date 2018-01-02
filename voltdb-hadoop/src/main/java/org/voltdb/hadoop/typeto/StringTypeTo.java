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

import static org.voltdb.hadoop.TypeAide.STRING;
import static org.voltdb.hadoop.TypeAide.VARBINARY;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;

import org.voltdb.common.Constants;
import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Function;

public class StringTypeTo extends TypeTo<String> {
    final static EnumSet<TypeAide> loose = EnumSet.allOf(TypeAide.class);
    final static EnumSet<TypeAide> strict = EnumSet.of(STRING,VARBINARY);

    public static final String NULL = "\\N";

    private final String m_dateFormat;

    private final static boolean isNull(String val) {
        return val == null || val.trim().isEmpty() || NULL.equals(val.trim());
    }

    public StringTypeTo(TypeAide type, boolean strictlyCompatible, String dateFormat) {
        super(type, strictlyCompatible);
        if (dateFormat != null && !dateFormat.trim().isEmpty()) {
            m_dateFormat = dateFormat;
        } else {
            m_dateFormat = Constants.ODBC_DATE_FORMAT_STRING;
        }
    }

    public StringTypeTo(TypeAide type, boolean strictlyCompatible) {
        this(type, strictlyCompatible, Constants.ODBC_DATE_FORMAT_STRING);
    }

    public StringTypeTo(TypeAide type) {
        this(type, true, Constants.ODBC_DATE_FORMAT_STRING);
    }

    @Override
    public Function<String, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<String, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private final TypeAide.Visitor<Function<String, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<String,Object>, Void, RuntimeException>() {
                @Override
                public Function<String, Object> visitVarBinary(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (v == null || NULL.equals(v)) return null;
                            return v.getBytes(Charsets.UTF_8);
                        }
                    };
                }
                @Override
                public Function<String, Object> visitTinyInt(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (isNull(v)) return null;
                            return Byte.parseByte(v);
                        }
                    };
                }
                @Override
                public Function<String, Object> visitTimestamp(Void p, Object v)
                        throws RuntimeException {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            SimpleDateFormat m_dfmt = new SimpleDateFormat(m_dateFormat);
                            if (isNull(v)) return null;
                            Date ts = null;
                            try {
                                ts = m_dfmt.parse(v);
                            } catch(ParseException e) {
                                throw new IncompatibleException("String("+v+") to Date",e);
                            }
                            return ts;
                        }
                    };
                }
                @Override
                public Function<String, Object> visitString(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (v == null || NULL.equals(v)) return null;
                            return v;
                        }
                    };
                }
                @Override
                public Function<String, Object> visitSmallInt(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (isNull(v)) return null;
                            return Short.parseShort(v);
                        }
                    };
                }
                @Override
                public Function<String, Object> visitInteger(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (isNull(v)) return null;
                            return Integer.parseInt(v);
                        }
                    };
                }
                @Override
                public Function<String, Object> visitFloat(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (isNull(v)) return null;
                            return Double.parseDouble(v);
                        }
                    };
                }
                @Override
                public Function<String, Object> visitDecimal(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (isNull(v)) return null;
                            return new BigDecimal(v.trim());
                        }
                    };
                }
                @Override
                public Function<String, Object> visitBigInt(Void p, Object v) {
                    return new Function<String, Object>() {
                        @Override
                        final public Object apply(String v) {
                            if (isNull(v)) return null;
                            return Long.parseLong(v);
                        }
                    };
                }
            };
}
