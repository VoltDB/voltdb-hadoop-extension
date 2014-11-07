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

package org.voltdb.hadoop;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;

/**
 * Adapts a {@linkplain VoltRecord} into a {@linkplain Text} instance. Null
 * numeric fields (including dates) are written as empty fields. Empty binary and
 * string fields are written out as empty fields, while their {@code null} equivalent
 * are written out as &quot;\N&quot;. Binary fields are hex encoded
 */
public class TextOutputAdapter extends RecordAdapter<Text, Text, RuntimeException> {
    final static String SEPARATOR_DFLT = "\t";
    final static String NULL = "\\N";

    final String m_dateFormat;
    final String m_separator;
    final StringBuilderFieldAdapter [] m_adapters;

    /**
     * Given a table's column types, it pre-builds the field adapters needed to construct
     * a {@linkplain Text} instance from a {@linkplain VoltRecord} using the default
     * separator (TAB) and date formatter specification (&quot;yyyy-MM-dd HH:mm:ss.SSS&quot;)
     *
     * @param types an array of column types
     */
    public TextOutputAdapter(VoltType[] types) {
        this(types, SEPARATOR_DFLT, Constants.ODBC_DATE_FORMAT_STRING);
    }

    /**
     * Given a table's column types, it pre-builds the field adapters needed to construct
     * a {@linkplain Text} instance from a {@linkplain VoltRecord} using the given
     * separator, and the default date formatter specification
     * (&quot;yyyy-MM-dd HH:mm:ss.SSS&quot;)
     *
     * @param types an array of column types
     * @param separator field separator
     */
    public TextOutputAdapter(VoltType[] types, String separator) {
        this(types, separator, Constants.ODBC_DATE_FORMAT_STRING);
    }

    /**
     * Given a table's column types, it pre-builds the field adapters needed to construct
     * a {@linkplain Text} instance from a {@linkplain VoltRecord} using the given
     * separator, and date formatter specification
     *
     * @param types an array of column types
     * @param separator field separator
     * @param dateFormat date formatter specification
     */
    public TextOutputAdapter(VoltType [] types, String separator, String dateFormat) {
        super(types);

        if (separator != null && !separator.isEmpty()) {
            m_separator = separator;
        } else {
            m_separator = SEPARATOR_DFLT;
        }
        if (dateFormat != null && !dateFormat.trim().isEmpty()) {
            m_dateFormat = dateFormat;
        } else {
            m_dateFormat = Constants.ODBC_DATE_FORMAT_STRING;
        }

        m_adapters = new StringBuilderFieldAdapter[m_types.length];
        for (int i = 0; i < m_adapters.length; ++i) {
            m_adapters[i] = m_types[i].accept(adapterVtor,i,null);
        }
    }

    /**
     * Use the pre-built field adapters to adapt a {@linkplain Text} instance
     * from a {@linkplain VoltRecord}
     *
     * @param param a {@linkplain Text} instance
     * @param record a VoltRecord
     * @return an adapted {@linkplain Text} instance
     */
    @Override
    public Text adapt(Text to, VoltRecord rec) throws RuntimeException {
        if (rec == null || rec.size() != m_adapters.length) {
            throw new RuntimeException("unmatched record field count");
        }
        if (to == null) {
            to = new Text();
        }
        StringBuilder sb = new StringBuilder(1024);
        for (int i = 0; i < m_adapters.length; ++i) {
            m_adapters[i].adapt(sb, rec);
        }
        to.set(sb.toString());
        return to;
    }

    public static abstract class StringBuilderFieldAdapter
        implements FieldAdapter<StringBuilder, VoltRecord, RuntimeException> {
        final int m_idx;
        public StringBuilderFieldAdapter(int idx) {
            m_idx = idx;
        }
    }

    final TypeAide.Visitor<StringBuilderFieldAdapter, Integer, RuntimeException> adapterVtor =
            new TypeAide.Visitor<StringBuilderFieldAdapter, Integer, RuntimeException>() {
                @Override
                public StringBuilderFieldAdapter visitVarBinary(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null) {
                                sb.append(StringUtils.byteToHexString((byte[])rec.get(m_idx)));
                            } else {
                                sb.append(NULL);
                            }
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitTinyInt(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null) {
                                sb.append(rec.get(m_idx));
                            }
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitTimestamp(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        final SimpleDateFormat m_dfmt = new SimpleDateFormat(m_dateFormat);
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null) {
                                sb.append(m_dfmt.format((Date)rec.get(m_idx)));
                            }
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitString(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            sb.append(rec.get(m_idx) != null ? (String)rec.get(m_idx) : NULL);
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitSmallInt(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null) {
                                sb.append(rec.get(m_idx));
                            }
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitInteger(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null) {
                                sb.append(rec.get(m_idx));
                            }
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitFloat(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null)
                                sb.append(rec.get(m_idx));
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitDecimal(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null) {
                                sb.append(((BigDecimal)rec.get(m_idx)).toString());
                            }
                        }
                    };
                }
                @Override
                public StringBuilderFieldAdapter visitBigInt(Integer p, Object v)
                        throws RuntimeException {
                    return new StringBuilderFieldAdapter(p) {
                        @Override
                        public final void adapt(StringBuilder sb, VoltRecord rec)
                                throws RuntimeException {
                            if (m_idx > 0) sb.append(m_separator);
                            if (rec.get(m_idx) != null) {
                                sb.append(rec.get(m_idx));
                            }
                        }
                    };
                }
            };
}
