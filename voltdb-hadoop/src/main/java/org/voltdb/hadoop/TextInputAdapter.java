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

package org.voltdb.hadoop;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.base.Throwables;

/**
 * Adapts a {@linkplain Text} instance into a {@linkplain VoltRecord}. Empty
 * numeric fields (including dates) are interpreted as nulls. Fields containing
 * &quot;\N&quot; value are interpreted as nulls. Empty hex encoded binary, or
 * empty strings are considered empty. String and binary fields containing only
 * &quot;\N&quot; are interpreted as nulls
 *
 */
public class TextInputAdapter extends RecordAdapter<VoltRecord, Text, RuntimeException> {
    final static String NULL = "\\N";
    final static String SEPARATOR_DFLT = "\t";

    private final String m_dateFormat;
    private final Splitter m_splitter;
    private final StringFieldAdapater [] m_adapters;

    private final static boolean isNull(String val) {
        return val.isEmpty() || NULL.equals(val);
    }

    /**
     * Given a table's column types, it pre-builds the field adapters needed to construct
     * a {@linkplain VoltRecord} from an instance of {@linkplain Text} using the default
     * separator (TAB) and date formatter specification (&quot;yyyy-MM-dd HH:mm:ss.SSS&quot;)
     *
     * @param types an array of column types
     */
    public TextInputAdapter(VoltType[] types) {
        this(types, SEPARATOR_DFLT, Constants.ODBC_DATE_FORMAT_STRING);
    }

    /**
     * Given a table's column types, it pre-builds the field adapters needed to construct
     * a {@linkplain VoltRecord} from an instance of {@linkplain Text} using the given
     * separator, and the default date formatter specification
     * (&quot;yyyy-MM-dd HH:mm:ss.SSS&quot;)
     *
     * @param types an array of column types
     * @param separator field separator
     */
    public TextInputAdapter(VoltType [] types, String separator) {
        this(types, separator, Constants.ODBC_DATE_FORMAT_STRING);
    }

    /**
     * Given a table's column types, it pre-builds the field adapters needed to construct
     * a {@linkplain VoltRecord} from an instance of {@linkplain Text} using the given
     * separator, and date formatter specification
     *
     * @param types an array of column types
     * @param separator field separator
     * @param dateFormat date formatter specification
     */
    public TextInputAdapter(VoltType [] types, String separator, String dateFormat) {
        super(types);
        if (separator == null || separator.isEmpty()) {
            separator = SEPARATOR_DFLT;
        }
        if (dateFormat != null && !dateFormat.trim().isEmpty()) {
            m_dateFormat = dateFormat;
        } else {
            m_dateFormat = Constants.ODBC_DATE_FORMAT_STRING;
        }
        m_splitter = Splitter.on(separator).trimResults();

        m_adapters = new StringFieldAdapater[m_types.length];
        for (int i = 0; i < m_adapters.length; ++i) {
            m_adapters[i] = m_types[i].accept(adapterVtor, i, null);
        }
    }

    /**
     * Use the pre-built field adapters to adapt a {@linkplain Text} instance
     * into a {@linkplain VoltRecord}
     *
     * @param param a {@linkplain Text} instance
     * @param record a VoltRecord
     * @return an adapted {@linkplain VoltRecord}
     */
    @Override
    public VoltRecord adapt(Text param, VoltRecord record)
            throws RuntimeException {

        checkArgument(param != null, "given text is null");
        checkArgument(
                record == null || (record != null && record.size() == 0),
                "given record is already initialized");

        if (record == null) {
            record = new VoltRecord();
        }
        int idx = 0;
        for (String field: m_splitter.split(param.toString())) {
            if (idx < m_adapters.length) {
                m_adapters[idx].adapt(record, field);
            }
            idx += 1;
        }
        if (idx != m_adapters.length) {
            throw new IllegalArgumentException(
                    "mismatched field counts: expected " + m_adapters.length + ", actual " + idx);
        }
        return record;
    }

    static abstract class StringFieldAdapater implements FieldAdapter<VoltRecord, String, RuntimeException> {
        protected final int m_idx;
        public StringFieldAdapater(int idx) {
            m_idx = idx;
        }
    }

    final TypeAide.Visitor<StringFieldAdapater, Integer, RuntimeException> adapterVtor =
            new TypeAide.Visitor<StringFieldAdapater, Integer, RuntimeException>() {
                @Override
                public StringFieldAdapater visitVarBinary(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(NULL.equals(val) ? null : StringUtils.hexStringToByte(val));
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitTinyInt(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(isNull(val) ? null : Byte.parseByte(val));
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitTimestamp(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        final SimpleDateFormat m_dfmt = new SimpleDateFormat(m_dateFormat);
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            try {
                                rec.add(isNull(val) ? null : m_dfmt.parse(val));
                            } catch (ParseException e) {
                                Throwables.propagate(e);
                            }
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitString(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(NULL.equals(val) ? null : val);
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitSmallInt(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(isNull(val) ? null : Short.parseShort(val));
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitInteger(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(isNull(val) ? null : Integer.parseInt(val));
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitFloat(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(isNull(val) ? null : Double.parseDouble(val));
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitDecimal(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(isNull(val) ? null : new BigDecimal(val));
                        }
                    };
                }
                @Override
                public StringFieldAdapater visitBigInt(Integer p, Object v)
                        throws RuntimeException {
                    return new StringFieldAdapater(p) {
                        @Override
                        public void adapt(VoltRecord rec, String val)
                                throws RuntimeException {
                            rec.add(isNull(val) ? null : Long.parseLong(val));
                        }
                    };
                }
            };
}
