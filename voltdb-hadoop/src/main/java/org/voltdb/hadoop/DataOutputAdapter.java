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

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.voltdb.VoltType;

/**
 * Serialized a {@linkplain VoltRecord} into a data stream. A VoltRecord is
 * serialized as follows:
 *
 * <ul>
 * <li>16 byte header signature</li>
 * </ul>
 * <p>and each field consists
 * <ul>
 * <li>1 byte null flag</li>
 * <li>if not null the field value</li>
 * </ul>
 *
 */
public class DataOutputAdapter extends RecordAdapter<VoltRecord,DataOutput, IOException> {

    private OutputFieldAdapter [] m_adapters;

    /**
     * Given a table's column types, it pre-builds the field adapters needed to serializes
     * a {@linkplain VoltRecord} into a data stream
     *
     * @param types an array of column types
     */
    public DataOutputAdapter(VoltType[] types) {
        super(types);
        m_adapters = new OutputFieldAdapter[m_types.length];
        for (int i = 0; i < m_adapters.length; ++i) {
            m_adapters[i] = m_types[i].accept(adapterVtor, i, null);
        }
    }

    /**
     * Use the pre-built field adapters to adapt a {@linkplain VoltRecord} into a
     * {@linkplain DataOutput} stream
     *
     * @param out a data output stream
     * @param record a VoltRecord
     * @return the same {@linkplain VoltRecord} as the one specified in the record parameter
     */
    @Override
    public VoltRecord adapt(DataOutput out, VoltRecord rec)
            throws IOException {
        if (rec == null || rec.size() != m_adapters.length) {
            throw new IOException("unmatched record field count");
        }
        out.writeLong(m_signature.getMostSignificantBits());
        out.writeLong(m_signature.getLeastSignificantBits());
        for (int i = 0; i < m_types.length; ++i) {
            m_adapters[i].adapt(out, rec);
        }
        return rec;
    }

    public static abstract class OutputFieldAdapter
        implements FieldAdapter<DataOutput, VoltRecord, IOException> {
        final int m_idx;
        public OutputFieldAdapter(int idx) {
            m_idx = idx;
        }
        static public boolean writeBoolean(DataOutput out, boolean bool) throws IOException {
            out.writeBoolean(bool);
            return bool;
        }
    }

    final static TypeAide.Visitor<OutputFieldAdapter, Integer, RuntimeException> adapterVtor =
            new TypeAide.Visitor<OutputFieldAdapter, Integer, RuntimeException>() {

        @Override
        public OutputFieldAdapter visitVarBinary(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        Text.writeString(out, StringUtils.byteToHexString((byte[])rec.get(m_idx)));
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitTinyInt(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        out.writeByte((Byte)rec.get(m_idx));
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitTimestamp(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        out.writeLong(((Date)rec.get(m_idx)).getTime());
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitString(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        Text.writeString(out, (String)rec.get(m_idx));
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitSmallInt(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        out.writeShort((Short)rec.get(m_idx));
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitInteger(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        out.writeInt((Integer)rec.get(m_idx));
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitFloat(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        out.writeDouble((Double)rec.get(m_idx));
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitDecimal(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        Text.writeString(out, ((BigDecimal)rec.get(m_idx)).toString());
                    }
                }
            };
        }

        @Override
        public OutputFieldAdapter visitBigInt(Integer p, Object v)
                throws RuntimeException {
            return new OutputFieldAdapter(p) {
                @Override
                public final void adapt(DataOutput out, VoltRecord rec) throws IOException {
                    if (writeBoolean(out, rec.get(m_idx) != null)) {
                        out.writeLong((Long)rec.get(m_idx));
                    }
                }
            };
        }
    };
}
