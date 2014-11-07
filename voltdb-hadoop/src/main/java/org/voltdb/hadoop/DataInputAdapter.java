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

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.voltdb.VoltType;

/**
 * Builds a {@linkplain VoltRecord} from a serialized data stream. A VoltRecord is
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
public class DataInputAdapter extends RecordAdapter<VoltRecord, DataInput, IOException> {
    private final InputFieldAdapter [] m_adapters;

    /**
     * Given a table's column types, it pre-builds the field adapters needed to de-serialize
     * a data stream into a {@linkplain VoltRecord}
     *
     * @param types an array of column types
     */
    public DataInputAdapter(VoltType [] types) {
        super(types);
        m_adapters = new InputFieldAdapter[types.length];
        for (int i=0; i < m_types.length; ++i) {
            m_adapters[i] = m_types[i].accept(fieldAdapterVtor, i, null);
        }
    }

    /**
     * Use the pre-built field adapters to adapt data from a {@linkplain DataInput} stream
     * into a VoltRecord
     *
     * @param in a data input stream
     * @param record a VoltRecord
     * @return an adapted {@linkplain VoltRecord}
     */
    @Override
    public VoltRecord adapt(DataInput in, VoltRecord record) throws IOException {
        UUID mark = new UUID(in.readLong(),in.readLong());
        if (!m_signature.equals(mark)) {
            throw new IOException("unmatched record prefix signature");
        }

        if (record == null) {
            record = new VoltRecord();
        }

        record.clear();

        for (int i = 0; i < m_types.length; ++i) {
            m_adapters[i].adapt(record, in);
        }
        return record;
    }

    public static abstract class InputFieldAdapter
        implements FieldAdapter<VoltRecord, DataInput, IOException> {
        final protected int m_idx;
        public InputFieldAdapter(int idx) {
            m_idx = idx;
        }
    }

    final static TypeAide.Visitor<InputFieldAdapter,Integer,RuntimeException> fieldAdapterVtor =
            new TypeAide.Visitor<InputFieldAdapter, Integer, RuntimeException>() {

        @Override
        public InputFieldAdapter visitVarBinary(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? StringUtils.hexStringToByte(Text.readString(in)) : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitTinyInt(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? in.readByte() : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitTimestamp(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? new Date(in.readLong()) : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitString(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? Text.readString(in) : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitSmallInt(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? in.readShort() : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitInteger(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? in.readInt() : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitFloat(Integer p, Object v) throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? in.readDouble() : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitDecimal(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? new BigDecimal(Text.readString(in)) : null);
                }
            };
        }

        @Override
        public InputFieldAdapter visitBigInt(Integer p, Object v)
                throws RuntimeException {
            return new InputFieldAdapter(p) {
                @Override
                public final void adapt(VoltRecord to, DataInput in) throws IOException {
                    to.add(in.readBoolean() ? in.readLong() : null);
                }
            };
        }
    };
}
