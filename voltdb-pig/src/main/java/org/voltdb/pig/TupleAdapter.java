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

package org.voltdb.pig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.voltdb.VoltType;
import org.voltdb.hadoop.FieldAdapter;
import org.voltdb.hadoop.RecordAdapter;
import org.voltdb.hadoop.VoltRecord;
import org.voltdb.hadoop.typeto.BigDecimalTypeTo;
import org.voltdb.hadoop.typeto.ByteArrayTypeTo;
import org.voltdb.hadoop.typeto.ByteTypeTo;
import org.voltdb.hadoop.typeto.DateTypeTo;
import org.voltdb.hadoop.typeto.DoubleTypeTo;
import org.voltdb.hadoop.typeto.IncompatibleException;
import org.voltdb.hadoop.typeto.LongTypeTo;
import org.voltdb.hadoop.typeto.IntegerTypeTo;
import org.voltdb.hadoop.typeto.StringTypeTo;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.collect.ImmutableList;

import static org.apache.pig.data.DataType.*;

/**
 * Adapts a {@link Tuple} into {@link VoltRecord}
 */
public class TupleAdapter extends RecordAdapter<VoltRecord, Tuple, ExecException> {

    final List<TupleFieldAdapter> m_adapters;

    /**
     * Given the pig tuple schema, and the destination VoltDB table column types,
     * it pre-builds the field adapters needed to adapt a {@linkplain Tuple} into
     * a {@linkplain VoltRecord}
     *
     * @param schema pig tuple schema
     * @param types an array of column types
     */
    public TupleAdapter(ResourceSchema schema, VoltType[] types) {
        super(types);

        ResourceFieldSchema [] fields = schema.getFields();
        if (fields.length != m_types.length) {
            throw new IncompatibleException("pig schema field count do not match target table's column count");
        }

        ImmutableList.Builder<TupleFieldAdapter> bldr = ImmutableList.builder();

        for (int i = 0; i < fields.length; ++i) {
            final byte pigType = fields[i].getType();

            if (pigType == LONG) {

                final LongTypeTo to = new LongTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<Long,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        vr.add(m_adjuster.apply((Long)t.get(m_idx)));
                    }
                });

            } else if (pigType == INTEGER) {

                final IntegerTypeTo to = new IntegerTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<Integer,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        vr.add(m_adjuster.apply((Integer)t.get(m_idx)));
                    }
                });

            } else if (pigType == FLOAT) {

                final DoubleTypeTo to = new DoubleTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<Double,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        Float field = (Float)t.get(m_idx);
                        vr.add(m_adjuster.apply(field != null ? field.doubleValue() : null));
                    }
                });

            } else if (pigType == DOUBLE) {

                final DoubleTypeTo to = new DoubleTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<Double,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        vr.add(m_adjuster.apply((Double)t.get(m_idx)));
                    }
                });

            } else if (pigType == BOOLEAN) {

                final ByteTypeTo to = new ByteTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<Byte,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        Boolean field = (Boolean)t.get(m_idx);
                        vr.add(m_adjuster.apply(field != null ? field ? (byte)1 : (byte)0 : null));
                    }
                });

            } else if (pigType == DATETIME) {

                final DateTypeTo to = new DateTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<Date,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        DateTime field = (DateTime)t.get(m_idx);
                        vr.add(m_adjuster.apply(field != null ? field.toDate() : null));
                    }
                });

            } else if (pigType == CHARARRAY) {

                final StringTypeTo to = new StringTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<String,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        vr.add(m_adjuster.apply((String)t.get(m_idx)));
                    }
                });

            } else if (pigType == BYTEARRAY) {

                final ByteArrayTypeTo to = new ByteArrayTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<byte[],Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        DataByteArray field = (DataByteArray)t.get(m_idx);
                        vr.add(m_adjuster.apply(field != null ? field.get() : null));
                    }
                });

            } else if (pigType == BIGDECIMAL) {

                final BigDecimalTypeTo to = new BigDecimalTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<BigDecimal,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        vr.add(m_adjuster.apply((BigDecimal)t.get(m_idx)));
                    }
                });

            } else if (pigType == BIGINTEGER) {

                final BigDecimalTypeTo to = new BigDecimalTypeTo(m_types[i]);
                bldr.add(new TupleFieldAdapter(i) {
                    final Function<BigDecimal,Object> m_adjuster = to.getAdjuster();
                    @Override
                    final public void adapt(Tuple t, VoltRecord vr) throws ExecException {
                        BigInteger field = (BigInteger)t.get(m_idx);
                        vr.add(m_adjuster.apply(field != null ? new BigDecimal(field) : null));
                    }
                });

            } else {

                String incompatible = DataType.findTypeName(pigType);
                throw new IncompatibleException("VoltDB does not support " + incompatible + " type");
            }
        }
        m_adapters = bldr.build();
    }

    /**
     * Use the pre-built field adapters to adapt a {@linkplain Tuple} into a
     * {@linkplain VoltRecord}
     *
     * @param tuple a pig tuple
     * @param record a volt record
     * @return adapted volt record
     */
    @Override
    public VoltRecord adapt(Tuple tuple, VoltRecord record) throws ExecException {
        if (record == null) {
            record = new VoltRecord();
        }
        if (tuple.size() != m_adapters.size()) {
            throw new ExecException(
                    "mismatched tuple size: expected is " + m_adapters.size()
                  + ", actual is " + tuple.size()
                  );
        }
        for (TupleFieldAdapter fieldAdapter: m_adapters) {
            fieldAdapter.adapt(tuple, record);
        }
        return record;
    }

    public static abstract class TupleFieldAdapter implements FieldAdapter<Tuple, VoltRecord, ExecException>{
        final protected int m_idx;
        public TupleFieldAdapter(int idx) {
            m_idx = idx;
        }
    }
}
