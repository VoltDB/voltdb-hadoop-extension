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
import java.io.DataOutput;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.RowWithMetaData;

import com.google_voltpatches.common.collect.FluentIterable;

/**
 * A {@link Writable} wrapper around a {@linkplain List} of object values that
 * represent the field contents of a Volt table row
 */
public class VoltRecord implements Writable, Iterable<Object> {
    private final List<Object> m_fields = new ArrayList<Object>(64);
    private String m_table;

    /*
     * Default constructor for deserializing operations
     */
    public VoltRecord() {
    }

    /**
     * Constructor that sets the table associated with the records fields
     * @param tableName a VoltDB table name
     */
    public VoltRecord(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("null or empty table name");
        }
        m_table = tableName;
    }

    /**
     * Constructors that sets both the field values, and the table name
     * associated with the field's content
     *
     * @param tableName a VoltDB table name
     * @param fields a list of fields values
     */
    public VoltRecord(String tableName, Object...fields) {
        this(tableName);
        FluentIterable.of(fields).copyInto(m_fields);
    }

    /**
     * Constructors that sets both the field values, and the table name
     * associated with the field's content
     *
     * @param tableName a VoltDB table name
     * @param fields a list of fields values
     */
    public VoltRecord(String tableName, Iterable<Object> fields) {
        this(tableName);
        FluentIterable.from(fields).copyInto(m_fields);
    }

    /**
     * Sets the record's table name only if it is not already set
     *
     * @param tableName VoltDB table name
     * @return itself for chained setter invocations
     */
    public VoltRecord setTableName(String tableName) {
        if (tableName != null && !tableName.trim().isEmpty() && m_table == null) {
            m_table = tableName;
        }
        return this;
    }

    /**
     * Gets the object value for the given index into the underlying list
     * @param atIdx index
     * @return the object value at the given index
     */
    public Object get(int atIdx) {
        return m_fields.get(atIdx);
    }

    /**
     * Sets the object at the given index into the underlying list
     *
     * @param atIdx index
     * @param value object value
     * @return itself for chained setter invocations
     */
    public VoltRecord set(int atIdx, Object value) {
        m_fields.set(atIdx, value);
        return this;
    }

    /**
     * Adds a field value to the underlying list
     *
     * @param field value
     * @return itself for chained setter invocations
     */
    public VoltRecord add(Object field) {
        m_fields.add(field);
        return this;
    }

    /**
     * Adds all the given values to the underlying list
     *
     * @param fields list of field values
     * @return itself for chained setter invocations
     */
    public VoltRecord addAll(Object [] fields) {
        FluentIterable.of(fields).copyInto(m_fields);
        return this;
    }

    /**
     * Adds all the given values to the underlying list
     *
     * @param fields list of field values
     * @return itself for chained setter invocations
     */
    public VoltRecord addAll(Iterable<Object> fields) {
        FluentIterable.from(fields).copyInto(m_fields);
        return this;
    }

    /**
     * Adds all the given values to the underlying list
     * (for groovy << operator)
     *
     * @param fields list of field values
     * @return itself for chained setter invocations
     */
    public VoltRecord leftShift(Iterable<Object> fields) {
        return addAll(fields);
    }

    /**
     * Returns the size of the underlying list of field values
     *
     * @return how many values contained wherein
     */
    public int size() {
        return m_fields.size();
    }

    /**
     * Clears  the underlying list
     *
     * @return itself for chained setter invocations
     */
    public VoltRecord clear() {
        m_fields.clear();
        return this;
    }

    /**
     * @return an {@linkplain Iterator} to the underlying list of field values
     */
    @Override
    public Iterator<Object> iterator() {
        return m_fields.iterator();
    }

    /**
     * Indicates whether or not the underlying list of field values is empty
     * @return true if it is empty. false if it is not
     */
    public boolean isEmpty() {
        return m_fields.isEmpty();
    }

    /**
     * Serializes itself to the given {@linkplain DataOutput} stream
     * It looks up the adapters cache for the data adapters associated
     * with this record's table name.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        DataAdapters adapters = DataAdapters.adaptersFor(m_table, null);
        if (adapters == null) {
            throw new IOException("no adapters configured for table " + m_table);
        }
        Text.writeString(out, m_table);
        adapters.forOutput().adapt(out, this);
    }

    /**
     * De-serializes itself from the given {@linkplain DataInput} stream
     * It looks up the adapters cache for the data adapters associated
     * with this record's table name.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        setTableName(Text.readString(in, 512));
        DataAdapters adapters = DataAdapters.adaptersFor(m_table, null);
        if (adapters == null) {
            throw new IOException("no adapters configured for table " + m_table);
        }
        adapters.forInput().adapt(in, this);
    }

    /**
     * Feeds its underlying list of field values to the given loader
     * @param loader a volt loader
     *
     * @throws IOException
     */
    public void write(CSVBulkDataLoader loader) throws IOException {
        RowWithMetaData meta = new RowWithMetaData(new WeakReference<VoltRecord>(this), -1);
        try {
            loader.insertRow(meta, m_fields.toArray(new Object[m_fields.size()]));
        } catch (InterruptedException e) {
            throw new IOException("interrupted loader insert", e);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((m_fields == null) ? 0 : m_fields.hashCode());
        result = prime * result + ((m_table == null) ? 0 : m_table.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VoltRecord other = (VoltRecord) obj;
        if (m_fields == null) {
            if (other.m_fields != null)
                return false;
        } else if (!m_fields.equals(other.m_fields))
            return false;
        if (m_table == null) {
            if (other.m_table != null)
                return false;
        } else if (!m_table.equals(other.m_table))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "VoltRecord [m_fields=" + m_fields + ", m_table=" + m_table
                + "]";
    }

}
