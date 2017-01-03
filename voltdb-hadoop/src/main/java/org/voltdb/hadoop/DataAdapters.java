/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
 * included in all copies or substantial portions of the SCopyright (C) 2008-2017 VoltDB Inc.oftware.
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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicStampedReference;

import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Holder class for {@linkplain VoltRecord} de/serializing adapters
 */
public class DataAdapters {

    /*
     * Adapters cache consists of an immutable map that is replaced every times
     * a new table is inserted into it. Updates are ignored
     */
    static AtomicStampedReference<Map<String,DataAdapters>> m_adapterCache =
            new AtomicStampedReference<Map<String,DataAdapters>>(
                    ImmutableMap.<String,DataAdapters>of(), 0
                    );

    /**
     * Does a cache lookup. If it is a miss it uses the given array of column
     * types to seed the cache for the given table name
     *
     * @param tableName
     * @param columnTypes an array of {@linkplain VoltType} representing the
     *     given table column types
     * @return the adapters for the given table
     */
    public static DataAdapters adaptersFor(String tableName, VoltType [] types) {

        DataAdapters adapters = m_adapterCache.getReference().get(tableName);
        if (adapters == null && types != null && types.length > 0) {
            adapters = new DataAdapters(tableName,types);

            Map<String,DataAdapters> oldmap,newmap;
            int [] stamp = new int[1];
            do try {
                oldmap = m_adapterCache.get(stamp);
                newmap = addEntry(oldmap, tableName, adapters);
            } catch (IllegalArgumentException ingoreDuplicates) {
                break;
            } while (!m_adapterCache.compareAndSet(oldmap, newmap, stamp[0], stamp[0]+1));

        }
        return adapters;
    }

    /*
     * Creates a new immutable map by copying the source's content and adding
     * the new entry after
     */
    private static <K,V> ImmutableMap<K,V> addEntry(Map<K,V> src, K tn, V value) {
        ImmutableMap.Builder<K,V> builder = ImmutableMap.builder();
        builder.putAll(src);
        builder.put(tn,value);
        return builder.build();
    }

    private final String m_tableName;
    private final DataInputAdapter m_input;
    private final DataOutputAdapter m_output;

    /**
     * Builds de/serializing data adapters for the given table and its column types
     * @param tableName
     * @param types table column types as an array of {@linkplain VoltType}s
     */
    public DataAdapters(String tableName, VoltType [] types) {
        m_tableName = tableName;
        m_input = new DataInputAdapter(types);
        m_output = new DataOutputAdapter(types);
    }

    public String getTableName() {
        return m_tableName;
    }

    /**
     * @return the serialization signature header
     */
    public UUID getSignature() {
        return m_input.getSignature();
    }

    /**
     * @return the de-serializing adapter
     */
    public DataInputAdapter forInput() {
        return m_input;
    }

    /**
     * @return the serializing adapter
     */
    public DataOutputAdapter forOutput() {
        return m_output;
    }
}
