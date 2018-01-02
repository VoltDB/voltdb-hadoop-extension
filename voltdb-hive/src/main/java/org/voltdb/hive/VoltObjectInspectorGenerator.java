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

package org.voltdb.hive;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.voltdb.VoltType;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.BiMap;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableBiMap;
import com.google_voltpatches.common.collect.ImmutableList;

import static com.google_voltpatches.common.base.Predicates.isNull;
import static com.google_voltpatches.common.base.Predicates.not;

/**
 * Makes sure that a destination Voltdb table can accept rows from a Hive
 * table backed by a {@link VoltStorageHandler}. It gets the types that comprise
 * rows from both VoltDB and Hive tables, and see if they match.
 *
 */
public class VoltObjectInspectorGenerator {

    private final static DecimalTypeInfo decimalTypeInfo = new DecimalTypeInfo(26, 12);

    private static final BiMap<VoltType,TypeInfo> initTypeMap() {
        ImmutableBiMap.Builder<VoltType,TypeInfo> map = ImmutableBiMap.builder();
        map.put(VoltType.TINYINT, TypeInfoFactory.getPrimitiveTypeInfo("tinyint"));
        map.put(VoltType.SMALLINT, TypeInfoFactory.getPrimitiveTypeInfo("smallint"));
        map.put(VoltType.INTEGER, TypeInfoFactory.getPrimitiveTypeInfo("int"));
        map.put(VoltType.BIGINT, TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
        map.put(VoltType.FLOAT, TypeInfoFactory.getPrimitiveTypeInfo("double"));
        map.put(VoltType.STRING, TypeInfoFactory.getPrimitiveTypeInfo("string"));
        map.put(VoltType.TIMESTAMP, TypeInfoFactory.getPrimitiveTypeInfo("timestamp"));
        map.put(VoltType.DECIMAL, decimalTypeInfo); // decimal(26,12)
        map.put(VoltType.VARBINARY, TypeInfoFactory.getPrimitiveTypeInfo("binary"));
        return map.build();
    }

    private final static BiMap<VoltType,TypeInfo> voltTypeToTypeInfo = initTypeMap();

    private final List<TypeInfo> m_columnTypes;
    private final List<String> m_columnNames;
    private final List<VoltType> m_voltTypes;
    private final ObjectInspector m_oi;

    /**
     * Checks table row type compatibility between Hive and VoltDB
     *
     * @param names Hive column names
     * @param types Hive column types
     * @param voltTypes VoltDB column types
     * @throws SerDeException
     */
    public VoltObjectInspectorGenerator(List<String> names, List<TypeInfo> types, VoltType [] voltTypes) throws SerDeException {

        if (names == null || names.isEmpty()) {
            throw new VoltSerdeException("passed in null or empty column names");
        }
        if (FluentIterable.from(names).anyMatch(isNull())) {
            throw new VoltSerdeException("found null column name list element");
        }
        if (types == null || types.isEmpty()) {
            throw new VoltSerdeException("passed in null or empty column types");
        }
        if (FluentIterable.from(types).anyMatch(isNull())) {
            throw new VoltSerdeException("found null column type list element");
        }
        if (voltTypes == null || voltTypes.length == 0) {
            throw new VoltSerdeException("passed in null or empty volt types array");
        }
        if (FluentIterable.of(voltTypes).anyMatch(isNull())) {
            throw new VoltSerdeException("found null volt type array element");
        }
        if (names.size() != types.size() || voltTypes.length != types.size()) {
            throw new VoltSerdeException("mismatched columns size betweeen column names/types, and volt types");
        }

        List<TypeInfo> incompatible = FluentIterable.from(types).filter(not(isVoltCompatible)).toList();
        if (!incompatible.isEmpty()) {
            throw new VoltSerdeException(
                    "types " + incompatible + " are not VoltDB compatible\n"
                  + "acceptable types are: " + voltTypeToTypeInfo.values()
                    );
        }

        m_columnTypes = FluentIterable.of(voltTypes).transform(typeMapper).toList();
        if (!m_columnTypes.equals(types)) {
            throw new VoltSerdeException(
                    "Hive columns types: " + types
                  + " do not match volt tables column types: " + m_columnTypes
                    );
        }

        m_voltTypes = ImmutableList.copyOf(voltTypes);
        m_columnNames = ImmutableList.copyOf(names);
        m_oi = createObjectInspector();
    }

    public VoltType [] getVoltTypes() {
        return m_voltTypes.toArray(new VoltType[m_voltTypes.size()]);
    }

    public List<TypeInfo> getColumnTypes() {
        return m_columnTypes;
    }

    public List<String> getColumnNames() {
        return m_columnNames;
    }

    /**
     * @return the {@link ObjectInspector} for the hive table
     */
    public ObjectInspector getObjectInspector() {
        return m_oi;
    }

    private ObjectInspector createObjectInspector() {
        List<ObjectInspector> columnOIs =
                FluentIterable.from(m_columnTypes).transform(primitiveToItsInspector).toList();
        return ObjectInspectorFactory.getStandardStructObjectInspector(m_columnNames, columnOIs);
    }

    private final static Function<VoltType, TypeInfo> typeMapper = new Function<VoltType, TypeInfo>() {
        @Override
        public TypeInfo apply(VoltType voltType) {
            return voltTypeToTypeInfo.get(voltType);
        }
    };

    private final static Predicate<TypeInfo> isVoltCompatible = new Predicate<TypeInfo>() {
        final Map<TypeInfo,VoltType> compatible = voltTypeToTypeInfo.inverse();
        @Override
        public boolean apply(TypeInfo type) {
            return compatible.containsKey(type);
        }
    };

    private final static Function<TypeInfo,ObjectInspector> primitiveToItsInspector =
            new Function<TypeInfo, ObjectInspector>() {
        @Override
        public ObjectInspector apply(TypeInfo ti) {
            PrimitiveTypeInfo pti = (PrimitiveTypeInfo)ti;
            return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
        }
    };

}
