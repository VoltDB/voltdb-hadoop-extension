/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.voltdb.VoltType;
import org.voltdb.hadoop.VoltConfiguration;
import org.voltdb.hadoop.VoltRecord;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.FluentIterable;

/**
 * A {@link SerDe} implementation that de/serializes {@link VoltRecord}s. For
 * serialization the following Hive column types are supported
 * <p><ul>
 * <li>{@literal tinyint}</li>
 * <li>{@literal smallint}</li>
 * <li>{@literal int}</li>
 * <li>{@literal bigint}</li>
 * <li>{@literal timestamp}</li>
 * <li>{@literal double}</li>
 * <li>{@literal decimal(26,12)}</li>
 * <li>{@literal string}</li>
 * <li>{@literal binary}</li>
 * </ul><p>
 * The following are this class's configuration properties that may be specified
 * with the {@literal SERDEPROPERTIES(...)} create table clause<p>
 * <ul>
 * <li>{@code voltdb.servers} (required) comma separated list of VoltDB servers
 * that comprise a VoltDB cluster</li>
 * <li>{@code voltdb.table} (required) destination VoltDB table</li>
 * <li>{@code voltdb.user} (optional) VoltDB user name</li>
 * <li>{@code voltdb.password} (optional) VoltDB user password</li>
 * </ul>
 * <p>
 */
public class VoltSerDe extends AbstractSerDe {

    public final static String USER_PROP = "voltdb.user";
    public final static String PASSWORD_PROP = "voltdb.password";
    public final static String SERVERS_PROP = "voltdb.servers";
    public final static String TABLE_PROP = "voltdb.table";

    private final Splitter m_splitter = Splitter.on(",").trimResults().omitEmptyStrings();
    private VoltObjectInspectorGenerator m_oig;
    private VoltConfiguration m_voltConf;

    @Override
    public Object deserialize(Writable w) throws SerDeException {
        if (!(w instanceof VoltRecord)) {
            throw new VoltSerdeException("expected an instance of VoltRecord");
        }
        VoltRecord vr = (VoltRecord)w;
        return FluentIterable.from(vr).toList();
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return m_oig.getObjectInspector();
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    /**
     * Reads the following SERDEPROPERTIES
     * <p>
     * <ul>
     * <li>{@code voltdb.servers} (required) comma separated list of VoltDB servers
     * that comprise a VoltDB cluster</li>
     * <li>{@code voltdb.table} (required) destination VoltDB table</li>
     * <li>{@code voltdb.user} (optional) VoltDB user name</li>
     * <li>{@code voltdb.password} (optional) VoltDB user password</li>
     * </ul>
     * <p>
     * and makes sure that the Hive table column types match the destination
     * VoltDB column types
     */
    @Override
    public void initialize(Configuration conf, Properties props) throws SerDeException {

        String columnNamesPropVal = props.getProperty(serdeConstants.LIST_COLUMNS,"");
        String columnTypesPropVal = props.getProperty(serdeConstants.LIST_COLUMN_TYPES,"");
        String serversPropVal = props.getProperty(SERVERS_PROP,"");

        String table = props.getProperty(TABLE_PROP,"");
        String user = props.getProperty(USER_PROP);
        String password = props.getProperty(PASSWORD_PROP);

        if (serversPropVal.trim().isEmpty() || table.trim().isEmpty()) {
            throw new VoltSerdeException(
                    "properties \"" + SERVERS_PROP
                  + "\", and \"" + TABLE_PROP + "\" must be minimally defined"
                  );
        }

        List<String> columnNames = m_splitter.splitToList(columnNamesPropVal);
        List<TypeInfo> columnTypes =
                TypeInfoUtils.getTypeInfosFromTypeString(columnTypesPropVal);

        String [] servers = m_splitter.splitToList(serversPropVal).toArray(new String[0]);
        if (servers.length == 0) {
            throw new VoltSerdeException(
                    "properties \"" + SERVERS_PROP
                  + "\", and \"" + TABLE_PROP + "\" must be minimally defined"
                  );
        }

        if (conf != null) {
            VoltConfiguration.configureVoltDB(conf, servers, user, password, table);
        }

        VoltType [] voltTypes = null;
        m_voltConf = new VoltConfiguration(table, servers, user, password);
        try {
            m_voltConf.isMinimallyConfigured();
            voltTypes = m_voltConf.getTableColumnTypes();
        } catch (IOException e) {
            throw new VoltSerdeException("uanble to setup a VoltDB context", e);
        }
        m_oig = new VoltObjectInspectorGenerator(columnNames, columnTypes, voltTypes);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return VoltRecord.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
        if (oi.getCategory() != Category.STRUCT) {
            throw new VoltSerdeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + oi.getTypeName());
        }
        VoltRecord vr = new VoltRecord(m_voltConf.getTableName());
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> structFields = soi.getAllStructFieldRefs();
        List<Object> fieldValues = soi.getStructFieldsDataAsList(obj);

        final int size = m_oig.getColumnTypes().size();

        for (int i = 0; i < size; ++i) {
            ObjectInspector fieldOI = structFields.get(i).getFieldObjectInspector();
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector)fieldOI;

            Object fieldValue = poi.getPrimitiveJavaObject(fieldValues.get(i));
            if (poi.getTypeInfo().equals(TypeInfoFactory.timestampTypeInfo)) {
                fieldValue = fieldValue != null ? new Date(((Timestamp)fieldValue).getTime()) : null;
            }
            vr.add(fieldValue);
        }

        return vr;
    }
}
