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

package org.voltdb.pig;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.hadoop.VoltConfiguration;
import org.voltdb.hadoop.VoltRecord;
import org.voltdb.hadoop.mapreduce.VoltOutputFormat;
import org.voltdb.hadoop.typeto.IncompatibleException;

/**
 * A Pig {@link StoreFunc} implementation that stores a pig data stream into a VoltDB
 * table
 */
public class VoltStorer extends StoreFunc {

    private VoltConfiguration m_conf;
    private ResourceSchema m_schema;
    private TupleAdapter m_adapter;
    private RecordWriter<Text,VoltRecord> m_writer;

    public VoltStorer() {
    }

    @Override
    public OutputFormat<Text,VoltRecord> getOutputFormat() throws IOException {
        return new VoltOutputFormat();
    }

    /**
     * the location parameter is what is passed to the pig STORE command. It consists
     * of a small JSON document containing the the following attributes
     * <ul>
     * <li>table: destination table name</li>
     * <li>servers: host names where the VoltDB cluster is running</li>
     * <li>user: [optional] database  user name</li>
     * <li>password: [optional] database user password</li>
     * </ul>
     * <p>For example:
     * <pre><code>
     * STORE stream INTO '{"table":"DESTINATION","servers":["host1","host2"]}'
     *     USING org.voltdb.pig.VoltStorer();
     * </code></pre>
     */
    @Override
    public void setStoreLocation(String loc, Job job) throws IOException {
        Location location = null;
        try {
            location = Location.fromJSON(loc);
        } catch (JSONException e) {
            throw new IOException("location specification error",e);
        }
        VoltConfiguration.configureVoltDB(
                job.getConfiguration(),
                location.getServers(),
                location.getUser(),
                location.getPassword(),
                location.getTable()
                );
        m_conf = new VoltConfiguration(job.getConfiguration());
        m_conf.isMinimallyConfigured();
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        try {
            m_adapter = new TupleAdapter(m_schema, m_conf.getTableColumnTypes());
        } catch (IncompatibleException e) {
            throw new IOException("failed to prepare writer",e);
        }
        m_writer = writer;
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        final String tableName = m_conf.getTableName();
        try {
            m_writer.write(new Text(tableName), m_adapter.adapt(t, new VoltRecord(tableName)));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        m_schema = s;
    }

    public static class Location {
        private final String [] servers;
        private final String table;
        private final String user;
        private final String password;

        public static Location fromJSON(String locationString) throws JSONException {
            JSONObject json = new JSONObject(locationString);

            JSONArray jsnServers = json.getJSONArray("servers");
            String [] servers = new String[jsnServers.length()];
            for (int i = 0; i < servers.length; ++i) {
                servers[i] = jsnServers.getString(i);
            }

            return new Location(
                    servers,
                    json.getString("table"),
                    json.optString("user", null),
                    json.optString("password", null)
                    );
        }

        public Location(String[] servers, String table, String user, String password) {
            this.servers = servers;
            this.table = table;
            this.user = user;
            this.password = password;
        }
        public String[] getServers() {
            return servers;
        }
        public String getTable() {
            return table;
        }
        public String getUser() {
            return user;
        }
        public String getPassword() {
            return password;
        }
    }
}
