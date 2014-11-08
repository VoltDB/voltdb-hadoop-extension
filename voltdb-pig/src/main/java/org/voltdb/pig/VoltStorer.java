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

import org.apache.hadoop.fs.Path;
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
    private String [] m_hosts = new String[]{"locahost"};
    private String m_user = null;
    private String m_password = null;

    private RecordWriter<Text,VoltRecord> m_writer;

    public VoltStorer() {
    }

    /**
     * the location parameter is what is passed to the pig STORE command. It consists
     * of a small JSON document containing the the following attributes
     * <ul>
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
    public VoltStorer(String...locs) {
        if(locs.length >= 1) {
            Location loc = null;
            try {
                loc = Location.fromJSON(locs[0]);
            } catch (JSONException e) {
                throw new IllegalArgumentException(e);
            }
            String [] servers = loc.getServers();
            m_hosts =  servers != null && servers.length > 0 ? servers : m_hosts;
            m_user = loc.getUser();
            m_password = loc.getPassword();
        }
    }

    @Override
    public OutputFormat<Text,VoltRecord> getOutputFormat() throws IOException {
        return new VoltOutputFormat();
    }

    @Override
    public void setStoreLocation(String loc, Job job) throws IOException {

        VoltConfiguration.configureVoltDB(
                job.getConfiguration(), m_hosts, m_user, m_password, loc
                );
        m_conf = new VoltConfiguration(job.getConfiguration());
        m_conf.isMinimallyConfigured();
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        return location;
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
                    json.optString("user", null),
                    json.optString("password", null)
                    );
        }

        public Location(String[] servers, String user, String password) {
            this.servers = servers;
            this.user = user;
            this.password = password;
        }
        public String[] getServers() {
            return servers;
        }
        public String getUser() {
            return user;
        }
        public String getPassword() {
            return password;
        }
    }
}
