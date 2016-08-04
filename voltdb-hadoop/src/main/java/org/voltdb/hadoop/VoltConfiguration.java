/*
 * The MIT License (MIT)
 *
 * This file is part of VoltDB.
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

package org.voltdb.hadoop;

import static com.google_voltpatches.common.base.Predicates.not;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicStampedReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVBulkDataLoader;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableMap;

/*
 * Helper class that reads/sets job configuration parameters and builds/caches
 * useful artifacts for Volt's data adapters and loaders
 */
public class VoltConfiguration {
    public static final String TMPJARS_PROP = "tmpjars";

    final static Log LOG = LogFactory.getLog("org.voltdb.hadoop");

    /** VoltDB cluster host names */
    public static final String HOSTNAMES_PROP = "mapred.voltdb.hostnames";
    /** VoltDB user name */
    public static final String USERNAME_PROP = "mapred.voltdb.username";
    /** VoltDB user password */
    public static final String PASSWORD_PROP = "mapred.voltdb.password";
    /** VoltDB loader batch size */
    public static final String BATCHSIZE_PROP = "mapred.voltdb.batchsize";
    /** VoltDB loader batch size default */
    public static final int    BATCHSIZE_DFLT = 300;

    /** How many seconds pass before the first loader flush occurs */
    public static final String FLUSHDELAY_PROP = "mapred.voltdb.flush.delay";
    /** FLush delay default */
    public static final int    FLUSHDELAY_DFLT = 10;
    /** How many seconds pass between flushes */
    public static final String FLUSHSECONDS_PROP = "mapred.voltdb.flush.seconds";
    /** Flush interval default*/
    public static final int    FLUSHSECONDS_DFLT = FLUSHDELAY_DFLT;
    /** Load destination table name */
    public static final String TABLENAME_PROP = "mapred.voltdb.table.name";

    /** VoltDB client timeout */
    public static final String CLIENT_TIMEOUT_PROP = "mapred.voltdb.client.timeout";
    /** Time out default */
    public static final long TIMEOUT_DFLT =  2 * 60 * 1000;

    /**max number of errors allowed  */
    public static final String BULKLOADER_MAX_ERRORS_PROP="mapred.voltdb.bulkerloader.max.errors";
    /**
     * Property for speculative execution of MAP tasks
     */
    public static final String MAP_SPECULATIVE_EXEC = "mapreduce.map.speculative";

    /**
     * Property for speculative execution of REDUCE tasks
     */
    public static final String REDUCE_SPECULATIVE_EXEC = "mapreduce.reduce.speculative";

    private final Config  m_config;

    /**
     * Sets the job configuration properties that correspond to the given parameters
     *
     * @param conf a {@linkplain Configuration}
     * @param hostNames an array of host names
     * @param userName
     * @param password
     * @param tableName destination table name
     */
    public static void configureVoltDB(Configuration conf, String [] hostNames,
            String userName, String password, String tableName) {
        conf.setBoolean(MAP_SPECULATIVE_EXEC, false);
        conf.setBoolean(REDUCE_SPECULATIVE_EXEC, false);

        conf.setStrings(HOSTNAMES_PROP, hostNames);
        if (!isNullOrEmpty.apply(userName)) {
            conf.set(USERNAME_PROP, userName);
        }
        if (!isNullOrEmpty.apply(password)) {
            conf.set(PASSWORD_PROP, password);
        }
        conf.set(TABLENAME_PROP, tableName);
    }

    public static void loadVoltClientJar(Configuration conf) {
        String voltJar = ClientImpl.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toString();

        if (voltJar.toLowerCase().endsWith(".jar")) {
            String [] jars = conf.getStrings(TMPJARS_PROP, new String[0]);
            jars = Arrays.copyOf(jars, jars.length+1);
            jars[jars.length-1] = voltJar;
            conf.setStrings(TMPJARS_PROP, jars);
        }
    }

    /**
     * Sets the job configuration properties that correspond to the given parameters
     *
     * @param conf a {@linkplain Configuration}
     * @param hostNames an array of host names
     * @param userName
     * @param password
     * @param tableName destination table name
     * @param batchSize
     * @param flushDelay
     * @param flushSeconds
     */
    public static void configureVoltDB(Configuration conf, String [] hostNames,
            String userName, String password, String tableName,
            int batchSize, int flushDelay, int flushSeconds) {

        configureVoltDB(conf, hostNames, userName, password, tableName);

        if (flushDelay > 0)   conf.setInt(FLUSHDELAY_PROP, flushDelay);
        if (flushSeconds > 0) conf.setInt(FLUSHSECONDS_PROP, flushSeconds);
        if (batchSize > 0)    conf.setInt(BATCHSIZE_PROP, batchSize);
    }

    /**
     * Sets the job configuration properties that correspond to the given parameters
     *
     * @param conf a {@linkplain Configuration}
     * @param hostNames an array of host names
     * @param userName
     * @param password
     * @param tableName destination table name
     * @param batchSize
     * @param clientTimeOut
     */
    public static void configureVoltDB(Configuration conf, String [] hostNames,
            String userName, String password, String tableName,
            int batchSize, long clientTimeOut, int maxErrors) {

        configureVoltDB(conf, hostNames, userName, password, tableName);

        if (clientTimeOut > 0)   conf.setLong(CLIENT_TIMEOUT_PROP, clientTimeOut);
        if (batchSize > 0)    conf.setInt(BATCHSIZE_PROP, batchSize);
        if (maxErrors > 0)    conf.setInt(BULKLOADER_MAX_ERRORS_PROP, maxErrors);
    }

    /*
     * Table column types cache consists of an immutable map that is replaced every times
     * a new table is inserted into it. Updates are ignored
     */
    private static AtomicStampedReference<Map<String, VoltType[]>> m_typeCache =
            new AtomicStampedReference<Map<String, VoltType[]>>(ImmutableMap.<String, VoltType[]>of(),0);

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

    /*
     * Does a cache lookup. If it is a miss it uses the remaining parameters
     * to connect to voltdb and query the given table column types
     */
    private static VoltType[] typesFor(Config config)
            throws IOException
    {
        VoltType [] types = m_typeCache.getReference().get(config.getTableName());
        if (types == null) {
            Client volt = getVoltDBClient(config);
            try {
                types = getTableColumnTypes(volt, config.getTableName());
                if (types.length == 0) {
                    throw new IOException("Table " + config.getTableName() + " does not exist");
                }
            } finally {
                try { volt.close();} catch (InterruptedException ignoreIt) {};
            }
            Map<String,VoltType[]> oldmap,newmap;
            int [] stamp = new int[1];
            do try {
                oldmap = m_typeCache.get(stamp);
                newmap = addEntry(oldmap, config.getTableName(), types);
            } catch (IllegalArgumentException ignoreDuplicates) {
                break;
            } while (!m_typeCache.compareAndSet(oldmap, newmap, stamp[0], stamp[0]+1));
        }
        return types;
    }

    /**
     * Does a cache lookup. If it is a miss it uses the given array of column
     * types to seed the cache for the given table name
     *
     * @param tableName
     * @param columnTypes
     * @return the tables column types
     */
    static VoltType[] typesFor(String tableName, VoltType[] columnTypes) {
        VoltType [] types = m_typeCache.getReference().get(tableName);
        if (types == null && columnTypes != null && columnTypes.length > 0) {
            Map<String,VoltType[]> oldmap,newmap;
            int [] stamp = new int[1];
            do try {
                oldmap = m_typeCache.get(stamp);
                newmap = addEntry(oldmap, tableName, columnTypes);
            } catch (IllegalArgumentException ignoreDuplicates) {
                break;
            } while (!m_typeCache.compareAndSet(oldmap, newmap, stamp[0], stamp[0]+1));
        }
        return types;
    }

    /**
     * Reads volt specific configuration parameters from the
     * given {@linkplain JobConf} job configuration
     *
     * @param conf job configuration
     */
    public VoltConfiguration(Configuration conf) {
        m_config = new Config(conf.get(TABLENAME_PROP),
                conf.getStrings(HOSTNAMES_PROP, new String[]{}),
                conf.get(USERNAME_PROP),
                conf.get(PASSWORD_PROP),
                conf.getInt(BATCHSIZE_PROP, BATCHSIZE_DFLT),
                conf.getLong(CLIENT_TIMEOUT_PROP, TIMEOUT_DFLT),
                conf.getInt(BULKLOADER_MAX_ERRORS_PROP, FaultCollector.MAXFAULTS));
    }

    /**
     * Constructs a configuration instance from the given parameters
     *
     * @param tableName
     * @param hosts
     * @param userName
     * @param password
     */
    public VoltConfiguration(String tableName, String [] hosts, String userName, String password) {
        Preconditions.checkArgument(
                tableName != null && !tableName.trim().isEmpty(),
                "null or empty table name");
        Preconditions.checkArgument(
                hosts != null && hosts.length > 0, "null or empty hosts");

        m_config = new Config(tableName, hosts, userName, password, BATCHSIZE_DFLT, TIMEOUT_DFLT, FaultCollector.MAXFAULTS);
    }

    public VoltConfiguration(Config config) {
        Preconditions.checkArgument(
                config.getTableName() != null && !config.getTableName() .trim().isEmpty(),
                "null or empty table name");
        Preconditions.checkArgument(
                config.getHosts() != null && config.getHosts().length > 0, "null or empty hosts");

        m_config = config;
    }

    /**
     * Is it configured to hold the minimum required configuration
     * properties
     * @return true if is minimally configured
     * @throws IOException if it is not, or it cannot access the VoltDB cluster
     */
    public boolean isMinimallyConfigured() throws IOException {
        if (isNullOrEmpty.apply(m_config.getTableName()) || m_config.getHosts().length == 0){
            throw new IOException(String.format("Properties %s and %s must be defined.", TABLENAME_PROP, HOSTNAMES_PROP));
        }

        VoltType[] columnTypes = getTableColumnTypes();
        if(columnTypes == null || columnTypes.length == 0){
            throw new IOException("Column types do not match");
        }
        return true;
    }

    /** used to check configuration parameters */
    final static Predicate<String> isNullOrEmpty = new Predicate<String>() {
        @Override
        public boolean apply(String str) {
            return str == null || str.trim().isEmpty();
        }
    };

    /*
     * It creates a VoltDB client from the given parameters.
     */
    private static ClientImpl getVoltDBClient(Config config) throws IOException {

        ClientConfig cf = new ClientConfig(config.getUserName(),config.getPassword());
        cf.setConnectionResponseTimeout(config.getClientTimeout());
        cf.setReconnectOnConnectionLoss(true);

        if (config.getHosts().length == 0 || FluentIterable.of(config.getHosts()).allMatch(isNullOrEmpty)) {
            throw new IOException("Hosts are improperly specified");
        }
        ClientImpl client = (ClientImpl)ClientFactory.createClient(cf);

        int failCount = 0, attemptCount = 0;
        for (String hostName: FluentIterable.of(config.getHosts()).filter(not(isNullOrEmpty))) try {
            attemptCount += 1;
            client.createConnection(hostName);
        } catch (IOException e) {
            failCount += 1;
            LOG.error("Failed to connect to host " + hostName, e);
        }

        if (failCount == attemptCount) {
            throw new IOException("Failed to connect to hosts " + Arrays.toString(config.getHosts()));
        }

        return client;
    }

    /*
     * Calls to the @SystemInformation system procedure to determine the given table
     * column types
     */
    private static VoltType[] getTableColumnTypes(Client volt, String tableName) throws IOException {
        ClientResponse cr = null;
        try {
            cr = volt.callProcedure("@SystemCatalog", "COLUMNS");
        } catch (ProcCallException e) {
            throw new IOException("call to @SystemCatalog", e);
        }
        Map<Long, VoltType> columns = new TreeMap<Long, VoltType>();
        VoltTable res = cr.getResults()[0];
        while (res.advanceRow()) {
            if (res.getString("TABLE_NAME").equalsIgnoreCase(tableName)) {
                columns.put(res.getLong("ORDINAL_POSITION"), VoltType.typeFromString(res.getString("TYPE_NAME")));
            }
        }
        return columns.values().toArray(new VoltType[0]);
    }

    private ClientImpl getVoltDBClient() throws IOException {
        return getVoltDBClient(m_config);
    }

    /**
     * Returns the column types for the configures destination table name. It also primes
     * the table column type, and table adapters caches
     *
     * @return an array of volt types representing the tables configured table column types
     * @throws IOException when it fails to communicate with the VoltDB cluster
     */
    public VoltType[] getTableColumnTypes() throws IOException {
        VoltType [] types = typesFor(m_config);
        DataAdapters.adaptersFor(m_config.getTableName(), types);
        return types;
    }

    /**
     * Returns a VoltDB bulk loader
     * @param errorHandler an asynchronous loader error handler
     * @return a VoltDB bulk loader
     * @throws IOException
     */
    public CSVBulkDataLoader getBulkLoader(BulkLoaderErrorHandler errorHandler) throws IOException {
        if (isNullOrEmpty.apply(m_config.getTableName())) {
            throw new IOException("Property " + TABLENAME_PROP + " is not specified");
        }
        CSVBulkDataLoader loader = null;
        try {
            loader = new CSVBulkDataLoader(
                    getVoltDBClient(), m_config.getTableName(), m_config.getBatchSize(), errorHandler);
        } catch (Exception e) {
            throw new IOException("Unable to instantiate a VoltDB bulk loader", e);
        }
        return loader;
    }

    public Config getConfig() {
        return m_config;
    }

    /**
     * A configuration property container
     *
     */
    public static class Config {
        private final String m_tableName;
        private final String [] m_hosts;
        private final String m_userName;
        private final String m_password;
        private final int m_batchSize;
        private final long m_clientTimeout;
        private final int m_maxBulkLoaderErrors;

        public Config(String tableName, String[] hosts, String userName, String password, int batchSize, long clientTimeout, int bulkLoaderMaxErrors){
            m_tableName = tableName;
            m_hosts = hosts;
            m_userName = userName;
            m_password = password;
            m_batchSize = batchSize;
            m_clientTimeout = clientTimeout;
            m_maxBulkLoaderErrors = bulkLoaderMaxErrors;
        }

        public String getUserName() {
            return m_userName;
        }

        private String getPassword() {
            return m_password;
        }

        private String [] getHosts() {
            return m_hosts;
        }

        public String getTableName() {
            return m_tableName;
        }

        public int getBatchSize(){
            return m_batchSize;
        }

        public long getClientTimeout() {
            return m_clientTimeout;
        }

        public int getMaxBulkLoaderErrors() {
            return m_maxBulkLoaderErrors;
        }

        @Override
        public String toString() {
            return String.format("Table: %s, User: %s, Password: %s, Servers: %s, Batch Size: %d, Client Timeout: %d, Max errors: %d",
                    m_tableName, m_userName, m_password, Arrays.toString(m_hosts), m_batchSize, m_clientTimeout, m_maxBulkLoaderErrors);
        }
    }
}
