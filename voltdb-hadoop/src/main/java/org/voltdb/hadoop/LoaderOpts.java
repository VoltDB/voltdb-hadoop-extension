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

import static com.google_voltpatches.common.base.Predicates.not;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.voltdb.hadoop.mapred.VoltLoader;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.FluentIterable;

/**
 * {@link VoltLoader} command line options parser
 * <p><pre>
 * org.voltdb.hadoop: usage: VoltLoader [OPTION]... FILE TABLE
 *       -p,--password <password>            user password
 *       -s,--servers <HOST[:PORT][,]...>    List of VoltDB servers to connect to
 *                                           (default: localhost)
 *       -u,--user <username>                database user
 * </pre>
 */
public class LoaderOpts {
    final static Log LOG = LogFactory.getLog("org.voltdb.hadoop");

    @SuppressWarnings("static-access")
    private final static Option serversOpt = OptionBuilder
            .withArgName("HOST[:PORT][,]...").withValueSeparator(',').hasArgs()
            .isRequired(false).withLongOpt("servers")
            .withDescription("List of VoltDB servers to connect to (default: localhost)")
            .create('s');

    @SuppressWarnings("static-access")
    private final static Option userOpt = OptionBuilder
            .withArgName("username").hasArg().isRequired(false)
            .withLongOpt("user").withDescription("database user")
            .create('u');

    @SuppressWarnings("static-access")
    private final static Option passwordOpt = OptionBuilder
            .withArgName("password").hasArg().isRequired(false)
            .withLongOpt("password").withDescription("user password")
            .create('p');

    private final static Options options = new Options();
    final static String usage;

    static {
        options.addOption(serversOpt);
        options.addOption(userOpt);
        options.addOption(passwordOpt);

        usage = getCommandUsage(options, "VoltLoader [OPTION]... FILE TABLE");
    }

    private final static String getCommandUsage(Options opts, String syntax) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        HelpFormatter hfmt = new HelpFormatter();

        hfmt.printHelp(pw, 80, syntax, null, opts, 8, 4, null);
        pw.close();

        return sw.toString();
    }

    final String [] m_servers;
    final String m_user;
    final String m_password;
    final String m_source;
    final String m_table;

    final static Predicate<String> isEmpty = new Predicate<String>() {
        @Override
        public boolean apply(String str) {
            return str.isEmpty();
        }
    };

    final static Function<String, String> trimmer = new Function<String, String>() {
        @Override
        public String apply(String str) {
            return str.trim();
        }
    };

    public LoaderOpts(String [] args) {
        BasicParser parser = new BasicParser();
        CommandLine cli = null;
        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            LOG.error("invalid program options",e);
            LOG.info(usage);
            Throwables.propagate(e);
        }

        String [] remaining = FluentIterable.of(cli.getArgs())
                .transform(trimmer).filter(not(isEmpty)).toArray(String.class);
        if (remaining.length != 2) {
            String msg = "invalid program invocation parameters";
            IllegalArgumentException e = new IllegalArgumentException(msg);
            LOG.error(msg,e);
            LOG.info(usage);
            throw e;
        }

        m_source = remaining[0];
        m_table = remaining[1];

        String [] servers = cli.getOptionValues('s');
        if (servers == null) servers = new String[] {"localhost"};
        m_servers = FluentIterable.of(servers)
                .transform(trimmer).filter(not(isEmpty)).toArray(String.class);

        m_user = cli.getOptionValue('u');
        m_password = cli.getOptionValue('p');
    }

    public String [] getServers() {
        return m_servers;
    }

    public String getUser() {
        return m_user;
    }

    public String getPassword() {
        return m_password;
    }

    public String getSource() {
        return m_source;
    }

    public String getTable() {
        return m_table;
    }

    public JobConf configure(JobConf conf) {
        VoltConfiguration.configureVoltDB(conf, m_servers, m_user, m_password, m_table);
        org.apache.hadoop.mapred.FileInputFormat.addInputPath(conf, new Path(m_source));
        return conf;
    }
}