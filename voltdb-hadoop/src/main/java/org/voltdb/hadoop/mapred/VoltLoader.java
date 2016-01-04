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

package org.voltdb.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.voltdb.hadoop.LoaderOpts;
import org.voltdb.hadoop.TextInputAdapter;
import org.voltdb.hadoop.VoltConfiguration;
import org.voltdb.hadoop.VoltRecord;

import com.google_voltpatches.common.base.Throwables;

public class VoltLoader {

    public static class LoadMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, VoltRecord> {

        private VoltConfiguration m_conf;
        private TextInputAdapter m_adapter;
        private String m_table;

        @Override
        public void configure(JobConf job) {
            m_conf = new VoltConfiguration(job);
            m_table = m_conf.getTableName();
            try {
                m_adapter = new TextInputAdapter(m_conf.getTableColumnTypes());
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<Text, VoltRecord> output, Reporter reporter)
                throws IOException {
            VoltRecord rec = m_adapter.adapt(value, null).setTableName(m_table);
            output.collect(new Text(m_table), rec);
        }

    }

    public static void main(String [] args) {

        JobClient client = new JobClient();
        JobConf conf = new JobConf(VoltLoader.class);

        VoltConfiguration.loadVoltClientJar(conf);
        new LoaderOpts(args).configure(conf);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(VoltRecord.class);
        conf.setOutputFormat(VoltOutputFormat.class);

        conf.setMapperClass(LoadMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setCombinerClass(IdentityReducer.class);

        client.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
