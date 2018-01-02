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

package org.voltdb.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.voltdb.hadoop.FaultCollector;
import org.voltdb.hadoop.TextOutputAdapter;
import org.voltdb.hadoop.VoltConfiguration;
import org.voltdb.hadoop.VoltRecord;
import org.voltdb.utils.CSVBulkDataLoader;

public class VoltOutputFormat extends OutputFormat<Text, VoltRecord> {

    @Override
    public RecordWriter<Text, VoltRecord> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new VoltRecordWriter(context.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
        VoltConfiguration conf = new VoltConfiguration(context.getConfiguration());
        conf.isMinimallyConfigured();
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new VoltOutputComitter();
    }

    public static class VoltRecordWriter extends RecordWriter<Text, VoltRecord> {
        protected final FaultCollector m_faultCollector;
        protected final CSVBulkDataLoader m_loader;

        public VoltRecordWriter(Configuration jobConf) throws IOException {
            VoltConfiguration conf = new VoltConfiguration(jobConf);
            TextOutputAdapter adapter = new TextOutputAdapter(conf.getTableColumnTypes(),"|");

            m_faultCollector = new FaultCollector(adapter, conf.getConfig().getMaxBulkLoaderErrors());
            m_loader = conf.getBulkLoader(m_faultCollector);
        }

        @Override
        public void write(Text key, VoltRecord record) throws IOException,
                InterruptedException {
            record.write(m_loader);
            m_faultCollector.check(false);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            try {
                m_faultCollector.check(true);
            } finally {
                try {m_loader.close();} catch (Exception ignoreIt) {}
            }
        }
    }

    public static class VoltOutputComitter extends OutputCommitter {
        @Override
        public void setupJob(JobContext jobContext) throws IOException {
        }
        @Override
        public void setupTask(TaskAttemptContext taskContext)
                throws IOException {
        }
        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext)
                throws IOException {
            return false;
        }
        @Override
        public void commitTask(TaskAttemptContext taskContext)
                throws IOException {
        }
        @Override
        public void abortTask(TaskAttemptContext taskContext)
                throws IOException {
        }
    }
}
