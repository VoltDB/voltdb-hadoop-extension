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

package org.voltdb.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.voltdb.hadoop.VoltRecord;

/**
 * An {@link InputFormat} that performs no reads whatsoever
 */
public class VoidInputFormat
    implements InputFormat<Text, VoltRecord>, JobConfigurable
{
    static final Log LOG = LogFactory.getLog(VoidInputFormat.class.getName());

    public static class DummyInputSplit implements InputSplit {
        public DummyInputSplit() {
        }

        @Override
        public long getLength() throws IOException {
            return 1;
        }

        @Override
        public String[] getLocations() throws IOException {
            return new String[0];
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
        }
    }

    public static class VoidRowsRecordReader implements RecordReader<Text, VoltRecord> {

        public VoidRowsRecordReader() {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Text createKey() {
            return new Text("DUMMY");
        }

        @Override
        public VoltRecord createValue() {
            return new VoltRecord("DUMMY");
        }

        @Override
        public long getPos() throws IOException {
            return 1L;
        }

        @Override
        public float getProgress() throws IOException {
            return 1f;
        }

        @Override
        public boolean next(Text key, VoltRecord vr) throws IOException {
            return false;
        }
    }

    @Override
    public RecordReader<Text, VoltRecord> getRecordReader(InputSplit arg0,
        JobConf arg1, Reporter arg2) throws IOException {
        return new VoidRowsRecordReader();
    }

    @Override
    public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
        InputSplit[] ret = new InputSplit[1];
        ret[0] = new DummyInputSplit();
        LOG.info("Calculating splits");
        return ret;
    }

    @Override
    public void configure(JobConf job) {
        LOG.info("Using void rows input format");
    }
}