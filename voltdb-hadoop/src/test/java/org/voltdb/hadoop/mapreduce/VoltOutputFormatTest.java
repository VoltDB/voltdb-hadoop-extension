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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.voltdb.VoltType.BIGINT;
import static org.voltdb.VoltType.FLOAT;
import static org.voltdb.VoltType.INTEGER;
import static org.voltdb.VoltType.STRING;
import static org.voltdb.VoltType.TIMESTAMP;
import static org.voltdb.VoltType.VARBINARY;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.hadoop.FaultCollector;
import org.voltdb.hadoop.VoltConfiguration;
import org.voltdb.hadoop.VoltRecord;
import org.voltdb.hadoop.mapreduce.VoltOutputFormat;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.RowWithMetaData;

@PrepareForTest({VoltOutputFormat.class})
@PowerMockIgnore({"org.apache.commons.logging.*","org.apache.log4j.*"})
@RunWith(PowerMockRunner.class)
public class VoltOutputFormatTest {
    static final VoltType [] CTYPES = new VoltType[] {INTEGER,BIGINT,FLOAT,STRING,TIMESTAMP,VARBINARY};
    static final String [] HOSTS = new String[]{"uno","due"};

    @Mock
    TaskAttemptContext context;

    @Mock
    Configuration conf;

    @Mock
    VoltConfiguration vc;

    @Mock
    CSVBulkDataLoader loader;

    @Captor
    ArgumentCaptor<FaultCollector> fccptr;

    VoltOutputFormat ofmt;
    RecordWriter<Text,VoltRecord> wrtr;

    long now = System.currentTimeMillis();

    @Before
    public void setup() throws Exception {
        PowerMockito.whenNew(VoltConfiguration.class).withAnyArguments().thenReturn(vc);

        when(context.getConfiguration()).thenReturn(conf);
        when(conf.get(eq("mapred.voltdb.table.name"))).thenReturn("THING");
        when(conf.getStrings(eq("mapred.voltdb.hostname"),any(HOSTS.getClass()))).thenReturn(HOSTS);
        when(vc.getBulkLoader(fccptr.capture())).thenReturn(loader);
        when(vc.getTableColumnTypes()).thenReturn(CTYPES);

        ofmt = new VoltOutputFormat();
        wrtr = ofmt.getRecordWriter(context);
    }

    @Test
    public void testCheckOutputSpec()  throws Exception {
        assertNotNull(fccptr.getValue());
        verify(vc,atLeast(1)).getBulkLoader(any(FaultCollector.class));
        verify(vc,atLeast(1)).getTableColumnTypes();
        ofmt.checkOutputSpecs(context);
    }

    @Test
    public void testLoaderIsInvokedCorrectly() throws Exception {
        Object [] fields = {1,1L,1.1D,"1",new Date(), "1".getBytes()};
        wrtr.write(new Text("hello"), new VoltRecord("THING",fields));
        verify(loader,times(1)).insertRow(any(RowWithMetaData.class), eq(fields));
        wrtr.close(null);
    }

    @Test
    public void testTollerateMinimalLoadFailures() throws Exception {
        FaultCollector fc = fccptr.getValue();

        for (int i = 0; i < 500; ++i) {
            VoltRecord vr = generateRecord(i);
            wrtr.write(new Text("THINGS"), vr);
        }
        for (int i = 0; i < 4; ++i) {
            VoltRecord vr = generateRecord(i);
            String error = String.format("Error number %4d", i);
            fc.handleError(metaDataFor(vr), mockFailure(error), error);
        }
        for (int i = 500; i < 1000; ++i) {
            VoltRecord vr = generateRecord(i);
            wrtr.write(new Text("THINGS"), vr);
        }
        wrtr.close(null);
        verify(loader,times(1000)).insertRow(any(RowWithMetaData.class),any(Object[].class));
    }

    @Test
    public void testThrowOnReachedFaultLimit() throws Exception {
        FaultCollector fc = fccptr.getValue();

        for (int i = 0; i < 500; ++i) {
            VoltRecord vr = generateRecord(i);
            wrtr.write(new Text("THINGS"), vr);
        }
        for (int i = 0; i < 100; ++i) {
            VoltRecord vr = generateRecord(i);
            String error = String.format("Error number %4d", i);
            fc.handleError(metaDataFor(vr), mockFailure(error), error);
        }
        VoltRecord vr = generateRecord(500);
        try {
            wrtr.write(new Text("THINGS"), vr);
            wrtr.close(null);
            fail("allowed to go over fault threshold");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("reached the maximum of allowable errors"));
        }
        verify(loader,times(501)).insertRow(any(RowWithMetaData.class),any(Object[].class));
    }

    ClientResponse mockFailure(final String error) {
        ClientResponse cr = mock(ClientResponse.class);
        when(cr.getStatusString()).thenReturn(error);
        return cr;
    }

    RowWithMetaData metaDataFor(VoltRecord vr) {
        return new RowWithMetaData(new WeakReference<VoltRecord>(vr), -1);
    }

    VoltRecord generateRecord(int i) {
        String str = String.format("Record number %5d", i);
        long li = i;
        double di = (i) + 0.12345D;
        Date ti = new Date(now+i);
        return new VoltRecord("THING",i,li,di,str,ti,str.getBytes());
    }
}
