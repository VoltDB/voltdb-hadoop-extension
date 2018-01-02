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

package org.voltdb.hadoop;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.RowWithMetaData;

/**
 * Collects asynchronous faults from the VoltDB's bulk loader, and may
 * be used to check whether or not faults were issued by a loader.
 */
public class FaultCollector implements BulkLoaderErrorHandler {

    private final static Log LOG = LogFactory.getLog("org.voltdb.hadoop");

    public static int MAXFAULTS = 10;
    public static int CHECKEVERY = 30;

    private final LinkedBlockingQueue<Fault> m_queue;
    private final TextOutputAdapter m_adapter;

    private volatile int m_faultCount = 0;
    private volatile int m_checkCount = 0;

    private final int m_maxBulkLoaderErrors;

    /**
     * Constructs a collector. It uses the given adapter to log
     * rows that are the source of the faults
     *
     * @param adapter to format {@linkplain VoltRecord}
     * @param maxErrors  The maximal number of errors before CSVBulkLoader stops processing input
     */
    public FaultCollector(TextOutputAdapter adapter, int maxErrors) {
        m_adapter = adapter;
        m_maxBulkLoaderErrors = maxErrors > 0 ? maxErrors : MAXFAULTS;
        m_queue = new LinkedBlockingQueue<Fault>(m_maxBulkLoaderErrors);
    }

    /**
     * Fault descriptor class. Holds tenuous references to client responses
     * and volt records to mitigate GC pressure
     */
    public final static class Fault {
        final WeakReference<VoltRecord> m_recordRef;
        final WeakReference<ClientResponse> m_respRef;

        @SuppressWarnings("unchecked")
        public Fault(RowWithMetaData rmd, ClientResponse cr) {
            m_recordRef = (WeakReference<VoltRecord>)rmd.rawLine;
            m_respRef = new WeakReference<ClientResponse>(cr);
        }

        public VoltRecord getVoltRecord() {
            return m_recordRef.get();
        }

        public ClientResponse getResponse() {
            return m_respRef.get();
        }

        public void clear() {
            m_respRef.clear();
            m_recordRef.clear();
        }
    }

    /**
     * Loader callback. If it is flooded with faults it drops them
     */
    @Override
    public boolean handleError(RowWithMetaData rmd, ClientResponse cr, String error) {
        ++m_faultCount;
        Fault fault = new Fault(rmd,cr);
        if (!m_queue.offer(fault)) {
            fault.clear();
        };
        return true;
    }

    @Override
    public boolean hasReachedErrorLimit() {
        final int currentCount = m_faultCount;
        return currentCount >= m_maxBulkLoaderErrors;
    }

    public void check(boolean eagerly) throws IOException {
        final int checkCount = (eagerly ? CHECKEVERY : ++m_checkCount);
        boolean checkNow = false;

        if ((checkNow = checkCount >= CHECKEVERY)) {
            m_checkCount = 0;
        }
        if (checkNow && !m_queue.isEmpty()) {
            StringBuilder sb = new StringBuilder(1024);

            List<Fault> faults = new ArrayList<Fault>();
            m_queue.drainTo(faults);

            for (Fault fault: faults) {
                VoltRecord rec = fault.getVoltRecord();
                ClientResponse rsp = fault.getResponse();
                if (rsp != null && rec != null) {
                    sb.setLength(0);
                    sb.append("Failed to load record into VoltDB\n");
                    sb.append("+cause: \"").append(rsp.getStatusString()).append("\"\n");
                    sb.append("+--row: [").append(m_adapter.adapt(null, rec).toString());
                    sb.append("]");
                    LOG.error(sb.toString());
                } else {
                    LOG.error("Failed to load record into VoltDB: [error details were garbage collected]");
                }
            }
        }
        if (hasReachedErrorLimit()) {
            throw new IOException("VoltDB loader reached the maximum of allowable errors: check logs for specific load errors. max error:" +
                       this.m_maxBulkLoaderErrors + " fault count:" + this.m_faultCount);
        }
    }
}
