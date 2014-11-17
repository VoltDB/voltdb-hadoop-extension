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

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.voltdb.hadoop.mapred.VoltOutputFormat;

/**
 * A {@link StorageHandler} implementation that supports insert only access to
 * VoltDB tables. This handler supports the following Hive column types
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
@SuppressWarnings({ "rawtypes", "deprecation" })
public class VoltStorageHandler extends  DefaultStorageHandler {

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return VoidInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return VoltOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return VoltSerDe.class;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
            Map<String, String> jobProperties) {
        throw new UnsupportedOperationException("this storage handler does not support reads");
    }
}
