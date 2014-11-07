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

package org.voltdb.hadoop;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.util.UUID;

import org.voltdb.VoltType;

/**
 * Base class for all the {@link VoltRecord} adapters
 *
 * @param <R> the adapter return type
 * @param <P> the adapter parameter type
 * @param <E> the exception type that the adapter throws
 */
public abstract class RecordAdapter<R, P, E extends Exception> {

    /* the tables column types */
    protected final TypeAide [] m_types;
    /*
     * the signature is a MD5 digest all the column types names
     * concatenated together
     */
    protected final UUID m_signature;

    public RecordAdapter(VoltType [] types) {
        checkArgument(
                types != null && types.length > 0,
                "given types is a null or empty array");

        StringBuilder sb = new StringBuilder(1024);
        m_types = new TypeAide[types.length];
        for (int i = 0; i < types.length; ++i) {
            m_types[i] = TypeAide.forType(types[i]);
            sb.append(types[i].name());
        }
        m_signature = Digester.digestMD5asUUID(sb.toString());
    }

    /**
     * Adapts a {@linkplain VoltRecord} from or to the given parameter
     * @param param source or destination of the adaptation
     *
     * @param record a {@linkplain VoltRecord}
     * @return the result of the adaptation
     * @throws E when the adaptation fails
     */
    public abstract R adapt(P param, VoltRecord record) throws E;

    /**
     * @return the serialization signature header value
     */
    public UUID getSignature() {
        return m_signature;
    }

}
