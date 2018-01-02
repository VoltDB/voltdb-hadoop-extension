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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * A data type mirror for {@link VoltType} that supports the visitor pattern
 */
public enum TypeAide {

    TINYINT(VoltType.TINYINT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitTinyInt(p,v);
        }
    },
    SMALLINT(VoltType.SMALLINT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitSmallInt(p,v);
        }
    },
    INTEGER(VoltType.INTEGER) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitInteger(p,v);
        }
    },
    BIGINT(VoltType.BIGINT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitBigInt(p,v);
        }
    },
    FLOAT(VoltType.FLOAT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitFloat(p,v);
        }
    },
    TIMESTAMP(VoltType.TIMESTAMP) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitTimestamp(p,v);
        }
    },
    STRING(VoltType.STRING) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitString(p,v);
        }
    },
    VARBINARY(VoltType.VARBINARY) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitVarBinary(p,v);
        }
    },
    DECIMAL(VoltType.DECIMAL) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitDecimal(p,v);
        }
    };

    private static final Map<VoltType, TypeAide> m_typeMap =
            new EnumMap<VoltType, TypeAide>(VoltType.class);

    private static List<TypeAide> values = ImmutableList.copyOf(values());

    static {
        for (TypeAide dt: values()) {
            m_typeMap.put(dt.voltType(), dt);
        }
    }

    /**
     * The associated {@link VoltType}
     */
    private final VoltType m_type;

    TypeAide(VoltType type) {
        m_type = type;
    }

    public VoltType voltType() {
        return m_type;
    }

    public interface Visitor<R,P,E extends Exception> {
        R visitTinyInt(P p, Object v) throws E;
        R visitSmallInt(P p, Object v) throws E;
        R visitInteger(P p, Object v) throws E;
        R visitBigInt(P p, Object v) throws E;
        R visitFloat(P p, Object v) throws E;
        R visitTimestamp(P p, Object v) throws E;
        R visitString(P p, Object v) throws E;
        R visitVarBinary(P p, Object v) throws E;
        R visitDecimal(P p, Object v) throws E;
    }

    public static TypeAide forOrdinal(int ordinal) {
        return values.get(ordinal);
    }

    public static TypeAide forType(VoltType type) {
        TypeAide dt = m_typeMap.get(type);
        if (dt == null) {
            throw new IllegalArgumentException("no mapping found for given volt type");
        }
        return dt;
    }

    public abstract <R,P,E extends Exception> R accept(Visitor<R,P,E> vtor, P p, Object v) throws E;
}
