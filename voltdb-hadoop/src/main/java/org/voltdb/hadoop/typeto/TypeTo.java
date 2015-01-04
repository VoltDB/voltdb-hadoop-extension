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

package org.voltdb.hadoop.typeto;

import org.voltdb.hadoop.TypeAide;

import com.google_voltpatches.common.base.Function;

public abstract class TypeTo<T> {

    protected final TypeAide m_typeTo;

    public TypeTo(TypeAide type, boolean strictlyCompatible) {
        m_typeTo = type;
        if (!isCompatibleWith(m_typeTo, strictlyCompatible)) {
            throw new IncompatibleException("not compatible with " + type);
        }
    }

    public TypeTo(TypeAide type) {
        this(type, true);
    }

    public abstract Function<T,Object> getAdjuster();

    public abstract Function<T,Object> getAdjusterFor(TypeAide type);

    public abstract boolean isCompatibleWith(TypeAide type, boolean strictly);

}
