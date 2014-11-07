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

package org.voltdb.pig

import groovy.lang.Closure;

import java.util.List;
import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;

class ResourceSchemaBuilder {
    ResourceSchema schema
    List<ResourceFieldSchema> fields = []

    ResourceSchemaBuilder() {
        schema = new ResourceSchema()
    }

    ResourceSchema make(Closure stanza) {
        Closure clone = stanza.clone()
        clone.delegate = this
        clone.resolveStrategy = Closure.DELEGATE_FIRST
        clone.call()

        schema.fields = fields as ResourceFieldSchema []

        return schema
    }

    void field(Map attributes) {
        ResourceFieldSchema field = new ResourceFieldSchema()

        field.name = attributes['name']
        field.type = attributes['type']
        field.schema = schema

        fields << field
    }
}
