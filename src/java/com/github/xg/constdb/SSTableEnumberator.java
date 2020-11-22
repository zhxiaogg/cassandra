/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.xg.constdb;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class SSTableEnumberator implements Enumerator<Object[]> {
    private Object[] current = null;

    private final UnfilteredRowIterator iterator;

    public SSTableEnumberator(List<DecoratedKey> keys, SSTableReader reader, List<String> selection) {
        TableMetadata metadata = reader.metadata();
        ColumnFilter columnFilter;
        if (selection.isEmpty()) {
            columnFilter = ColumnFilter.all(metadata);
        } else {
            List<ColumnMetadata> columns = selection.stream().map(c -> ByteBuffer.wrap(c.getBytes(StandardCharsets.UTF_8))).map(metadata::getColumn).collect(Collectors.toList());
            columnFilter = ColumnFilter.selection(RegularAndStaticColumns.builder().addAll(columns).build());
        }
        // TODO: if keys list is empty, scan all data from the table
        List<UnfilteredRowIterator> iters = keys.stream().map(k -> reader.iterator(k, Slices.ALL, columnFilter, false, SSTableReadsListener.NOOP_LISTENER)).collect(Collectors.toList());
        iterator = UnfilteredRowIterators.merge(iters);
    }

    private Object[] convert(Row row) {
        Object[] values = new Object[row.columnCount()];
        int i = 0;
        for (ColumnData d : row) {
            Cell<?> c = (Cell<?>) d;
            Object value = CellUtils.getValue(c, c.column().type);
            values[i++] = value;
        }
        return values;
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        boolean hasNext = iterator.hasNext();
        if (hasNext) {
            current = convert((Row) iterator.next());
        } else {
            current = null;
        }
        return hasNext;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        iterator.close();
    }
}
