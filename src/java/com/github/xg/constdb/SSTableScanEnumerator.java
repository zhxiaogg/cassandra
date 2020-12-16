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
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class SSTableScanEnumerator implements Enumerator<Object[]> {
    private final ISSTableScanner scanner;

    private Object[] current = null;
    private UnfilteredRowIterator iterator;

    public SSTableScanEnumerator(SSTableReader reader) {
        scanner = reader.getScanner();
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        while (iterator == null || !iterator.hasNext()) {
            if (!scanner.hasNext()) {
                current = null;
                iterator = null;
                return false;
            } else {
                iterator = scanner.next();
            }
        }
        current = convert((Row) iterator.next());
        return true;
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
    public void reset() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void close() {
        scanner.close();
    }
}
