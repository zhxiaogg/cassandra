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

import com.github.xg.constdb.rels.SSTableScan;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SSTable extends AbstractQueryableTable
        implements TranslatableTable {
    private final TableMetadata metadata;
    private final ConstDBSchema schema;

    protected SSTable(TableMetadata metadata, ConstDBSchema schema) {
        super(Object[].class);
        this.metadata = metadata;
        this.schema = schema;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String s) {
        return null;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return new SSTableScan(toRelContext.getCluster(), relOptTable);
    }

    /**
     * Returns an enumerable over a given projection of the fields.
     */
    @SuppressWarnings("unused") // called from generated code
    public Enumerable<Object> filterScan(final DataContext root,
                                         List<DecoratedKey> keys, List<String> selection) {
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        // TODO: this should be part of the schema
        File dataDir = new File("/tmp/sstable");
        Descriptor desc = new Descriptor(dataDir, metadata.keyspace, metadata.name, 1);
        SSTableReader reader = SSTableReader.open(desc);
        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                return (Enumerator) new SSTableEnumberator(
                        keys,
                        reader,
                        selection);
            }
        };
    }

    public Enumerable<Object> scan(final DataContext root,
                                   List<DecoratedKey> keys, List<String> selection) {
        // final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        // TODO: this should be part of the schema
        File dataDir = new File("/tmp/sstable");
        Descriptor desc = new Descriptor(dataDir, metadata.keyspace, metadata.name, 1);
        SSTableReader reader = SSTableReader.open(desc);
        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                return (Enumerator) new SSTableScanEnumerator(reader);
            }
        };
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        return schema.getRelDataType(metadata.name, false).apply(relDataTypeFactory);
    }

    public String toString() {
        return "CassandraTable {" + metadata.name + "}";
    }

}
