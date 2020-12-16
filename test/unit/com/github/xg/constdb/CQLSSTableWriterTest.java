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

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class CQLSSTableWriterTest {
    private static final AtomicInteger idGen = new AtomicInteger(0);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private String keyspace;
    private String table;
    private String qualifiedTable;
    private File dataDir;

    @Before
    public void perTestSetup() throws IOException {
        keyspace = "cql_keyspace" + idGen.incrementAndGet();
        table = "table" + idGen.incrementAndGet();
        qualifiedTable = keyspace + '.' + table;
        String rootDir = tempFolder.newFolder().getAbsolutePath();
        dataDir = new File(rootDir + File.separator + keyspace + File.separator + table);
        assert dataDir.mkdirs();
    }

    @Test
    public void create_new_table() throws IOException {
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                + "  k int PRIMARY KEY,"
                + "  v1 text,"
                + "  v2 int"
                + ")";
        String insert = "INSERT INTO " + qualifiedTable + " (k, v1, v2) VALUES (?, ?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                //.sorted()
                .inDirectory(dataDir)
                .forTable(schema)
                .using(insert)
                .build();

        writer.addRow(0, "test1", 24);
        writer.addRow(1, "test2", 44);
        writer.addRow(2, "test3", 42);
        writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));


        writer.close();

        for (File file : dataDir.listFiles()) {
            System.out.println(dataDir.toPath().relativize(file.toPath()));
        }

        TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspace, table);
        SchemaSerDer.serialize(tableMetadata, dataDir.toPath().resolve("schema.json"));


        // ================= read side =================//
        // try read sstable with its api, this is the ideal and also simplest way to read data
        // TODO: what's this?
        DatabaseDescriptor.clientInitialization();
        TableMetadata metadata2 = SchemaSerDer.deserialize(dataDir.toPath().resolve("schema.json"));

        Descriptor desc = new Descriptor(dataDir, keyspace, table, 1);
        // TODO: waht's this?
        TableMetadataRef metadata = TableMetadataRef.forOfflineTools(metadata2);
        SSTableReader sstable = SSTableReader.open(desc, metadata);
        DecoratedKey decoratedKey = metadata2.partitioner.decorateKey(ByteBufferUtil.bytes(1));
        UnfilteredRowIterator iter = sstable.iterator(decoratedKey, Slices.ALL, ColumnFilter.all(metadata2), false, SSTableReadsListener.NOOP_LISTENER);
        for (UnfilteredRowIterator it = iter; it.hasNext(); ) {
            Unfiltered o = it.next();
            for (ColumnData d : (Row) o) {
                Cell<?> cell = (Cell<?>) d;
                Object value = getValue(cell, cell.column().type);
                System.out.println(value);
            }
            System.out.println("row: " + o);
        }

        // TODO: what's this?
        sstable.selfRef().release();
    }

    public static <V> Object getValue(Cell<V> cell, AbstractType<?> type) {
        return type.getSerializer().deserialize(cell.value(), cell.accessor());
    }
}
