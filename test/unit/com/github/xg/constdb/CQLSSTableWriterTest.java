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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
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
        dataDir = new File(tempFolder.newFolder().getAbsolutePath() + File.separator + keyspace + File.separator + table);
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
                .inDirectory(dataDir)
                .forTable(schema)
                .using(insert).build();

        writer.addRow(0, "test1", 24);
        writer.addRow(1, "test2", 44);
        writer.addRow(2, "test3", 42);
        writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));


        writer.close();

        for (File file : dataDir.listFiles()) {
            System.out.println(dataDir.toPath().relativize(file.toPath()));
        }

        // try read sstable with its api, this is the ideal and also simplest way to read data
        TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspace, table);
        Descriptor desc = new Descriptor(dataDir, keyspace, table, 1);
        SSTableReader sstable = SSTableReader.open(desc);
        DecoratedKey decoratedKey = tableMetadata.partitioner.decorateKey(ByteBufferUtil.bytes(1));
        UnfilteredRowIterator iter = sstable.iterator(decoratedKey, Slices.ALL, ColumnFilter.all(tableMetadata), false, SSTableReadsListener.NOOP_LISTENER);
        for (UnfilteredRowIterator it = iter; it.hasNext(); ) {
            Unfiltered o = it.next();
            System.out.println("row: " + o);
        }

        // All the following tries failed, because they require a well initialized keyspace, just can't figure it out for now.
        // Keyspace.mockKS()
        // try to reuse c*
//        Keyspace.setInitialized();
//        CQLStatement statement = QueryProcessor.parseStatement(String.format("select * from %s where k = 1", qualifiedTable)).prepare(ClientState.forInternalCalls());
//        ResultMessage resultMessage = statement.executeLocally(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
//        System.out.println(resultMessage);

        // try query command
//        SinglePartitionReadQuery singlePartitionReadQuery = SinglePartitionReadQuery.create(tableMetadata, (int) System.currentTimeMillis() / 1000, decoratedKey, null, null);
//        ReadExecutionController executionController = singlePartitionReadQuery.executionController();
//        UnfilteredPartitionIterator iter = singlePartitionReadQuery.executeLocally(executionController);
//        for (UnfilteredPartitionIterator it = iter; it.hasNext(); ) {
//            Object o = it.next();
//            System.out.println(o);
//        }
        sstable.selfRef().release();
    }
}
