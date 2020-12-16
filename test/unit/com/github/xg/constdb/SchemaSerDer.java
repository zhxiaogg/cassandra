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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xg.constdb.metadata.SSTableColumn;
import com.github.xg.constdb.metadata.SSTableMeta;
import org.apache.cassandra.schema.TableMetadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaSerDer {
    public static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    public static void serialize(TableMetadata tableMetadata, Path path) {
        String keyspace = tableMetadata.keyspace;
        String tableName = tableMetadata.name;
        String partitioner = tableMetadata.partitioner.getClass().getCanonicalName();
        List<SSTableColumn> partitionKeys = tableMetadata.partitionKeyColumns().stream().map(SSTableColumn::fromColumnMetadata).collect(Collectors.toList());
        List<SSTableColumn> clusteringKeys = tableMetadata.clusteringColumns().stream().map(SSTableColumn::fromColumnMetadata).collect(Collectors.toList());
        List<SSTableColumn> regularColumns = tableMetadata.regularColumns().stream().map(SSTableColumn::fromColumnMetadata).collect(Collectors.toList());

        SSTableMeta tableMeta = new SSTableMeta(keyspace, tableName, partitioner, partitionKeys, clusteringKeys, regularColumns);
        try {
            String tableMetaStr = JSON_OBJECT_MAPPER.writeValueAsString(tableMeta);
            Files.write(path, tableMetaStr.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new IllegalStateException("serialize table meta failed.", e);
        }
    }

    public static TableMetadata deserialize(Path path) {
        try {
            String json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            SSTableMeta ssTableMeta = JSON_OBJECT_MAPPER.readValue(json, SSTableMeta.class);
            return ssTableMeta.getTableMetaData();
        } catch (IOException e) {
            throw new IllegalStateException("deserialize table meta failed.", e);
        }
    }

}
