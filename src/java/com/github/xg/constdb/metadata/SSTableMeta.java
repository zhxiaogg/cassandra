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

package com.github.xg.constdb.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;

public class SSTableMeta {
    private final String keyspace;
    private final String tableName;
    private final String partitioner;
    private final List<SSTableColumn> partitionKeys;
    private final List<SSTableColumn> clusteringKeys;
    private final List<SSTableColumn> regularColumns;

    @JsonCreator
    public SSTableMeta(@JsonProperty("keyspace") String keyspace,
                       @JsonProperty("tableName") String tableName,
                       @JsonProperty("partitioner") String partitioner,
                       @JsonProperty("partitionKeys") List<SSTableColumn> partitionKeys,
                       @JsonProperty("clusteringKeys") List<SSTableColumn> clusteringKeys,
                       @JsonProperty("regularColumns") List<SSTableColumn> regularColumns) {

        this.keyspace = keyspace;
        this.tableName = tableName;
        this.partitioner = partitioner;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.regularColumns = regularColumns;
    }

    @JsonIgnore
    public TableMetadata getTableMetaData() {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, tableName);
        if (!this.partitioner.equals(Murmur3Partitioner.class.getCanonicalName())) {
            throw new IllegalArgumentException("Unsupported partitioner: " + this.partitioner);
        }
        builder.partitioner(Murmur3Partitioner.instance);
        partitionKeys.forEach(c -> builder.addPartitionKeyColumn(c.getName(), c.getCql3Type().getType()));
        clusteringKeys.forEach(c -> builder.addClusteringColumn(c.getName(), c.getCql3Type().getType()));
        regularColumns.forEach(c -> builder.addRegularColumn(c.getName(), c.getCql3Type().getType()));
        return builder.build();
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public List<SSTableColumn> getPartitionKeys() {
        return partitionKeys;
    }

    public List<SSTableColumn> getClusteringKeys() {
        return clusteringKeys;
    }

    public List<SSTableColumn> getRegularColumns() {
        return regularColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableMeta that = (SSTableMeta) o;
        return Objects.equals(keyspace, that.keyspace) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(partitioner, that.partitioner) &&
                Objects.equals(partitionKeys, that.partitionKeys) &&
                Objects.equals(clusteringKeys, that.clusteringKeys) &&
                Objects.equals(regularColumns, that.regularColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, tableName, partitioner, partitionKeys, clusteringKeys, regularColumns);
    }

    @Override
    public String toString() {
        return "SSTableMeta{" +
                "keyspace='" + keyspace + '\'' +
                ", tableName='" + tableName + '\'' +
                ", partitioner='" + partitioner + '\'' +
                ", partitionKeys=" + partitionKeys +
                ", clusteringKeys=" + clusteringKeys +
                ", regularColumns=" + regularColumns +
                '}';
    }
}
