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
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.schema.ColumnMetadata;

import java.util.Objects;

public class SSTableColumn {
    private final String name;
    private final String typeName;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SSTableColumn(@JsonProperty("name") String name, @JsonProperty("typeName") String typeName) {
        this.name = name;
        this.typeName = typeName;
    }

    public static SSTableColumn fromColumnMetadata(ColumnMetadata columnMetadata) {
        String typeName = ((CQL3Type.Native) columnMetadata.type.asCQL3Type()).name();
        String name = columnMetadata.name.toString();
        return new SSTableColumn(name, typeName);
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    @JsonIgnore
    public CQL3Type getCql3Type() {
        return CQL3Type.Native.valueOf(typeName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableColumn that = (SSTableColumn) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(typeName, that.typeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, typeName);
    }

    @Override
    public String toString() {
        return "SSTableColumn{" +
                "name='" + name + '\'' +
                ", typeName='" + typeName + '\'' +
                '}';
    }
}
