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

import com.google.common.collect.ImmutableCollection;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.commons.lang3.NotImplementedException;

import java.util.HashMap;
import java.util.Map;

public class ConstDBSchema extends AbstractSchema {

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        for (TableMetadata table : Schema.instance.getTablesAndViews("ttt")) {
            Table t = new SSTable(table, this);
            tables.put(table.name, t);
        }
        return tables;
    }

    public RelProtoDataType getRelDataType(String columnFamily, boolean view) {
        TableMetadata tableMetadata = Schema.instance.getTableMetadata("ttt", columnFamily);
        ImmutableCollection<ColumnMetadata> columns = tableMetadata.columns();

        // Temporary type factory, just for the duration of this method. Allowable
        // because we're creating a proto-type, not a type; before being used, the
        // proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        for (ColumnMetadata column : columns) {
            final SqlTypeName typeName =
                    CqlToSqlTypeConversionRules.instance().lookup(column.type.asCQL3Type());

            switch (typeName) {
                case ARRAY:
                case MULTISET:
                case MAP:
                case STRUCTURED:
                case ANY:
                    throw new NotImplementedException(typeName + " is not supported for now!");
                default:
                    fieldInfo.add(column.name.toString(), typeName).nullable(true);
                    break;
            }
        }

        return RelDataTypeImpl.proto(fieldInfo.build());
    }
}
