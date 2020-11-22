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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.cassandra.cql3.CQL3Type;

import java.util.Map;

/**
 * CqlToSqlTypeConversionRules defines mappings from CQL types to
 * corresponding SQL types.
 */
public class CqlToSqlTypeConversionRules {
    //~ Static fields/initializers ---------------------------------------------

    private static final CqlToSqlTypeConversionRules INSTANCE =
            new CqlToSqlTypeConversionRules();

    //~ Instance fields --------------------------------------------------------

    private final Map<CQL3Type, SqlTypeName> rules =
            ImmutableMap.<CQL3Type, SqlTypeName>builder()
                    .put(CQL3Type.Native.UUID, SqlTypeName.CHAR)
                    .put(CQL3Type.Native.TIMEUUID, SqlTypeName.CHAR)

                    .put(CQL3Type.Native.ASCII, SqlTypeName.VARCHAR)
                    .put(CQL3Type.Native.TEXT, SqlTypeName.VARCHAR)
                    .put(CQL3Type.Native.VARCHAR, SqlTypeName.VARCHAR)

                    .put(CQL3Type.Native.INT, SqlTypeName.INTEGER)
                    .put(CQL3Type.Native.VARINT, SqlTypeName.INTEGER)
                    .put(CQL3Type.Native.BIGINT, SqlTypeName.BIGINT)
                    .put(CQL3Type.Native.TINYINT, SqlTypeName.TINYINT)
                    .put(CQL3Type.Native.SMALLINT, SqlTypeName.SMALLINT)

                    .put(CQL3Type.Native.DOUBLE, SqlTypeName.DOUBLE)
                    .put(CQL3Type.Native.FLOAT, SqlTypeName.REAL)
                    .put(CQL3Type.Native.DECIMAL, SqlTypeName.DOUBLE)

                    .put(CQL3Type.Native.BLOB, SqlTypeName.VARBINARY)

                    .put(CQL3Type.Native.BOOLEAN, SqlTypeName.BOOLEAN)

                    .put(CQL3Type.Native.COUNTER, SqlTypeName.BIGINT)

                    // number of nanoseconds since midnight
                    .put(CQL3Type.Native.TIME, SqlTypeName.BIGINT)
                    .put(CQL3Type.Native.DATE, SqlTypeName.DATE)
                    .put(CQL3Type.Native.TIMESTAMP, SqlTypeName.TIMESTAMP)
                    .build();

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the
     * {@link org.apache.calcite.util.Glossary#SINGLETON_PATTERN singleton}
     * instance.
     */
    public static CqlToSqlTypeConversionRules instance() {
        return INSTANCE;
    }

    /**
     * Returns a corresponding {@link SqlTypeName} for a given CQL type name.
     *
     * @param name the CQL type name to lookup
     * @return a corresponding SqlTypeName if found, ANY otherwise
     */
    public SqlTypeName lookup(CQL3Type name) {
        return rules.getOrDefault(name, SqlTypeName.ANY);
    }
}
