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

package com.github.xg.constdb.rels;

import com.github.xg.constdb.SSTable;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.Pair;
import org.apache.cassandra.db.DecoratedKey;

import java.util.Collections;
import java.util.List;

public class SSTableFilterScan extends TableScan implements EnumerableRel {

    private final List<DecoratedKey> keys;
    private final List<String> selection;

    public SSTableFilterScan(RelOptCluster cluster, RelOptTable table, List<DecoratedKey> keys, List<String> selection) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
        this.keys = keys;
        this.selection = selection;
    }

    public SSTableFilterScan(RelOptCluster cluster, RelOptTable table) {
        this(cluster, table, Collections.emptyList(), Collections.emptyList());
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer prefer) {
        PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        prefer.preferArray());

        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(table.getExpression(SSTable.class),
                                "scan", implementor.getRootExpression(),
                                Expressions.constant(keys), Expressions.constant(selection))));
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return null;
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return null;
    }

    @Override
    public DeriveMode getDeriveMode() {
        return null;
    }
}
