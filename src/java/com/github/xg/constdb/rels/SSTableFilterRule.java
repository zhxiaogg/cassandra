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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Collections;
import java.util.List;

public class SSTableFilterRule extends RelRule<SSTableFilterRule.Config> {

    public SSTableFilterRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        final LogicalFilter filter = relOptRuleCall.rel(0);
        final SSTableScan scan = relOptRuleCall.rel(1);
        RexNode condition = filter.getCondition();
        // extract partition keys from condition
        List<DecoratedKey> keys = getPartitionsKeys(condition);
        if (keys == null) {
            // this condition is not supported yet
            return;
        }
        relOptRuleCall.transformTo(new SSTableScan(scan.getCluster(), scan.getTable(), keys, Collections.emptyList()));
    }

    private List<DecoratedKey> getPartitionsKeys(RexNode condition) {
        throw new NotImplementedException("");
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalFilter.class).oneInput(b1 ->
                                b1.operand(SSTableScan.class).noInputs()))
                .as(Config.class);

        @Override
        default SSTableFilterRule toRule() {
            return new SSTableFilterRule(this);
        }
    }
}
