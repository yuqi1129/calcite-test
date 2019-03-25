package com.netease.yuqi.aux.rel;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/9/30 下午2:27
 */

import com.netease.yuqi.aux.cost.DogRelMetadataQuery;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class DogTableScan extends TableScan implements DogRel {
	private RelOptCost cost;

	public DogTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
		super(cluster, traitSet, table);
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		//return super.computeSelfCo(planner, mq);

		if (cost != null) {
			return cost;
		}

		cost = DogRelMetadataQuery.INSTANCE.getCumulativeCost(this);
		return cost;
		//return DogRelMetadataQuery.INSTNACE.getCumulativeCost(this);
		//return DogRelMetadataQuery.INSTNACE.getNonCumulativeCost(this);
	}
}
