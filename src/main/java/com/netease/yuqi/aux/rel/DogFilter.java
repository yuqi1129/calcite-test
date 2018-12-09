package com.netease.yuqi.aux.rel;
/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/9/30 下午2:24
 */

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class DogFilter extends Filter implements DogRel {
	public DogFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
		super(cluster, traits, child, condition);
	}

	@Override
	public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
		return null;
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
//		RelNode input = this.input;

//		RelOptCost inputCost;
//		if (input instanceof RelSubset) {
//			inputCost = ((RelSubset) input).
//		}
//
//		RelOptCost inputCost = mq.getCumulativeCost(this.input);


		return mq.getCumulativeCost(this);


	}
}
