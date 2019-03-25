package com.netease.yuqi.aux;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2019/3/19 下午3:37
 */

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.lang.reflect.Field;

public class DogVolcanoPlanner extends VolcanoPlanner {

	public DogVolcanoPlanner() {
	}

	public DogVolcanoPlanner(RelOptCostFactory costFactory, Context externalContext) {
		super(costFactory, externalContext);
	}

	public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {

		assert rel != null : "pre-condition: rel != null";
		if (rel instanceof RelSubset) {
			try {
				Field field = RelSubset.class.getDeclaredField("bestCost");
				field.setAccessible(true);
				return (RelOptCost) field.get(rel);
				//return ((RelSubset) rel).bestCost;
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("this is a test");
			}
		}
		if (rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
				== Convention.NONE) {
			return costFactory.makeInfiniteCost();
		}
		RelOptCost cost = rel.computeSelfCost(this, mq);

		RelOptCost zeroCost;
		try {
			//子类通过反射获取父类的私有成员变量
			Field zero = VolcanoPlanner.class.getDeclaredField("zeroCost");
			zero.setAccessible(true);
			zeroCost = (RelOptCost) zero.get(this);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		if (!zeroCost.isLt(cost)) {
			// cost must be positive, so nudge it
			cost = costFactory.makeTinyCost();
		}
		for (RelNode input : rel.getInputs()) {
			cost = cost.plus(getCost(input, mq));
		}
		return cost;
	}
}
