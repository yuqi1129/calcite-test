package com.netease.yuqi.aux.cost;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/12/9 下午4:15
 */

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class DogCostAccumulator {

	public DogCostAccumulator() {
	}

	public static final DogCostAccumulator INSTACNE = new DogCostAccumulator();

	public RelOptCost getAccumulative(RelNode node, RelMetadataQuery mq) {
		//todo
		return null;
	}


	public RelOptCost getAccumulative(TableScan node, RelMetadataQuery mq) {
		//make the most a constant value
		//todo use row count as the cost
		return node.getCluster().getPlanner().getCostFactory().makeCost(1.0, 1.0, 1.0);
		//return node.getCluster().getPlanner().getCostFactory().makeCost(node.estimateRowCount(mq), 0, 0);
	}


	public RelOptCost getAccumulative(Project project, RelMetadataQuery mq) {

		RelNode input = project.getInput();

		double childRows = 0;
		if (input instanceof RelSubset) {
			//childCost = ((RelSubset) input).bestCost().getRows();
		} else {
			childRows = input.computeSelfCost(project.getCluster().getPlanner(), mq).getRows();
		}
		//make some estimate

		//do some filter for example
		/**
		 * where id < 1 , childRows *= 0.01
		 * where id is not null, childRows  *= 0.5
		 * ....
		 */
		return input.computeSelfCost(project.getCluster().getPlanner(), mq);
	}

	public RelOptCost getAccumulative(Filter filter, RelMetadataQuery mq) {
		//make some estimate

		RelNode node = filter.getInput();
		if (node instanceof RelSubset) {
			//todo
		} else {
			//todo
		}

		//just for test
		return filter.getInput().computeSelfCost(filter.getCluster().getPlanner(), mq);
	}

	//todo for more implement;
}
