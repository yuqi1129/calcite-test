package com.netease.yuqi.aux.cost;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/10/18 下午9:58
 */

import com.netease.yuqi.aux.rel.DogProject;
import com.netease.yuqi.aux.rel.DogTableScan;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public class DogCumulativeCost implements MetadataHandler<BuiltInMetadata.CumulativeCost> {

	public static final RelMetadataProvider SOURCE =
			ReflectiveRelMetadataProvider.reflectiveSource(
					BuiltInMethod.CUMULATIVE_COST.method, new DogCumulativeCost());

	public DogCumulativeCost() {
	}

	@Override
	public MetadataDef<BuiltInMetadata.CumulativeCost> getDef() {
		return BuiltInMetadata.CumulativeCost.DEF;
	}

	public RelOptCost getCumulativeCost(DogTableScan tableScan, RelMetadataQuery query) {
		//
		return tableScan.getCluster().getPlanner().getCostFactory().makeCost(20000, 10, 1);
	}

	public RelOptCost getCumulativeCost(DogProject dogProject, RelMetadataQuery query) {
		//
		RelNode input = dogProject.getInput();

		RelOptCost inputCost;
		if (input instanceof RelSubset) {
			//inputCost = ((RelSubset) input)
		}

		return dogProject.getCluster().getPlanner().getCostFactory().makeCost(20000, 10, 1);
	}



}
