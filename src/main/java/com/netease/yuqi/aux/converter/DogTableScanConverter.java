package com.netease.yuqi.aux.converter;
/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/9/25 下午9:20
 */

import com.netease.yuqi.aux.rel.DogRel;
import com.netease.yuqi.aux.rel.DogTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class DogTableScanConverter extends ConverterRule {

	public static final DogTableScanConverter INSTANCE = new DogTableScanConverter(
			EnumerableTableScan.class,
			EnumerableConvention.INSTANCE,
			DogRel.CONVENTION,
			"DogTableScan"
	);

	@Override
	public boolean matches(RelOptRuleCall call) {
		return super.matches(call);
	}

	public DogTableScanConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
		super(clazz, in, out, description);
	}

	@Override
	public RelNode convert(RelNode rel) {
		EnumerableTableScan tableScan = (EnumerableTableScan) rel;
		return new DogTableScan(tableScan.getCluster(), RelTraitSet.createEmpty().plus(DogRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()), tableScan.getTable());
	}
}
