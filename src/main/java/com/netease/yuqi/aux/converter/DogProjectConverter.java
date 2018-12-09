package com.netease.yuqi.aux.converter;
/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/9/30 下午2:32
 */

import com.netease.yuqi.aux.rel.DogProject;
import com.netease.yuqi.aux.rel.DogRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class DogProjectConverter extends ConverterRule {

	public static final DogProjectConverter INSTANCE = new DogProjectConverter(
			LogicalProject.class,
			Convention.NONE,
			DogRel.CONVENTION,
			"DogProjectConverter"
	);

	public DogProjectConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
		super(clazz, in, out, description);
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		return super.matches(call);
	}

	@Override
	public RelNode convert(RelNode rel) {
		LogicalProject logicalProject = (LogicalProject) rel;
		RelNode input = convert(logicalProject.getInput(), logicalProject.getInput().getTraitSet().replace(DogRel.CONVENTION).simplify());
		return new DogProject(
				logicalProject.getCluster(),
				RelTraitSet.createEmpty().plus(DogRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
				input,
				logicalProject.getProjects(),
				logicalProject.getRowType()
		);
	}
}
