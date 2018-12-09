package com.netease.yuqi.aux.cost;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/10/18 下午9:41
 */

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

public abstract class DogBuiltInMetadata extends BuiltInMetadata {


	//total cost of something
	public interface TotalCost extends Metadata {
		MetadataDef<DogBuiltInMetadata.TotalCost> DEF = MetadataDef.of(DogBuiltInMetadata.TotalCost.class,
				DogBuiltInMetadata.TotalCost.Handler.class, BuiltInMethod.SELECTIVITY.method);

		RelOptCost getTotalCost(RexNode predicate);

		/** Handler API. */
		interface Handler extends MetadataHandler<DogBuiltInMetadata.TotalCost> {
			RelOptCost getTotalCost(RelNode r, RelMetadataQuery mq);
		}
	}
}
