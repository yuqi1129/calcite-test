package com.netease.yuqi.aux.cost;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/10/18 下午9:35
 */

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdAllPredicates;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdExpressionLineage;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMdMinRowCount;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdTableReferences;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;

public class DogRelMetaDataProvider extends ChainedRelMetadataProvider {
	public static final DogRelMetaDataProvider INSTANCE = new DogRelMetaDataProvider();


	public DogRelMetaDataProvider() {
		super(
				ImmutableList.of(
						RelMdPercentageOriginalRows.SOURCE,
						RelMdColumnOrigins.SOURCE,
						RelMdExpressionLineage.SOURCE,
						RelMdTableReferences.SOURCE,
						RelMdNodeTypes.SOURCE,
						RelMdRowCount.SOURCE,
						RelMdMaxRowCount.SOURCE,
						RelMdMinRowCount.SOURCE,
						RelMdUniqueKeys.SOURCE,
						RelMdColumnUniqueness.SOURCE,
						RelMdPopulationSize.SOURCE,
						RelMdSize.SOURCE,
						RelMdParallelism.SOURCE,
						RelMdDistribution.SOURCE,
						RelMdMemory.SOURCE,
						RelMdDistinctRowCount.SOURCE,
						RelMdSelectivity.SOURCE,
						RelMdExplainVisibility.SOURCE,
						RelMdPredicates.SOURCE,
						RelMdAllPredicates.SOURCE,
						RelMdCollation.SOURCE,
						DogCumulativeCost.SOURCE
				)
		);
	}
}
