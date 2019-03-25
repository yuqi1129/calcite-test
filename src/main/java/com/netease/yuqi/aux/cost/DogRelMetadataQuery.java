package com.netease.yuqi.aux.cost;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/12/9 下午4:02
 */

import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class DogRelMetadataQuery extends RelMetadataQuery {
	public DogRelMetadataQuery(JaninoRelMetadataProvider metadataProvider, RelMetadataQuery prototype) {
		super(THREAD_PROVIDERS.get(), EMPTY);
	}

	public DogRelMetadataQuery() {
		super(THREAD_PROVIDERS.get(), EMPTY);
	}

	public static DogRelMetadataQuery instance() {
		THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(DogRelMetaDataProvider.INSTANCE));
		return new DogRelMetadataQuery();
	}

	public static final DogRelMetadataQuery INSTANCE = instance();
}
