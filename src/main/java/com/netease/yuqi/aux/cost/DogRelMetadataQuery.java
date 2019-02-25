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
		this(null, null);
		THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(DogRelMetaDataProvider.INSTANCE));
	}

	public static final DogRelMetadataQuery INSTNACE = new DogRelMetadataQuery();


}
