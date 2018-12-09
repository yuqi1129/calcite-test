package com.netease.yuqi.aux.rel;
/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/9/23 下午9:09
 */

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

//show final relnode implemnt dog
public interface DogRel extends RelNode {
	Convention CONVENTION = new Convention.Impl("Dog", DogRel.class);
}
