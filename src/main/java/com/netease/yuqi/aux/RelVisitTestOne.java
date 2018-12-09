package com.netease.yuqi.aux;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/6/25 下午9:42
 */

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

public class RelVisitTestOne extends RelVisitor {
	List<TableScan> list = Lists.newArrayList();


	@Override
	public void visit(RelNode node, int ordinal, RelNode parent) {
		if (node instanceof TableScan) {
			list.add((TableScan) node);
		}
		super.visit(node, ordinal, parent);
	}
}
