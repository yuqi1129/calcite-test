package com.netease.yuqi.aux;
/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/9/23 下午9:57
 */

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class DogSqlValidator extends SqlValidatorImpl {
	public DogSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, SqlConformance conformance) {
		super(opTab, catalogReader, typeFactory, conformance);
	}
}
