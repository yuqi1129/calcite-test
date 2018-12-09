package com.netease.yuqi.calcatetest;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/5/31 下午8:34
 */

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

public class TestTwo {

	public static void main(String[] args) {

		try {
			SchemaPlus rootSchema = Frameworks.createRootSchema(true);
			rootSchema.add("USERS", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
					builder.add("age", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.CHAR));
					return builder.build();
				}
			});

			final FrameworkConfig config = Frameworks.newConfigBuilder()
					.parserConfig(SqlParser.Config.DEFAULT)
					.defaultSchema(rootSchema)
					.build();
			Planner planner = Frameworks.getPlanner(config);
			//dataContext = new MyDataContext(planner);

			SqlNode parse =
					planner.parse("insert into users select y, x\n"
							+ "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
							+ "where x > 1");

			SqlNode validate = planner.validate(parse);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
