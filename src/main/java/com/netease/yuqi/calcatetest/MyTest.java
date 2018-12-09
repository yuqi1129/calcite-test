package com.netease.yuqi.calcatetest;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/10/23 下午9:56
 */

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

public class MyTest {
	public static void main(String[] args) {
		try {
			SchemaPlus rootSchema = Frameworks.createRootSchema(true);

			rootSchema.add("TABLE_RESULT", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

					RelDataType t0 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.FLOAT), true);
					RelDataType t1 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TINYINT), true);
					RelDataType t2 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.SMALLINT), true);
					RelDataType t3 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
					RelDataType t4 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.FLOAT), true);
					RelDataType t5 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
					RelDataType t6 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
					RelDataType t7 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
					RelDataType t8 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
					RelDataType t9 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
					RelDataType t10 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
					RelDataType t11 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);


					builder.add("ID", t0);
					builder.add("byte_test".toUpperCase(), t1);
					builder.add("short_test".toUpperCase(), t2);
					builder.add("int_test".toUpperCase(), t3);
					builder.add("float_test".toUpperCase(), t4);
					builder.add("double_test".toUpperCase(), t5);
					builder.add("long_test".toUpperCase(), t6);
					builder.add("boolean_test".toUpperCase(), t7);
					builder.add("date_test".toUpperCase(), t8);
					builder.add("time_test".toUpperCase(), t9);
					builder.add("timestamp_test".toUpperCase(), t10);
					builder.add("string_test".toUpperCase(), t11);

					return builder.build();
				}
			});


			rootSchema.add("TABLE_RESULT_COPY", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

					RelDataType t0 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
					RelDataType t1 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TINYINT), true);
					RelDataType t2 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.SMALLINT), true);
					RelDataType t3 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
					RelDataType t4 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.FLOAT), true);
					RelDataType t5 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
					RelDataType t6 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
					RelDataType t7 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), true);
					RelDataType t8 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
					RelDataType t9 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
					RelDataType t10 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
					RelDataType t11 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);


					builder.add("ID", t0);
					builder.add("byte_test1".toUpperCase(), t1);
					builder.add("short_test1".toUpperCase(), t2);
					builder.add("int_test1".toUpperCase(), t3);
					builder.add("float_test1".toUpperCase(), t4);
					builder.add("double_test1".toUpperCase(), t5);
					builder.add("long_test1".toUpperCase(), t6);
					builder.add("boolean_test1".toUpperCase(), t7);
					builder.add("date_test1".toUpperCase(), t8);
					builder.add("time_test1".toUpperCase(), t9);
					builder.add("timestamp_test1".toUpperCase(), t10);
					builder.add("string_test1".toUpperCase(), t11);

					return builder.build();
				}
			});
			final FrameworkConfig config = Frameworks.newConfigBuilder()
					.parserConfig(SqlParser.Config.DEFAULT)
					.defaultSchema(rootSchema)
					.build();
			Planner planner = Frameworks.getPlanner(config);


			String sql = "select id, if(id > 0, time_test, date_test) from table_result";
			String sql1 = "select id, case when id > 0 then time_test else date_test end from table_result";
			String sql2 = "select * from table_result a natural left join table_result_copy b";

			String sql3 = "select case when 1 = 1 then sum(id) when 1 = 2 then first_value(int_test) else 0 end from table_result";



			SqlNode parse = planner.parse(sql3);
			SqlNode validate = planner.validate(parse);

			RelRoot root = planner.rel(validate);

			System.out.println(RelOptUtil.toString(root.rel));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
