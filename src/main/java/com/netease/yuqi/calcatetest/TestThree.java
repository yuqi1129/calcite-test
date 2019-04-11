package com.netease.yuqi.calcatetest;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/6/1 下午2:25
 */

import com.netease.yuqi.aux.RelVisitTestOne;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;

public class TestThree {

	public static void main(String[] args) {
		try {
			SchemaPlus rootSchema = Frameworks.createRootSchema(true);
			rootSchema.add("USERS", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

					RelDataType t1 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
					RelDataType t2 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.CHAR), true);
					RelDataType t3 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);

					builder.add("ID", t1);
					builder.add("NAME", t2);
					builder.add("TIME_D", t3);

//					builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.INTEGER));
//					builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.CHAR));
//
//					builder.add("TIME_D", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.DATE));
					return builder.build();
				}
			});

			rootSchema.add("SCORE", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

					RelDataType t1 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
					RelDataType t2 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);

					builder.add("ID", t1);
					builder.add("SCORE", t2);



//					builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.INTEGER));
//					builder.add("SCORE", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.INTEGER));
					return builder.build();
				}
			});


			rootSchema.add("TABLE_RESULT", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

					RelDataType t1 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
					RelDataType t2 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.CHAR), true);
					RelDataType t3 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);

					builder.add("ID", t1);
					builder.add("NAME", t2);
					builder.add("SCORE", t3);


//					builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.INTEGER));
//					builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.CHAR));
//					builder.add("SCORE", new BasicSqlType(new RelDataTypeSystemImpl() {
//					}, SqlTypeName.INTEGER));
					return builder.build();
				}
			});


			rootSchema.add("USER_BEHAVIOR", new AbstractTable() {
				@Override
				public RelDataType getRowType(RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

					RelDataType id = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
					RelDataType idType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
					RelDataType timestamp = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);


					RelDataType source = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
					RelDataType action = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
					RelDataType text = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

					RelDataType keyWord = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
					RelDataType tag = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
					RelDataType category = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);

					RelDataType extra_attribute = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
					RelDataType date = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);

					builder.add("ID", id);
					builder.add("ID_TYPE", idType);
					builder.add("TIMESTAMP", timestamp);


					builder.add("SOURCE", source);
					builder.add("ACTION", action);
					builder.add("TEXT", text);

					builder.add("KEYWORD", keyWord);
					builder.add("TAG", tag);
					builder.add("CATEGORY", category);

					builder.add("EXTRA_ATTRIBUTE", extra_attribute);
					builder.add("DATE", date);


					return builder.build();
				}
			});

			final FrameworkConfig config = Frameworks.newConfigBuilder()
					.parserConfig(SqlParser.Config.DEFAULT)
					.defaultSchema(rootSchema)
					.build();
			Planner planner = Frameworks.getPlanner(config);

			/**
			//dataContext = new MyDataContext(planner);

//			SqlNode parse =
//					planner.parse("insert into users select y, x\n"
//							+ "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
//							+ "where x > 1");
//			SqlNode validate = planner.validate(parse);
			*/


			//SqlNode parse1 = planner.parse("insert into table_result select a.id as id, a.name as name, b.score as score from users a inner join score b on a.id = b.id where a.id * 2 = 5".toUpperCase());
			//SqlNode parse1 = planner.parse("insert into table_result(id, name, score) select a.id as id, a.name as name, 1 from users a where month(a.time_d - interval '30' day) >= 2");
			//SqlNode parse1 = planner.parse("insert into table_result select * from (select a.id as id, a.name as name, a.id * id from users a where a.id > 5) where id < 100");

			//SqlNode parse1 = planner.parse("insert into table_result select * from (select a.id as id, a.name as name, case when(row_number() over (partition by id order by name) > 2) then 1 else 0 end as score from users a) where id < 1000");

			//SqlNode parse1 = planner.parse("select 'good' like '%1%' || true");
			//SqlNode parse1 = planner.parse("insert into table_result select * from (select a.id as id, a.name as name, b.score from users a left join score b on a.id = b.id where b.score is not null)");
			String sql = "select time_d from users where id > 100 or id > 400 and id in (select id from users where id < 500)";

			String sql1 = "select id, case when sum(score) / count(distinct name) is not null then sum(score) / count(distinct name) else 0 end tmp from table_result group by id";

			//另外一个case when 不支持 when 后面不支持负号
			String sql2 = "select id, score between - score * 5 and 100 from table_result";

			String sql3 = "select id, score from table_result where id > 0.06 - 0.01 and id < 0.06 + 0.01";

			String sql4 = "select id, case when sum(score) is not null then 1 else 0 end tmp from table_result group by id";
			//SqlNode parse1 = planner.parse("insert into table_result select id+1, name, score from (select a.id + 1 as id, a.name as name, b.score from users a left join score b on a.id = b.id where a.time_d between '2008-09-12' and cast('2015-09-21' as date) + interval '60' second)");


			//select id,text,top-100 from table A where source=app AND action=usage AND (text contains "com.autonavi.minimap" OR text contains "com.baidu.BaiduMap")


			String sql5 = "select id, text from user_behavior where source = 1 and action =2 and ( text like 'com.autonavi.minimap' or text like 'com.baidu.BaiduMap')";

			SqlNode parse1 = planner.parse(sql5);

			//SqlNode parse1 = planner.parse("insert into table_result select id, name , max(id) from users");
			SqlNode validate = planner.validate(parse1);

			RelRoot root = planner.rel(validate);

			System.out.println("Before------------------>");
			System.out.print(RelOptUtil.toString(root.rel));


			/**
			RexBuilder builder1 = root.rel.getCluster().getRexBuilder();
			LogicalFilter filter = (LogicalFilter) root.rel.getInput(0).getInput(0);
			RexCall call = (RexCall) ((RexCall) ((RexCall) filter.getCondition()).operands.get(0)).getOperands().get(1);

			builder1.makeCall(call.getOperator(), call.getOperands());
			*/


			HepProgramBuilder builder = new HepProgramBuilder();
			//builder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
			//builder.addRuleInstance(FilterJoinRule.JOIN);
			//builder.addRuleCollection(Programs.CALC_RULES);
			//builder.addRuleCollection(Programs.RULE_SET);
			builder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
			builder.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
			//builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
			HepPlanner hepPlanner = new HepPlanner(builder.build());
			root.rel.getCluster().getPlanner().setExecutor(RexUtil.EXECUTOR);
			hepPlanner.setRoot(root.rel);

			RelOptUtil.findTables(root.rel).forEach(a -> System.out.println(a.getQualifiedName()));;
			System.out.println(RelOptUtil.getVariablesSet(root.rel));

			/****************************************************************/
			RelNode node1 = root.rel.accept(new RelShuttleImpl());
			/****************************************************************/


			RelVisitTestOne testOne = new RelVisitTestOne();
			testOne.go(root.rel);





			RelNode node = hepPlanner.findBestExp();
			System.out.println(RelOptUtil.toString(node));

			//now we will try to use

			//VolcanoPlanner volcanoPlanner = new VolcanoPlanner();


//			VolcanoPlanner volcanoPlanner = (VolcanoPlanner) root.rel.getCluster().getPlanner();
//			volcanoPlanner.setRoot(root.rel);
//
//			RelNode finalNode = volcanoPlanner.findBestExp();
//
//			System.out.println(RelOptUtil.toString(finalNode, SqlExplainLevel.ALL_ATTRIBUTES));
//
//			System.out.println("After-------------------->");
//			System.out.print(RelOptUtil.toString(node));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
