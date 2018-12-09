package com.netease.yuqi.calcatetest;
/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2018/9/16 下午5:32
 */

import com.netease.yuqi.aux.rel.DogRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;


public class TestFour {
	public static void main(String[] args) {
		try {
			SchemaPlus rootSchema = Frameworks.createRootSchema(true);
			rootSchema.add("USERS", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
					builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.INTEGER));
					builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.CHAR));

					builder.add("TIME_D", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.DATE));
					return builder.build();
				}
			});

			rootSchema.add("SCORE", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
					builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.INTEGER));
					builder.add("SCORE", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.INTEGER));
					return builder.build();
				}
			});


			rootSchema.add("TABLE_RESULT", new AbstractTable() {
				public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
					RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
					builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.INTEGER));
					builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.CHAR));
					builder.add("SCORE", new BasicSqlType(new RelDataTypeSystemImpl() {
					}, SqlTypeName.INTEGER));
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
			//SqlNode parse1 = planner.parse("insert into table_result select id+1, name, score from (select a.id + 1 as id, a.name as name, b.score from users a left join score b on a.id = b.id where a.time_d between '2008-09-12' and cast('2015-09-21' as date) + interval '60' second)");

			SqlNode parse1 = planner.parse("select a.id, b.id from (select null as id from users) a left join (select null as id from score) b on a.id = b.id");
			//SqlNode parse1 = planner.parse("insert into table_result select id, name , max(id) from users");


			SqlNode parse2 = planner.parse("with T as select * from users; select * from T");
			SqlNode validate = planner.validate(parse2);

			RelRoot root = planner.rel(validate);

			System.out.println("Before------------------>");
			System.out.print(RelOptUtil.toString(root.rel));


			/**
			 RexBuilder builder1 = root.rel.getCluster().getRexBuilder();
			 LogicalFilter filter = (LogicalFilter) root.rel.getInput(0).getInput(0);
			 RexCall call = (RexCall) ((RexCall) ((RexCall) filter.getCondition()).operands.get(0)).getOperands().get(1);

			 builder1.makeCall(call.getOperator(), call.getOperands());
			 */


			//HepProgramBuilder builder = new HepProgramBuilder();
			//builder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
			//builder.addRuleInstance(FilterJoinRule.JOIN);
			//builder.addRuleCollection(Programs.CALC_RULES);
			//builder.addRuleCollection(Programs.RULE_SET);
			//builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
			//HepPlanner hepPlanner = new HepPlanner(builder.build());
			//hepPlanner.setRoot(root.rel);

			//RelOptUtil.findTables(root.rel).forEach(a -> System.out.println(a.getQualifiedName()));;
			//System.out.println(RelOptUtil.getVariablesSet(root.rel));

			/****************************************************************/
			//RelNode node1 = root.rel.accept(new RelShuttleImpl());
			/****************************************************************/


			//RelVisitTestOne testOne = new RelVisitTestOne();
			//testOne.go(root.rel);





			//RelNode node = hepPlanner.findBestExp();


			//now we will try to use

			VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
			//volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
			//VolcanoPlanner volcanoPlanner = (VolcanoPlanner) root.rel.getCluster().getPlanner();
			//volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
			//volcanoPlanner.setRoot(root.rel);

			//VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
			RelOptCluster relOptCluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)));

			relOptCluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
			RelTraitSet desiredTraits = relOptCluster.traitSetOf(DogRel.CONVENTION);


			RelNode relNode = root.rel;
			if (!relNode.getTraitSet().equals(desiredTraits)) {
				relNode = volcanoPlanner.changeTraits(relNode, desiredTraits);
			}

			//RelTraitSet relTraits = relNode.getTraitSet().replace(desiredTraits).simplify();

			volcanoPlanner.setRoot(relNode);

			RelNode finalNode = volcanoPlanner.findBestExp();


			System.out.println(RelOptUtil.toString(finalNode, SqlExplainLevel.ALL_ATTRIBUTES));

			System.out.println("After-------------------->");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
