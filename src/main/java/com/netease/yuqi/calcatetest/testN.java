package com.netease.yuqi.calcatetest;/*
 * Author: park.yq@alibaba-inc.com
 * Date: 2019/1/15 下午1:28
 */

import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.Properties;

import static java.lang.System.exit;
import static org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION;
import static org.apache.calcite.sql.type.InferTypes.VARCHAR_1024;
import static org.apache.calcite.sql.type.SqlTypeTransforms.FORCE_NULLABLE;

public class testN {
	public static class TestSchema {
		public final Triple[] rdf = {new Triple("s", "p", "o")};
	}


	public static class FeatureTableSchema {
		public final FeatureTable[] DATA = {new FeatureTable()};
	}

	public static class FeatureTable {
		public String TRACE_ID = "";
		public String FEATURE = "";
	}

	public static class LabelTableSchema {
		public final LabelTable[] DATA = {new LabelTable()};
	}

	public static class LabelTable {
		public String TRACE_ID = "";
		public String LABEL = "";
	}

	public static void main(String[] args) {
		String sql = "select a.\"s\", count(a.\"s\") from T.\"rdf\" a group by a.\"s\"";
		//explain(sql);


		sql = "select my_function(feature_table.trace_id), train(feature_table.feature,label_table.label) " + "\n" +
				"from feature_table_data.data feature_table join label_table_data.data label_table " + "\n" +
				"on feature_table.trace_id = label_table.trace_id " + "\n" +
				"and qps_limit(feature_table.trace_id) <> '0'";
		explain(sql);
        /* fail
        sql = "set a=1";
        explain(sql);
        */
	}



	public static class QpsLimit {
		public static String qps_limit(String a) {
			System.out.println(a);
			return a;
		}
	}

	public static class Train {
		public static String train(String s1, String s2) {
			return s1 + s2;
		}
	}


	public static void explain(String sql) {
		System.out.println("sql: \n" + sql);

		CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);


		SchemaPlus schemaPlus = rootSchema.plus();

		//SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

		// add table
		schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
		schemaPlus.add("FEATURE_TABLE_DATA", new ReflectiveSchema(new FeatureTableSchema()));
		schemaPlus.add("LABEL_TABLE_DATA", new ReflectiveSchema(new LabelTableSchema()));

		// add udf
		schemaPlus.add("QPS_LIMIT", ScalarFunctionImpl.create(QpsLimit.class, "qps_limit"));
		schemaPlus.add("TRAIN", ScalarFunctionImpl.create(Train.class, "train"));


		//通过schemaPlus 增加的函数、udf、表一定要通过CalciteCataLogReader读取，否则系统获取udf等
		//reflective schema-->table ReflectiveTable
		//即将udf等放入Schema中，然后将scheam以参数传入CalciteCatalogReader
		//如果不想通过这种方式可以直接将Function 加入ListOpeartorTable, 可以采用如下的方式

		CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(rootSchema, Lists.newArrayList(),
				new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT), new CalciteConnectionConfigImpl(new Properties()));

		ListSqlOperatorTable listSqlOperatorTable = new ListSqlOperatorTable();
		listSqlOperatorTable.add(new SqlFunction(
				"my_function".toUpperCase(),
				SqlKind.OTHER_FUNCTION,
				ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARCHAR), FORCE_NULLABLE),//return type is varchar with length of 1024
				VARCHAR_1024, //parameter varchar with length of 1024
				OperandTypes.family(SqlTypeFamily.CHARACTER),
				SqlFunctionCategory.STRING
				)
		);

		FrameworkConfig frameworkConfig =
				Frameworks.newConfigBuilder()
						.defaultSchema(schemaPlus)
						.operatorTable(ChainedSqlOperatorTable.of(SqlStdOperatorTable.instance(),
								calciteCatalogReader, listSqlOperatorTable))
						.build();

		SqlParser.ConfigBuilder parserConfig =
				SqlParser.configBuilder(frameworkConfig.getParserConfig());

		//SQL 大小写不敏感
		parserConfig.setCaseSensitive(false).setConfig(parserConfig.build());

		Planner planner = Frameworks.getPlanner(frameworkConfig);

		SqlNode sqlNode;
		RelRoot relRoot = null;
		try {
			//parser阶段
			sqlNode = planner.parse(sql);
			//validate阶段
			planner.validate(sqlNode);
			//获取RelNode树的根
			relRoot = planner.rel(sqlNode);
		} catch (Exception e) {
			e.printStackTrace();
			exit(-1);
		}

		RelNode relNode = relRoot.project();

		System.out.println("relNode: \n" + RelOptUtil.toString(relNode));
	}
}


