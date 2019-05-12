package com.netease.yuqi.fun;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.Properties;

import static com.netease.yuqi.fun.FunctionTestOne.TYPE_FACTORY;
import static com.netease.yuqi.fun.FunctionTestOne.TYPE_SYSTEM;
import static org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES;

/**
 * Author yuqi
 * Time 12/5/19 10:30
 **/
public class FunctionTestTwo {
    private static final CalciteSchema ROOT_SCHEMA = CalciteSchema
            .createRootSchema(false, false);

    private static final SqlParser.ConfigBuilder builder = SqlParser.configBuilder();
    static {
        //以下需要设置成大写并且忽略大小写
        builder.setLex(Lex.MYSQL);
        builder.setQuotedCasing(Casing.TO_UPPER);
        builder.setUnquotedCasing(Casing.TO_UPPER);
        builder.setCaseSensitive(false);

        ROOT_SCHEMA.add("test", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory
                        .Builder(TYPE_FACTORY);
                //列id, 类型int
                builder.add("id", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER));
                //列name, 类型为varchar
                builder.add("name", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
                builder.add("time_str", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
                return builder.build();
            }
        });
    }

    private static final FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(ROOT_SCHEMA.plus())
            .parserConfig(builder.build())
            //这里没有将修改Parser.jj方式放入StdOperatorTable中
            .operatorTable(SqlStdOperatorTable.instance())
            .build();

    public static void main(String[] args) {
        String sql1 = "select func1(name) from test where id > 4";
        System.out.println("-------- func1 test -------");
        RelRoot relRoot1 = sqlToRelNode(sql1);
        System.out.println(RelOptUtil.toString(relRoot1.rel, ALL_ATTRIBUTES));

        String sql2 = "select func2(id) from test where id > 4";
        System.out.println("-------- func2 test -------");
        RelRoot relRoot2 = sqlToRelNode(sql2);
        System.out.println(RelOptUtil.toString(relRoot2.rel, ALL_ATTRIBUTES));
    }


    public static RelRoot sqlToRelNode(String sql) {

        try {
            SchemaPlus plus = ROOT_SCHEMA.plus();
            plus.add("FUNC1", ScalarFunctionImpl.create(
                    FunctionUtil.class, "func1"));

            plus.add("FUNC2", ScalarFunctionImpl.create(
                    FunctionUtil.class, "func2"));


            SqlParser parser = SqlParser.create(sql, config.getParserConfig());
            SqlNode sqlNode = parser.parseStmt();

            //这里需要注意大小写问题，否则表会无法找到
            Properties properties = new Properties();
            properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                    String.valueOf(config.getParserConfig().caseSensitive()));

            CalciteCatalogReader calciteCatalogReader =  new CalciteCatalogReader(
                    CalciteSchema.from(rootSchema(plus)),
                    CalciteSchema.from(config.getDefaultSchema()).path(null),
                    TYPE_FACTORY,
                    new CalciteConnectionConfigImpl(properties));

            //to supported user' define function
            SqlOperatorTable sqlOperatorTable = ChainedSqlOperatorTable
                    .of(config.getOperatorTable(), calciteCatalogReader);

            TestSqlValidatorImpl validator = new TestSqlValidatorImpl(
                    sqlOperatorTable,
                    calciteCatalogReader,
                    TYPE_FACTORY,
                    SqlConformanceEnum.DEFAULT);

            //try to union trait set
            //addRelTraitDef for is HepPlanner has not effect in fact
            VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
            SqlNode validateSqlNode = validator.validate(sqlNode);
            final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
            RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, rexBuilder);

            final SqlToRelConverter.Config sqlToRelConverterConfig
                    = SqlToRelConverter.configBuilder()
                    .withConfig(config.getSqlToRelConverterConfig())
                    .withTrimUnusedFields(false)
                    .withConvertTableAccess(false)
                    .build();

            final SqlToRelConverter sqlToRelConverter =
                    new SqlToRelConverter(null, validator,
                            calciteCatalogReader, cluster, config.getConvertletTable(),
                            sqlToRelConverterConfig);

            RelRoot root =
                    sqlToRelConverter.convertQuery(validateSqlNode, false, true);
            root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
            final RelBuilder relBuilder = sqlToRelConverterConfig
                    .getRelBuilderFactory().create(cluster, null);

            //change trait set of TableScan
            return root.withRel(
                    RelDecorrelator.decorrelateQuery(root.rel, relBuilder));

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (;;) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }

    public static class TestSqlValidatorImpl  extends SqlValidatorImpl {
        public TestSqlValidatorImpl(SqlOperatorTable opTab,
                                    SqlValidatorCatalogReader catalogReader,
                                    RelDataTypeFactory typeFactory,
                                    SqlConformance conformance) {
            super(opTab, catalogReader, typeFactory, conformance);
        }
    }
}
