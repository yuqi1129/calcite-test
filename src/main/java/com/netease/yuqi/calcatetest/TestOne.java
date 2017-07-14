package com.netease.yuqi.calcatetest;

import com.sun.org.apache.xerces.internal.xinclude.XPointerFramework;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

/**
 * Created with calcite-test.
 * User: hzyuqi1
 * Date: 2017/7/14
 * Time: 15:18
 * To change this template use File | Settings | File Templates.
 */

public class TestOne {

    public static class TestSchema {
        public final Triple[] rdf = {new Triple("s", "p", "o")};
    }

    public static void main(String[] args) {
        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

        schemaPlus.add("s", new ReflectiveSchema(new TestSchema()));
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        configBuilder.defaultSchema(schemaPlus);

        FrameworkConfig frameworkConfig = configBuilder.build();

        SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());

        paresrConfig.setCaseSensitive(false).setConfig(paresrConfig.build());

        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = null;
        RelRoot relRoot = null;
        try {
            sqlNode = planner.parse("select * from \"s\".\"rdf\" \"a\", \"s\".\"rdf\" \"b\"" +
                    "where \"a\".\"s\" = 5 and \"b\".\"s\" = 5 limit 5, 1000");
            planner.validate(sqlNode);
            relRoot = planner.rel(sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
        }

        RelNode relNode = relRoot.project();
        System.out.print(RelOptUtil.toString(relNode));
    }
}
