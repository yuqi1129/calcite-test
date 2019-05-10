package com.netease.yuqi.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.parser.impl.YuqiSqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Assert;
import org.junit.Test;

/**
 * Author yuqi
 * Time 10/5/19 16:15
 **/
public class ParserTest {

    @Test(expected = RuntimeException.class)
    public void testSubmitWrong() {
        String sql = "submit job as 'select * from test'";
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.TO_UPPER)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        .build())
                .build();
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());

        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println(sqlNode.toString());
            Assert.fail();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testSubmitWrongOK() {
        String sql = "submit job as 'select * from test'";

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setParserFactory(YuqiSqlParserImpl.FACTORY)
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.TO_UPPER)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        .build())
                .build();
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());

        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println(sqlNode.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
