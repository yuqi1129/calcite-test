package com.netease.yuqi.util;

import org.apache.calcite.rel.RelNode;
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
import org.apache.commons.lang.time.StopWatch;

import java.util.Map;

/**
 * Author yuqi
 * Time 10/4/19 17:26
 **/
public class UserBehaviorParser {
    private SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    private final FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema)
            .build();

    public static final UserBehaviorParser INSTANCE = new UserBehaviorParser().init();

    //default add
    private void addTable() {
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

                RelDataType extra_attribute_key = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ARRAY), true);

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
                builder.add("EXTRA_ATTRIBUTE_KEY", extra_attribute_key);


                return builder.build();
            }
        });
    }

    /**
     *
     * @param tableName
     */
    public void addTable(String tableName, Map<String, SqlTypeName> columenNameAndType) {
        AbstractTable abstractTable = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

                for (Map.Entry<String, SqlTypeName> entry : columenNameAndType.entrySet()) {
                    RelDataType type = typeFactory.createTypeWithNullability(typeFactory.createSqlType(entry.getValue()), true);
                    builder.add(entry.getKey(), type);
                }

                return builder.build();
            }
        };

        rootSchema.add(tableName, abstractTable);
    }

    private UserBehaviorParser init() {
        addTable();
        return this;
    }

    public RelNode getRelNode(String sql) {
        try {
            Planner planner = Frameworks.getPlanner(config);
            SqlNode node = planner.parse(sql);
            SqlNode validateNode = planner.validate(node);
            RelRoot root = planner.rel(validateNode);
            return root.rel;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public long getTimeOfParser(String sql, int times) {

        try {

            Planner planner = Frameworks.getPlanner(config);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            for (int i = 0; i < times; i++) {
                planner.parse(sql);
            }

            return stopWatch.getTime();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
