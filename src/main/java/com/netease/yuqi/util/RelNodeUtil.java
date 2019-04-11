package com.netease.yuqi.util;

import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

/**
 * Author yuqi
 * Time 10/4/19 17:51
 **/
public class RelNodeUtil {
    public static void main(String[] args) {
        String sql = "select id, text from user_behavior where source = 1 and action =2 and ( text like 'com.autonavi.minimap' or text like 'com.baidu.BaiduMap')";

        RelNode relNode = UserBehaviorParser.INSTANCE.getRelNode(sql);
        System.out.println(RelOptUtil.toString(relNode));


        Map<String, SqlTypeName> newTable = Maps.newHashMap();

        newTable.put("ID", SqlTypeName.INTEGER);

        UserBehaviorParser.INSTANCE.addTable("ZHANG", newTable);


        String newSql = "select id from zhang";

        System.out.println(RelOptUtil.toString(UserBehaviorParser.INSTANCE.getRelNode(newSql)));


        String anotherSql = "select a.id, a.text, b.id from user_behavior a inner join zhang b on a.id = b.id";
        System.out.println(RelOptUtil.toString(UserBehaviorParser.INSTANCE.getRelNode(anotherSql)));
    }
}
