package com.netease.yuqi.util;

import org.apache.calcite.sql.SqlNode;

/**
 * Author yuqi
 * Time 11/4/19 10:06
 **/
public class TestTimeConsumption {
    public static void main(String[] args) {

        String sql = "select id, text from user_behavior where source = 1 and action =2 and ( text like 'com.autonavi.minimap' or text like 'com.baidu.BaiduMap')";

        int i = 1;
        int times = 1;
        while (i < 8) {
            System.out.println("execute " + times + " take " + UserBehaviorParser.INSTANCE.getTimeOfParser(sql, times) + " millseconds");
            i++;
            times *= 10;
        }

    }
}
