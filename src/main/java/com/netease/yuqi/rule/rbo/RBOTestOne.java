package com.netease.yuqi.rule.rbo;

import com.netease.yuqi.fun.FunctionTestTwo;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.ProjectMergeRule;

/**
 * Author yuqi
 * Time 13/5/19 20:19
 **/
public class RBOTestOne {

    //
    public static void main(String[] args) {
        //
        String sql = "select id, c + 1, _UTF16'中国' from (select id, id + 1 as c from test where id < 5) t";

        RelRoot root = FunctionTestTwo.sqlToRelNode(sql);

        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);

        System.out.println("---------------------Before optimization-----------------");
        System.out.println(RelOptUtil.toString(root.rel));
        System.out.println("---------------------After optimization -----------------");
        HepPlanner hepPlanner = new HepPlanner(builder.build());
        hepPlanner.setRoot(root.rel);
        RelNode finalNode = hepPlanner.findBestExp();

        System.out.println(RelOptUtil.toString(finalNode));
    }
}
