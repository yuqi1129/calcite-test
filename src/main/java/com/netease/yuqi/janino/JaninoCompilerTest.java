package com.netease.yuqi.janino;


import org.apache.calcite.util.javac.JaninoCompiler;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

/**
 * Author yuqi
 * Time 19/5/19 20:57
 **/
public class JaninoCompilerTest {
    public static void main(String[] args) {
        try {
            File file = new File("/Users/yuqi/project/calcite-test/src/main/java/com/netease/yuqi/janino/TestClass.java");
            InputStream inputStream = new FileInputStream(file);
            List<String> lines = IOUtils.readLines(inputStream);
            StringBuilder builder = new StringBuilder();
            lines.remove(0);
            lines.forEach(a -> builder.append(a).append("\n"));
            String classContent = builder.toString();

            JaninoCompiler janinoCompiler = new JaninoCompiler();
            janinoCompiler.getArgs().setFullClassName("TestClass");
            janinoCompiler.getArgs().setSource(classContent, "TestClass.java");
            janinoCompiler.getArgs().setDestdir("/Users/yuqi/project/calcite-test/src/main/java/com/netease/yuqi/janino");
            janinoCompiler.compile();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
