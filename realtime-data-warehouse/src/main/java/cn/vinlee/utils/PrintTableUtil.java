package cn.vinlee.utils;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 打印输出工具类.
 *
 * @author Vinlee Xiao
 * @className PrintTableUtil
 * @date 2022/7/23 18:11:38
 **/
public class PrintTableUtil {

    private PrintTableUtil() {

    }

    public static void printTable(StreamTableEnvironment tEnv, Table tableName, String printName) {
        Table tmpTable = tEnv.sqlQuery("select * from " + tableName);
        tEnv.toChangelogStream(tmpTable).print(printName);
    }
}
