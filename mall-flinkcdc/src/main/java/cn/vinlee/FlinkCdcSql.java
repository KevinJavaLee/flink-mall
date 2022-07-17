package cn.vinlee;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 通过FlinkCDC sql采集MySQL业务数据.
 *
 * @author Vinlee Xiao
 * @className FlinkCDC
 * @description 通过FlinkCDC sql采集MySQL业务数据
 * @date 2022/7/10 13:25:44
 **/
public class FlinkCdcSql {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建一张表映射到FlinkCDC
        tableEnv.executeSql("CREATE TABLE mysql_binlog (\n" +
                " id INT NOT NULL,\n" +
                " tm_name STRING,\n" +
                " logo_url STRING,\n" +
                " PRIMARY KEY (`id`) NOT ENFORCED \n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'hadoop2',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'Kevin1401597760',\n" +
                " 'database-name' = 'gmall',\n" +
                " 'table-name' = 'base_trademark'\n" +
                ")");

        tableEnv.executeSql("select * from mysql_binlog").print();
    }
}
