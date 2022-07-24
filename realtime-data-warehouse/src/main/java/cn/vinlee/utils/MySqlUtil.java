package cn.vinlee.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MySQL配置类
 *
 * @author Vinlee Xiao
 * @className MySqlUtil
 * @date 2022/7/23 14:01:39
 **/
public class MySqlUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlUtil.class);

    private MySqlUtil() {
        
    }

    /**
     * @param tableName 表明.
     * @return 返回Flink Sql配置语句.
     */
    public static String mySqlLookUpTableDdl(String tableName) {
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://hadoop2:3306/gmall', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '100', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = 'Kevin1401597760', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }


    public static String getBaseDicLookUpDdl() {
        String result = "create table `base_dic`( " +
                "    `dic_code` string, " +
                "    `dic_name` string, " +
                "    `parent_code` string, " +
                "    `create_time` timestamp, " +
                "    `operate_time` timestamp, " +
                "    primary key(`dic_code`) not enforced " +
                ")" + MySqlUtil.mySqlLookUpTableDdl("base_dic");

        LOGGER.info("MySqlUtil: [ {} ]", result);
        return result;
    }
}
