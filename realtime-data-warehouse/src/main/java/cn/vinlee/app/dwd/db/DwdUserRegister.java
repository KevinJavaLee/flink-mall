package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.DwdUserRegisterSql;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.PrintTableUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 用户注册明细表.
 *
 * @author Vinlee Xiao
 * @className DwdUserRegister
 * @date 2022/7/26 21:36:54
 **/
public class DwdUserRegister {
    /**
     * 要读取的Kafka主题.
     */
    static final String SOURCE_TOPIC = "ods_base_db";
    /**
     * 要插入的kafka主题.
     */
    static final String SINK_TOPIC = "dwd_user_register_detail";

    public static void main(String[] args) throws Exception {
        //1.设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //2.设定时区的事件为GMT+8
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        //3.设置checkpoint保存地方
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 2000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/gmall/dw");
        //重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));

        String sourceDdl = CustomKafkaUtil.getKafkaDdl(SOURCE_TOPIC, "dwd_user_register_consumer_group");
        String sourceSql = DwdUserRegisterSql.SOURCE_TOPIC + sourceDdl;
        tEnv.executeSql(sourceSql);

        //4.读取用户注册信息表
        Table userInfoTable = tEnv.sqlQuery(DwdUserRegisterSql.USER_INFO);
        tEnv.createTemporaryView("user_info", userInfoTable);
        PrintTableUtil.printTable(tEnv, userInfoTable, "用户信息表");

        //5.建立Upsert-Kafka表保存数据到对应的Kafka主题中去
        String sinkSql = DwdUserRegisterSql.SINK_TOPIC + CustomKafkaUtil.getUpsertDdl(SINK_TOPIC);
        tEnv.executeSql(sinkSql);

        //6.将数据保存到对应的主题中去
        tEnv.executeSql("insert into dwd_user_register_detail select user_id,date_format(create_time,'yyyy-MM-dd') " +
                " date_id , create_time,ts " +
                " from " +
                "user_info").print();
        env.execute();
    }
}
