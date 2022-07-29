package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.DwdInteractionFavorAddSql;
import cn.vinlee.utils.CustomKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author Vinlee Xiao
 * @className DwdInteractionFavorAdd
 * @date 2022/7/26 21:05:37
 **/
public class DwdInteractionFavorAdd {
    /**
     * 要读取的Kafka主题.
     */
    static final String SOURCE_TOPIC = "ods_base_db";
    /**
     * 要插入的kafka主题.
     */
    static final String SINK_TOPIC = "dwd_interaction_favor_add_detail";

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

        String sourceDdl = CustomKafkaUtil.getKafkaDdl(SOURCE_TOPIC, "dwd_interaction_favor_add_consumer_group");
        String sourceSql = DwdInteractionFavorAddSql.SOURCE_TOPIC + sourceDdl;
        tEnv.executeSql(sourceSql);

        //4.读取收藏域数据
        Table favorInfoTable = tEnv.sqlQuery(DwdInteractionFavorAddSql.FAVOR_INFO_ADD);
        tEnv.createTemporaryView("favor_info", favorInfoTable);

        //5.创建 dwd_interaction_favor_add_detial
        String sinkSql = DwdInteractionFavorAddSql.SINK_TOPIC + CustomKafkaUtil.getUpsertDdl(SINK_TOPIC);
        tEnv.executeSql(sinkSql);

        //6.将数据保存到Kafka中对象的主题
        tEnv.executeSql("insert into dwd_interaction_favor_add_detail select * from favor_info").print();

        env.execute();
    }
}
