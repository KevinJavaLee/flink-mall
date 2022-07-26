package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.DwdTradeOrderAddSql;
import cn.vinlee.utils.CustomKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 订单加购明细表.
 *
 * @author Vinlee Xiao
 * @className DwdTradeOrderAdd
 * @date 2022/7/25 19:40:11
 **/
public class DwdTradeOrderAdd {
    /**
     * 要读取的Kafka主题.
     */
    static final String SOURCE_TOPIC = "dwd_trade_order_detail";
    /**
     * 要插入的kafka主题.
     */
    static final String SINK_TOPIC = "dwd_trade_order_add_detail";

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

        String sourceDdl = CustomKafkaUtil.getKafkaDdl(SOURCE_TOPIC, "dwd_trade_order_add_consumer_group");
        String sourceSql = DwdTradeOrderAddSql.DWD_TRADE_ORDER_DETAIL + sourceDdl;

        tEnv.executeSql(sourceSql);

        //4.过滤下单数据
        Table filterTable = tEnv.sqlQuery("select * from dwd_trade_order_detail_table where `type`='insert'");
        tEnv.createTemporaryView("filter_table", filterTable);

        //5.定义插入Kafka数据ddl语句
        String sinkSql = DwdTradeOrderAddSql.SINK_TOPIC_DDL + CustomKafkaUtil.getKafkaDdl(SINK_TOPIC,
                "dwd_trade_order_add_producer_group");
        tEnv.executeSql(sinkSql);

        tEnv.executeSql("insert into dwd_trade_order_add_table select * from " + filterTable).print();

        env.execute();
    }
}
