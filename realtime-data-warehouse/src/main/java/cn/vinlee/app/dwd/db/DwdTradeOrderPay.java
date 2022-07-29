package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.DwdTradeOrderPaySql;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.MySqlUtil;
import cn.vinlee.utils.PrintTableUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 交易域成功支付事实表.
 *
 * @author Vinlee Xiao
 * @className DwdTradeOrderPay
 * @date 2022/7/25 20:56:38
 **/
public class DwdTradeOrderPay {
    static final String SOURCE_TOPIC = "dwd_trade_order_detail";
    static final String SINK_TOPIC = "dwd_trade_order_pay_detail";

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

        String sourceDdl = CustomKafkaUtil.getKafkaDdl(SOURCE_TOPIC, "dwd_trade_order_pay_consumer_group");
        String sourceSql = DwdTradeOrderPaySql.DWD_TRADE_ORDER_DETAIL + sourceDdl;

        tEnv.executeSql(sourceSql);

        //4.读取支付数据 1.状态为修改 2.支付状态为1602 3.且old数据中支付状态数据不为空
        String sourceTopicDb = CustomKafkaUtil.getTopicDatabaseDdl("dwd_trade_order_pay_consumer_group");
        tEnv.executeSql(sourceTopicDb);

        //5.过滤出支付信息
        Table paymentInfoTable = tEnv.sqlQuery(DwdTradeOrderPaySql.PAY_INFO);
        tEnv.createTemporaryView("payment_info", paymentInfoTable);
        PrintTableUtil.printTable(tEnv, paymentInfoTable, "支付信息");

        //6.读取MySQL base_dic表构建维表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        tEnv.executeSql(baseDicLookUpDdl);

        //7.将三个表连接，得到支付宽表信息
        Table joinTable = tEnv.sqlQuery(DwdTradeOrderPaySql.JOIN_PAY_INFO);
        tEnv.createTemporaryView("result_table", joinTable);

        //7.将数据保存到Kafka dwd_trade_pay_detail主题中
        String sinkSql = DwdTradeOrderPaySql.SINK_TOPIC_DDL + CustomKafkaUtil.getKafkaDdl(SINK_TOPIC,
                "dwd_trade_order_pay_producer_group");
        tEnv.executeSql(sinkSql);

        //8.将关联结果插入到Kafka中
        tEnv.executeSql("insert into dwd_trade_order_pay_detail select * from " + joinTable);

        env.execute();
    }
}
