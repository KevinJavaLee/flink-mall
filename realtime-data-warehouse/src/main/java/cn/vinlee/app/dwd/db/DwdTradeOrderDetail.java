package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.TradeOrderDetailSql;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.MySqlUtil;
import cn.vinlee.utils.PrintTableUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

/**
 * 订单有关的明细数据.
 *
 * @author Vinlee Xiao
 * @className DwdTradeOrderDetail
 * @date 2022/7/23 17:37:31
 **/
public class DwdTradeOrderDetail {
    static final String DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    static final Logger LOGGER = LoggerFactory.getLogger(DwdTradeOrderDetail.class);

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
        env.getCheckpointConfig().setCheckpointTimeout(60 * 2000);
        //重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));

        //4.使用DDL方法读取Kafka ods_base_log中的数据
        String sourceSql = CustomKafkaUtil.getTopicDatabaseDdl("dwd_trade_order_detail_consumer_group");
        tEnv.executeSql(sourceSql);

        //5.过滤出订单明细数据
        Table orderDetailTable = tEnv.sqlQuery(TradeOrderDetailSql.ORDER_DETAIL);
        tEnv.createTemporaryView("order_detail", orderDetailTable);
        //5.1测试数据
//        PrintTableUtil.printTable(tEnv, orderDetailTable,"订单明细数据");

        //6.过滤出订单数据
        Table orderInfoTable = tEnv.sqlQuery(TradeOrderDetailSql.ORDER_INFO);
        tEnv.createTemporaryView("order_info", orderInfoTable);
        //6.2测试数据
//        PrintTableUtil.printTable(tEnv, orderInfoTable,"订单数据);

        //7.过滤出订单明细活动数据
        Table orderActivityTable = tEnv.sqlQuery(TradeOrderDetailSql.ORDER_DETAIL_ACTIVITY);
        tEnv.createTemporaryView("order_activity", orderActivityTable);
//        PrintTableUtil.printTable(tEnv, orderActivityTable, "订单明细活动数据");

        //8.订单明细购物券数据
        Table orderCouponTable = tEnv.sqlQuery(TradeOrderDetailSql.ORDER_DETAIL_COUPON);
        tEnv.createTemporaryView("order_coupon", orderCouponTable);
//        PrintTableUtil.printTable(tEnv, orderCouponTable, "订单明细购物券数据");

        //9.构建MySQL lookup表 
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        tEnv.executeSql(baseDicLookUpDdl);

        //10.关联五张无张表
        Table resultTable = tEnv.sqlQuery(TradeOrderDetailSql.ORDER_DETAIL_JOIN);
        tEnv.createTemporaryView("result_table", resultTable);
        PrintTableUtil.printTable(tEnv, resultTable, "连接表");

        //11.将连接表产生的数据存储到Kafka中
        String sinkSql = TradeOrderDetailSql.SINK_TOPIC_SQL + CustomKafkaUtil.getUpsertDdl(DWD_TRADE_ORDER_DETAIL);
        tEnv.executeSql(sinkSql);
//
        TableResult tableResult =
                tEnv.executeSql("insert into dwd_trade_order_detail_table select * from result_table " + resultTable);
        tableResult.print();

        env.execute();

    }
}
