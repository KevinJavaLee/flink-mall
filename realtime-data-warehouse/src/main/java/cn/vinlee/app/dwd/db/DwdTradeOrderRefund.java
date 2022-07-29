package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.DwdTradeOrderRefundSql;
import cn.vinlee.bean.OrderInfoRefundBean;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.MySqlUtil;
import cn.vinlee.utils.PrintTableUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;
import java.util.Set;

/**
 * 退单订单数据表.
 *
 * @author Vinlee Xiao
 * @className DwdTradeOrderRefund
 * @date 2022/7/25 21:52:37
 **/
public class DwdTradeOrderRefund {
    static final String SOURCE_TOPIC = "ods_base_db";
    /**
     * 要插入的kafka主题.
     */
    static final String SINK_TOPIC = "dwd_trade_order_refund_detail";

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

        //4.得到Kafka ods_base_db数据.
        String sourceDdl = CustomKafkaUtil.getKafkaDdl(SOURCE_TOPIC, "dwd_trade_order_refund_consumer_group");
        String sourceSql = DwdTradeOrderRefundSql.ODS_BASE_DB + sourceDdl;
        tEnv.executeSql(sourceSql);

        //5.读取退单数据
        Table orderRefundInfoTable = tEnv.sqlQuery(DwdTradeOrderRefundSql.ORDER_REFUND_TABLE);
        tEnv.createTemporaryView("order_refund_info", orderRefundInfoTable);
//        PrintTableUtil.printTable(tEnv, orderRefundInfoTable, "退单数据");

        //6.读取订单数据
        Table orderInfoTable = tEnv.sqlQuery(DwdTradeOrderRefundSql.ORDER_INFO_TABLE);
        DataStream<OrderInfoRefundBean> orderRefundInfoDs = tEnv.toAppendStream(orderInfoTable, OrderInfoRefundBean.class);
        orderRefundInfoDs.print();
        //7.筛选符合条件的订单表退单数据
        SingleOutputStreamOperator<OrderInfoRefundBean> filterDataStream = orderRefundInfoDs.filter(data -> {
                    String oldData = data.getOld();
                    if (oldData != null) {
                        JSONObject jsonObject = JSON.parseObject(oldData);
                        Set<String> keySet = jsonObject.keySet();

                        return keySet.contains("order_status");
                    }

                    return false;
                }
        );


        //8.将订单表退化为流
        Table orderRefundTable = tEnv.fromDataStream(filterDataStream);
        tEnv.createTemporaryView("order_info_refund", orderRefundTable);
//        PrintTableUtil.printTable(tEnv, orderRefundTable, "退单表数据");

        //9.建立MySQL-LookUp字典表
        String baseDicLookUpDdl = MySqlUtil.getBaseDicLookUpDdl();
        tEnv.executeSql(baseDicLookUpDdl);

        //10.关联三张表获得退单宽表数据
        Table resultTable = tEnv.sqlQuery(DwdTradeOrderRefundSql.JOIN_TABLE);
        tEnv.createTemporaryView("result_table", resultTable);
        PrintTableUtil.printTable(tEnv, resultTable, "连接表");


        String sinkSql = DwdTradeOrderRefundSql.SINK_TOPIC_DDL + CustomKafkaUtil.getUpsertDdl(SINK_TOPIC);
        tEnv.executeSql(sinkSql);
//
        tEnv.executeSql("insert into dwd_trade_order_refund_detail select * from " + resultTable).print();
        env.execute();

    }
}
