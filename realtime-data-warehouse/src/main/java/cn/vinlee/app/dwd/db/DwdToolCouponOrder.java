package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.DwdToolCouponOrderSql;
import cn.vinlee.bean.CouponUseOrderBean;
import cn.vinlee.utils.CustomKafkaUtil;
import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;
import java.util.Map;
import java.util.Set;

/**
 * 工具域优惠券使用事实表
 *
 * @author Vinlee Xiao
 * @className DwdToolCouponOrder
 * @date 2022/7/26 19:49:43
 **/
public class DwdToolCouponOrder {
    /**
     * 要读取的Kafka主题.
     */
    static final String SOURCE_TOPIC = "ods_base_db";
    /**
     * 要插入的kafka主题.
     */
    static final String SINK_TOPIC = "dwd_tool_coupon_order_detail";

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

        String sourceDdl = CustomKafkaUtil.getKafkaDdl(SOURCE_TOPIC, "dwd_tool_coupon_order_consumer_group");
        String sourceSql = DwdToolCouponOrderSql.SOURCE_TOPIC + sourceDdl;
        tEnv.executeSql(sourceSql);


        //4.读取优惠券领用数据,封装为流
        Table couponUserOrderTable = tEnv.sqlQuery(DwdToolCouponOrderSql.COUPON_USE);
        DataStream<CouponUseOrderBean> couponUseOrderBeanDataStream = tEnv.toAppendStream(couponUserOrderTable, CouponUseOrderBean.class);


        //5.转换成DataStream进行修改 筛选优惠券事件被修改的数据
        SingleOutputStreamOperator<CouponUseOrderBean> filterDataStream = couponUseOrderBeanDataStream.filter(data -> {
            String oldObj = data.getOld();

            if (oldObj != null) {
                Map parseObject = JSON.parseObject(oldObj, Map.class);
                Set keySet = parseObject.keySet();
                return keySet.contains("using_time");
            }

            return false;
        });


        //6.将DataStream转换成Flink SQL
        Table filterTable = tEnv.fromDataStream(filterDataStream).as("id", "coupon_id", "user_id",
                "order_id", "date_id", "order_time", "old", "ts");
        tEnv.createTemporaryView("filter_table", filterTable);

        //7.建立dwd_tool_coupon_order_detail表
        String sinkSql = DwdToolCouponOrderSql.SINK_TOPIC + CustomKafkaUtil.getUpsertDdl(SINK_TOPIC);
        tEnv.executeSql(sinkSql);

        //8.将数据保存到Kafka中
        tEnv.executeSql(" insert into dwd_tool_coupon_order_detail select id,coupon_id,user_id,order_id,date_id," +
                "order_time,ts from " + filterTable).print();

        env.execute();

    }
}
