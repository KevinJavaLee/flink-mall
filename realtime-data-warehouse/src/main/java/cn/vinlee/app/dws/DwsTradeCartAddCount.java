package cn.vinlee.app.dws;

import cn.vinlee.app.func.dws.DwsTradeCartAddPhoenixSinkFun;
import cn.vinlee.bean.CartAddUniqueBean;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.DateFormatUtil;
import cn.vinlee.utils.MyClickHouseUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.ZoneId;

/**
 * dws层订单加购需求.
 *
 * @author Vinlee Xiao
 * @className DwsTradeCartAddCount
 * @date 2022/7/31 19:38:24
 **/
public class DwsTradeCartAddCount {
    static final String SOURCE_TOPIC = "dwd_trade_cart_add_detail";

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

        //4.从Kafka中加载数据.
        FlinkKafkaConsumerBase<String> sourceKafka = CustomKafkaUtil.getKafkaConsumer(SOURCE_TOPIC, "consumer_page_view_group").setStartFromEarliest();
        DataStreamSource<String> sourceDataStream = env.addSource(sourceKafka);
        SingleOutputStreamOperator<JSONObject> mapStream = sourceDataStream.map(JSON::parseObject);

        //5.提取水位线生辰时间戳
        SingleOutputStreamOperator<JSONObject> mapStreamWithWm = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                String dateTime = jsonObject.getString("operate_time");
                if (dateTime == null) {
                    dateTime = jsonObject.getString("create_time");
                }
                return DateFormatUtil.toTimestamp(dateTime, true);
            }
        }));

        //6.按照user_id进行分组
        KeyedStream<JSONObject, String> keyedStream = mapStreamWithWm.keyBy(jsonObject -> jsonObject.getString("user_id"));

        //7.过滤出独立用户
        SingleOutputStreamOperator<CartAddUniqueBean> flatMapDataStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUniqueBean>() {

            //上次加购的时间
            private transient ValueState<String> lastCartAddTimeState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cart_add", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);

                lastCartAddTimeState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<CartAddUniqueBean> collector) throws Exception {
                //订单上次加购的时间
                String lastCartAddTime = lastCartAddTimeState.value();

                String dateTime = jsonObject.getString("operate_time");
                if (dateTime == null) {
                    dateTime = jsonObject.getString("create_time");
                }

                //yyyy-MM-dd HH:mm:ss
                String currentDateTime = dateTime.split(" ")[0];
                if (lastCartAddTime == null || !lastCartAddTime.equals(currentDateTime)) {
                    lastCartAddTimeState.update(currentDateTime);
                    collector.collect(new CartAddUniqueBean("", "", 1L, 0L));

                }

            }
        });

        //8.进行窗口统计
        AllWindowedStream<CartAddUniqueBean, TimeWindow> windowStream = flatMapDataStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //9.进行数据的聚合
        SingleOutputStreamOperator<CartAddUniqueBean> reduceStream = windowStream.reduce(new ReduceFunction<CartAddUniqueBean>() {
            @Override
            public CartAddUniqueBean reduce(CartAddUniqueBean cartAddUniqueBean, CartAddUniqueBean t1) throws Exception {
                cartAddUniqueBean.setCartAddUniqueCount(cartAddUniqueBean.getCartAddUniqueCount() + t1.getCartAddUniqueCount());
                return cartAddUniqueBean;
            }
        }, new AllWindowFunction<CartAddUniqueBean, CartAddUniqueBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<CartAddUniqueBean> input,
                              Collector<CartAddUniqueBean> collector) throws Exception {
                CartAddUniqueBean cartAddUniqueBean = input.iterator().next();

                //补充窗口信息
                cartAddUniqueBean.setStartTime(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                cartAddUniqueBean.setEndTime(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                cartAddUniqueBean.setTs(System.currentTimeMillis());

                collector.collect(cartAddUniqueBean);
            }
        });

        //8.将数据写入到Clickhouse中
        reduceStream.print("最终的结果");
        reduceStream.addSink(MyClickHouseUtil.getClickHouseSink("insert into gmall.dws_trade_cart_add_unique_window " +
                "values(?,?,?,?)"));

        reduceStream.addSink(new DwsTradeCartAddPhoenixSinkFun("upsert into GMALL_REALTIME" +
                ".dws_trade_cart_add_unique_window " +
                "values(?,?,?,?)"));

        env.execute();
    }
}
