package cn.vinlee.app.dws;

import cn.vinlee.app.func.dws.DwsUserLoginPhoenixSinkFun;
import cn.vinlee.bean.UserLoginBean;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.DateFormatUtil;
import cn.vinlee.utils.MyClickHouseUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.ZoneId;

/**
 * 用户登陆页面统计.
 *
 * @author Vinlee Xiao
 * @className DwsUserLoginCount
 * @date 2022/7/31 13:59:42
 **/
public class DwsUserLoginCount {
    static final String SOURCE_TOPIC = "dwd_traffic_page_log";
    static final int CONTINUE_DAY = 8;

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
        FlinkKafkaConsumerBase<String> sourceKafka = CustomKafkaUtil.getKafkaConsumer(SOURCE_TOPIC,
                "consumer_page_view_group").setStartFromEarliest();
        DataStreamSource<String> sourceDataStream = env.addSource(sourceKafka);

        SingleOutputStreamOperator<JSONObject> mapStream = sourceDataStream.map(JSON::parseObject);


        //5.得到mid不为空且last_page_id==null的数据
        SingleOutputStreamOperator<JSONObject> filterDataStream = mapStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                return common.getString("mid") != null && page.getString("last_page_id") == null;
            }
        });

        //6.提取事件时间生成水位线
        SingleOutputStreamOperator<JSONObject> filterDatastreamWithWaterMark = filterDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLong("ts")));

        //7.按照uid进行分组
        KeyedStream<JSONObject, String> keyedStream = filterDatastreamWithWaterMark.keyBy(data -> data.getJSONObject("common").getString("uid"));

        //8.状态编程，进行回流用户的独立访客数据的统计
        SingleOutputStreamOperator<UserLoginBean> processDataStream =
                keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private transient ValueState<String> lastVisitStateDateTime;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitStateDateTime = getRuntimeContext().getState(new ValueStateDescriptor<>("last" +
                                "-datetime", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {

                        String lastVisitTime = lastVisitStateDateTime.value();

                        //获取数据中的时间并转换成日期
                        Long ts = jsonObject.getLong("ts");
                        String currentDateTime = DateFormatUtil.toDate(ts);

                        //独立访客数据和回流用户数据
                        long uniqueUserCnt = 0L;
                        long backCnt = 0L;

                        //如果保存的日期为null,则为新用户
                        if (lastVisitTime == null) {
                            uniqueUserCnt = 1L;
                            lastVisitStateDateTime.update(currentDateTime);

                        } else {

                            //日期与当前日期不同，则为今天的第一条数据
                            if (!lastVisitTime.equals(currentDateTime)) {
                                uniqueUserCnt = 1L;
                                lastVisitStateDateTime.update(currentDateTime);

                                Long lastTimestamp = DateFormatUtil.toTimestamp(lastVisitTime);
                                long days = (ts - lastTimestamp) / (1000 * 60 * 60 * 24);

                                if (days >= CONTINUE_DAY) {
                                    backCnt = 1;
                                }
                            }
                        }

                        //如果独立用户数据为1，则输出
                        if (uniqueUserCnt == 1L) {
                            collector.collect(new UserLoginBean("", "", backCnt, uniqueUserCnt, System.currentTimeMillis()));
                        }

                    }
                });

        //9.开窗聚合函数
        SingleOutputStreamOperator<UserLoginBean> resultStream = processDataStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean userLoginBean, UserLoginBean t1) throws Exception {
                userLoginBean.setUniqueUserCount(userLoginBean.getUniqueUserCount() + t1.getUniqueUserCount());
                userLoginBean.setBackUserCount(userLoginBean.getBackUserCount() + t1.getBackUserCount());
                return userLoginBean;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> input,
                              Collector<UserLoginBean> collector) throws Exception {
                UserLoginBean value = input.iterator().next();

                value.setStartTime(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                value.setEndTime(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                collector.collect(value);
            }
        });

        resultStream.print("最后结果");

        resultStream.addSink(MyClickHouseUtil.getClickHouseSink("insert into gmall.dws_user_login_window values" +
                "(?,?,?,?,?)"));

        resultStream.addSink(new DwsUserLoginPhoenixSinkFun("upsert into GMALL_REALTIME.dws_user_login_window " +
                "values" +
                "(?,?,?,?,?)"));

        env.execute();
    }
}
