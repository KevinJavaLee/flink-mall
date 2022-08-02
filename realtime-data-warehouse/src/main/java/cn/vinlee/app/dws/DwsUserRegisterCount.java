package cn.vinlee.app.dws;

import cn.vinlee.app.func.dws.DwsUserRegisterPhoenixSinkFun;
import cn.vinlee.bean.UserRegisterBean;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.DateFormatUtil;
import cn.vinlee.utils.MyClickHouseUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @author Vinlee Xiao
 * @className DwsUserRegisterCount
 * @date 2022/7/31 18:09:11
 **/
public class DwsUserRegisterCount {
    static final String SOURCE_TOPIC = "dwd_user_register_detail";

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
                "dws_user_register_consumer").setStartFromEarliest();
        DataStreamSource<String> sourceDataStream = env.addSource(sourceKafka);
        //5.封装数据为UserRegisterBean
        SingleOutputStreamOperator<UserRegisterBean> mapStream = sourceDataStream.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            Long ts = jsonObject.getLong("ts");
            return new UserRegisterBean("", "", 1L, ts * 1000);
        });

        //6.生成水位线
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWm = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)
        ).withTimestampAssigner((SerializableTimestampAssigner<UserRegisterBean>) (userRegisterBean, l) -> userRegisterBean.getTs()));

//        userRegisterWithWm.print();
        //7.开窗聚合
        AllWindowedStream<UserRegisterBean, TimeWindow> windowStream = userRegisterWithWm.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<UserRegisterBean> resultStream = windowStream.reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean userRegisterBean, UserRegisterBean t1) throws Exception {
                userRegisterBean.setRegisterUserCount(userRegisterBean.getRegisterUserCount() + t1.getRegisterUserCount());
                return userRegisterBean;
            }
        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<UserRegisterBean> data,
                              Collector<UserRegisterBean> collector) throws Exception {
                UserRegisterBean registerBean = data.iterator().next();
                registerBean.setStartTime(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                registerBean.setEndTime(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                collector.collect(registerBean);
            }
        });
        resultStream.print("结果");
        resultStream.addSink(MyClickHouseUtil.getClickHouseSink("insert into gmall.dws_user_register_window values(?," +
                "?,?,?)"));

        resultStream.addSink(new DwsUserRegisterPhoenixSinkFun("upsert into GMALL_REALTIME.dws_user_register_window " +
                "values(?," +
                "?,?,?)"));


        env.execute();
    }
}
