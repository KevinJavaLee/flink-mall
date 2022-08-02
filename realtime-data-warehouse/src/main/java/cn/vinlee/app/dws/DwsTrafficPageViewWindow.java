package cn.vinlee.app.dws;

import cn.vinlee.app.func.dws.DwsPageViewWindownPhoenixSinkFun;
import cn.vinlee.bean.TrafficHomeDetailBean;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.DateFormatUtil;
import cn.vinlee.utils.MyClickHouseUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
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
 * Dws层页面各浏览窗口统计.
 *
 * @author Vinlee Xiao
 * @className DwsTrafficPageViewWindow
 * @date 2022/7/30 22:29:16
 **/
public class DwsTrafficPageViewWindow {
    static final String SOURCE_TOPIC = "dwd_traffic_page_log";

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

        //5.过滤数据，只需要访问主页和商品详情页的数据
        SingleOutputStreamOperator<JSONObject> filterDataStream = mapStream.filter(new FilterFunction<JSONObject>() {

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String page = jsonObject.getJSONObject("page").getString("page_id");
                return "good_detail".equals(page) || "home".equals(page);

            }
        });
        //6.声明水位线
        SingleOutputStreamOperator<JSONObject> homeDetailStream = filterDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLong("ts")));

        //7.按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = homeDetailStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        //8.计算主页和各个详情页的独立访客 使用flatMap
        SingleOutputStreamOperator<TrafficHomeDetailBean> homeDetailPageFlatMapStream =
                keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailBean>() {

                    private transient ValueState<String> homeLastVisitTimeState;
                    private transient ValueState<String> detailLastVisiTimeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //设置TTL状态
                        StateTtlConfig stateTtlConfig =
                                new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();

                        //初始化Home页面访问日期状态
                        ValueStateDescriptor<String> homeTimeDescriptor = new ValueStateDescriptor<>("home-time", String.class);
                        homeTimeDescriptor.enableTimeToLive(stateTtlConfig);
                        homeLastVisitTimeState = getRuntimeContext().getState(homeTimeDescriptor);

                        //初始化商品页面详情访问日期状态
                        ValueStateDescriptor<String> detailTimeDescriptor = new ValueStateDescriptor<>("detail-time", String.class);
                        detailTimeDescriptor.enableTimeToLive(stateTtlConfig);
                        detailLastVisiTimeState = getRuntimeContext().getState(detailTimeDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailBean> collector) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        Long ts = jsonObject.getLong("ts");
                        String currentDateTime = DateFormatUtil.toDate(ts);

                        //商品主页和商品详情页访问数据量
                        long homePageUserVisitorCnt = 0;
                        long detailUserVisitorCnt = 0;

                        if ("home".equals(pageId)) {
                            String homeLastVisitDateTime = homeLastVisitTimeState.value();

                            if (homeLastVisitDateTime == null || !homeLastVisitDateTime.equals(currentDateTime)) {
                                homePageUserVisitorCnt = 1L;
                                homeLastVisitTimeState.update(currentDateTime);

                            }
                        } else {

                            String detailLastVisitDateTime = detailLastVisiTimeState.value();
                            if (detailLastVisitDateTime == null || !detailLastVisitDateTime.equals(currentDateTime)) {
                                detailUserVisitorCnt = 1L;
                                detailLastVisiTimeState.update(currentDateTime);

                            }
                        }

                        if (detailUserVisitorCnt != 0L || homePageUserVisitorCnt != 0L) {
                            collector.collect(new TrafficHomeDetailBean("", "", homePageUserVisitorCnt, detailUserVisitorCnt,
                                    System.currentTimeMillis()));
                        }

                    }
                });

        homeDetailPageFlatMapStream.print();

        //9.窗口聚合函数
        SingleOutputStreamOperator<TrafficHomeDetailBean> resultDataStream = homeDetailPageFlatMapStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        ).reduce(new ReduceFunction<TrafficHomeDetailBean>() {
            @Override
            public TrafficHomeDetailBean reduce(TrafficHomeDetailBean trafficHomeDetailBean, TrafficHomeDetailBean t1) throws Exception {
                trafficHomeDetailBean.setGoodDetailUserVisitorCount(trafficHomeDetailBean.getGoodDetailUserVisitorCount() + t1.getGoodDetailUserVisitorCount());
                trafficHomeDetailBean.setHomeUserVisitorCount(t1.getHomeUserVisitorCount() + trafficHomeDetailBean.getHomeUserVisitorCount());
                return trafficHomeDetailBean;
            }
        }, new AllWindowFunction<TrafficHomeDetailBean, TrafficHomeDetailBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailBean> input,
                              Collector<TrafficHomeDetailBean> collector) throws Exception {
                TrafficHomeDetailBean pageViewCnt = input.iterator().next();

                //设置窗口开始和结束事件
                pageViewCnt.setStartTime(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                pageViewCnt.setEndTime(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                collector.collect(pageViewCnt);
            }
        });

        //10.将结果输出
        resultDataStream.print();

        String ckSinkSql = "insert into gmall.dws_traffic_page_view_window " +
                "values(?,?,?,?,?)";
        //10.1将结果输出到Clickhouse中
        resultDataStream.addSink(MyClickHouseUtil.getClickHouseSink(ckSinkSql));

        //10.2将结果输出到Phoenix中
        resultDataStream.addSink(new DwsPageViewWindownPhoenixSinkFun("upsert into GMALL_REALTIME" +
                ".dws_traffic_page_view_window" +
                " " +
                "values(?,?,?,?,?)"));

        env.execute();
    }
}
