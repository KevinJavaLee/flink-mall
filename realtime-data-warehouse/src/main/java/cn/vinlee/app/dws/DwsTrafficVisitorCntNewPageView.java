package cn.vinlee.app.dws;

import cn.vinlee.app.func.dws.DwsPageViewVisitorCntWindownPhoenixSinkFun;
import cn.vinlee.bean.TrafficPageViewBean;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.DateFormatUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple4;

import java.time.Duration;
import java.time.ZoneId;

/**
 * 访客类别页面粒度.
 *
 * @author Vinlee Xiao
 * @className DwsTrafficNewPageView
 * @date 2022/7/30 14:54:13
 **/
public class DwsTrafficVisitorCntNewPageView {
    static final String PAGE_TOPIC = "dwd_traffic_page_log";
    static final String UNIQUE_VISITOR = "dwd_traffic_unique_visitor_detail";
    static final String USER_JUMP = "dwd_traffic_user_jump_detail";
    static final String GROUP_ID = "dwd_traffic_vc_ch_ar_is_new_page";

    static final Logger LOGGER = LoggerFactory.getLogger(DwsTrafficVisitorCntNewPageView.class);

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
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(1), org.apache.flink.api.common.time.Time.minutes(1)));

        //4.读取页面数据、独立用户访问数据、用户跳出数据
        FlinkKafkaConsumerBase<String> pageViewKafkaSource =
                CustomKafkaUtil.getKafkaConsumer(PAGE_TOPIC, GROUP_ID).setStartFromEarliest();
        FlinkKafkaConsumerBase<String> uniqueVisitorKafkaSource = CustomKafkaUtil.getKafkaConsumer(UNIQUE_VISITOR,
                GROUP_ID).setStartFromEarliest();
        FlinkKafkaConsumerBase<String> userJumpKafkaSource =
                CustomKafkaUtil.getKafkaConsumer(USER_JUMP, GROUP_ID).setStartFromEarliest();
        DataStreamSource<String> pageDataStream = env.addSource(pageViewKafkaSource);
        DataStreamSource<String> uniqueVisitorDataStream =
                env.addSource(uniqueVisitorKafkaSource);
        DataStreamSource<String> userJumpDataStream = env.addSource(userJumpKafkaSource);


        //5.将三个流统一数据格式
        //5.1得到独立访客数据
        SingleOutputStreamOperator<TrafficPageViewBean> uniqueVisitorMapDataStream =
                uniqueVisitorDataStream.map(line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    JSONObject common = jsonObject.getJSONObject("common");
                    return new TrafficPageViewBean("", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            1L, 0L, 0L, 0L, 0L,
                            jsonObject.getLong("ts"));

                });

        //5.2处理用户跳出数据
        SingleOutputStreamOperator<TrafficPageViewBean> userJumpMapDataStream = userJumpDataStream.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts"));
        });


        //5.3 处理页面浏览数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewMapDataStream = pageDataStream.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");

            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");

            long sessionVisitor = 0L;
            if (lastPageId == null) {
                sessionVisitor = 1L;
            }

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    sessionVisitor,
                    1L,
                    page.getLong("during_time"),
                    0L,
                    jsonObject.getLong("ts"));
        });

        //6.合并三个流
        SingleOutputStreamOperator<TrafficPageViewBean> unionDataStream = trafficPageViewMapDataStream.union(userJumpMapDataStream, uniqueVisitorMapDataStream).assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }

                })
        );


        //7.按照元组类型进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = unionDataStream.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return new Tuple4<>(trafficPageViewBean.getRegion(), trafficPageViewBean.getChannel(),
                        trafficPageViewBean.getIsNew(), trafficPageViewBean.getVersionChannel());
            }
        });

        //8.开窗聚合

        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> reduceDataStream =
                windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean trafficPageViewBean, TrafficPageViewBean t1) throws Exception {
//                        LOGGER.info("unique visitor t1=[{}], t2=[{}] , sum=[{}]",
//                                t1.getUniqueVisitorCount(), trafficPageViewBean.getUniqueVisitorCount(),
//                                t1.getUniqueVisitorCount() + trafficPageViewBean.getUniqueVisitorCount());
                        trafficPageViewBean.setUniqueVisitorCount(t1.getUniqueVisitorCount() + trafficPageViewBean.getUniqueVisitorCount());
                        trafficPageViewBean.setPageVisitorCount(t1.getPageVisitorCount() + trafficPageViewBean.getPageVisitorCount());
                        trafficPageViewBean.setDuringSum(t1.getDuringSum() + trafficPageViewBean.getDuringSum());
                        trafficPageViewBean.setUserJumpCount(t1.getUserJumpCount() + trafficPageViewBean.getUserJumpCount());
                        trafficPageViewBean.setSessionVisitorCount(t1.getSessionVisitorCount() + trafficPageViewBean.getSessionVisitorCount());
                        return trafficPageViewBean;
                    }
                }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                                      TimeWindow timeWindow, Iterable<TrafficPageViewBean> input,
                                      Collector<TrafficPageViewBean> collector) throws Exception {
                        //获取数据
                        TrafficPageViewBean trafficPageViewBean = input.iterator().next();

                        //获取窗口信息
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();

                        //补充窗口信息
                        trafficPageViewBean.setStartTime(DateFormatUtil.toYmdHms(start));
                        trafficPageViewBean.setEndTime(DateFormatUtil.toYmdHms(end));

                        //输出数据
                        collector.collect(trafficPageViewBean);
                    }
                });

        //9.将数据写入Clickhouse和Phoenix中
//        reduceDataStream.addSink(MyClickHouseUtil.getClickHouseSink("insert into " +
//                "gmall.dws_traffic_channel_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //10.将数据写入Phoenix
        String phoenixSinkSql = "upsert into " +
                "GMALL_REALTIME.dws_traffic_channel_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)";
        reduceDataStream.addSink(new DwsPageViewVisitorCntWindownPhoenixSinkFun(phoenixSinkSql));
        env.execute();
    }
}
