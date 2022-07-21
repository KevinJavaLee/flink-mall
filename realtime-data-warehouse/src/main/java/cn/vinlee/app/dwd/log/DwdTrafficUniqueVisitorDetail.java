package cn.vinlee.app.dwd.log;

import cn.vinlee.app.func.dwd.UniqueVisitorFilterFunc;
import cn.vinlee.common.CustomKafkaUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 流量域独立访客事实表.
 *
 * @author Vinlee Xiao
 * @className DwdTrafficUniqueVisitorDetail
 * @date 2022/7/20 20:42:22
 **/
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //2.设置checkpoint相关的数据。
        env.enableCheckpointing(30000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop1:8020/dw"
        );


        //3.读取kafka dwd_traffic_page_log 中的数据
        String sourceTopic = "dwd_traffic_page_log";
        String consumerGroup = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumerBase<String> kafkaConsumerBase = CustomKafkaUtil.getKafkaConsumer(sourceTopic, consumerGroup).setStartFromEarliest();
        DataStreamSource<String> kafkaSourceStream = env.addSource(kafkaConsumerBase);

        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {

        };

        SingleOutputStreamOperator<JSONObject> processStream = kafkaSourceStream.process(new ProcessFunction<String,
                JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });


        //4.过滤掉非last_page_id=null
        SingleOutputStreamOperator<JSONObject> filterStream = processStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String value = jsonObject.getJSONObject("page").getString("last_page_id");
                return value == null;
            }
        });

        //5.按照mid进行分组
        KeyedStream<JSONObject, String> keyedByMidStream = filterStream.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //6.使用状态编程进行每日登录数据去重
        SingleOutputStreamOperator<JSONObject> filterVisitorStream =
                keyedByMidStream.filter(new UniqueVisitorFilterFunc());

        //7.将数据落盘到Kafka
        String sinkTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaProducer<String> kafkaProducer = CustomKafkaUtil.getKafkaProducer(sinkTopic);
        SingleOutputStreamOperator<String> sinkMapStream = filterVisitorStream.map(data -> data.toJSONString());
        sinkMapStream.addSink(kafkaProducer);
        env.execute();
    }
}
