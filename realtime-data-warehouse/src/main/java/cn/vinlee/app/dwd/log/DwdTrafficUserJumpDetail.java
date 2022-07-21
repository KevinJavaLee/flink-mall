package cn.vinlee.app.dwd.log;

import cn.vinlee.common.CustomKafkaUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 用户跳出事务事实表.
 *
 * @author Vinlee Xiao
 * @className DwdTrafficUserJumpDetail
 * @date 2022/7/20 21:41:55
 **/
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //2.设置checkpoint相关的数据。
        env.enableCheckpointing(30000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop1:8020/dw"
        );


        //3.读取kafka dwd_traffic_page_log 中的数据
        String sourceTopic = "dwd_traffic_page_log";
        String consumerGroup = "dwd_traffic_unique_jump_detail";
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

        //4.提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> dataStreamWithTs = processStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        //5.按照用户mid进行排序
        KeyedStream<JSONObject, String> keyedStream = dataStreamWithTs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //6.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //7.将模式序列作用于流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //8.提取匹配上的事件以及超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time-out") {
        };
        SingleOutputStreamOperator<JSONObject> selectDataStream = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("first").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("first").get(0);
                    }

                });

        DataStream<JSONObject> timeOutStream = selectDataStream.getSideOutput(timeOutTag);

        //9.合并两个流事件
        DataStream<JSONObject> unionStream = selectDataStream.union(timeOutStream);
        
        //10.输出到kafak中
        String sinkTopic = "dwd_traffic_user_jump_detail";
        unionStream.map(d -> d.toJSONString()).addSink(CustomKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute();
    }
}
