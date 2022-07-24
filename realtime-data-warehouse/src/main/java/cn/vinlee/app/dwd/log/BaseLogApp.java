package cn.vinlee.app.dwd.log;

import cn.vinlee.app.func.dwd.DwdTopicEnum;
import cn.vinlee.app.func.dwd.ValidateOldNewCustomersFunc;
import cn.vinlee.utils.CustomKafkaUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 对日志数据进行分类输出到对应的Kafka主题中去.
 *
 * @author Vinlee Xiao
 * @className BaseLogApp
 * @date 2022/7/19 19:00:13
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // 1.设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 2.开启checkpoint
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(100000L);
        env.getCheckpointConfig().setCheckpointTimeout(150000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/gmall/dw");

        // 3.读取Kafka ods_base_log 日志数据
        FlinkKafkaConsumerBase<String> kafkaSource =
                CustomKafkaUtil.getKafkaConsumer("ods_base_log", "base_log_consumer")
                        .setStartFromEarliest();

        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

        // 4.将数据转换成Json数据.过滤掉非Json数据。
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> processJsonStream =
                kafkaDataStream.process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(
                                    String s,
                                    ProcessFunction<String, JSONObject>.Context context,
                                    Collector<JSONObject> collector)
                                    throws Exception {

                                try {
                                    JSONObject jsonObject = JSON.parseObject(s);
                                    collector.collect(jsonObject);
                                } catch (Exception e) {
                                    context.output(dirtyTag, s);
                                }
                            }
                        });

        // 5.使用状态编程，做新老用户状态 校验
        KeyedStream<JSONObject, String> keyedByMidStream = processJsonStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> validatedMapStream = keyedByMidStream.map(new ValidateOldNewCustomersFunc());
        validatedMapStream.print("校验过后的访客数据");
        // 6.使用侧输出流对数据进行分流
        //6.1页面浏览:主流

        //6.2启动日志:侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        //6.3曝光日志:侧输出流
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        //6.4动作日志:侧输出流
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        //6.5错误日志:侧输出流
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<String> pageDataStream = validatedMapStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                String jsonString = jsonObject.toJSONString();

                //6.6.1尝试获取数据中的Error字段
                String errorInfo = jsonObject.getString("err");
                if (errorInfo != null) {
                    //输出数据到错误日志中
                    context.output(errorTag, jsonString);
                }

                //6.6.2尝试获取启动字段
                String startInfo = jsonObject.getString("start");
                if (startInfo != null) {
                    context.output(startTag, jsonString);
                } else {

                    //6.6.3取出页面与时间戳
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");
                    String common = jsonObject.getString("common");

                    //6.6.4尝试获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    if (displays != null && !displays.isEmpty()) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displayObj = displays.getJSONObject(i);
                            displayObj.put("page_id", pageId);
                            displayObj.put("ts", ts);
                            displayObj.put("common", common);

                            context.output(displayTag, displayObj.toJSONString());
                        }
                    }

                    //6.6.5 尝试获取动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && !actions.isEmpty()) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject actionObj = actions.getJSONObject(i);
                            actionObj.put("page_id", pageId);
                            actionObj.put("ts", ts);
                            actionObj.put("common", common);

                            context.output(actionTag, actionObj.toJSONString());
                        }
                    }

                    //6.6.6输出数据到页面流浪日志
                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }

            }
        });

        //7.提个各种类型的数据
        DataStream<String> startDataStream = pageDataStream.getSideOutput(startTag);
        DataStream<String> errorDataStream = pageDataStream.getSideOutput(errorTag);
        DataStream<String> displayDataStream = pageDataStream.getSideOutput(displayTag);
        DataStream<String> actionDataStream = pageDataStream.getSideOutput(actionTag);


        //8.将各个流的数据分别写到Kafka对应的主题种去.
        pageDataStream.print("page");
        startDataStream.print("start");
        errorDataStream.print("error");
        displayDataStream.print("display");
        actionDataStream.print("action");

        pageDataStream.addSink(CustomKafkaUtil.getKafkaProducerExactly(DwdTopicEnum.DWD_TRAFFIC_PAGE_LOG.getName()));
        pageDataStream.addSink(CustomKafkaUtil.getKafkaProducerExactly(DwdTopicEnum.DWD_TRAFFIC_START_LOG.getName()));
        pageDataStream.addSink(CustomKafkaUtil.getKafkaProducerExactly(DwdTopicEnum.DWD_TRAFFIC_ERROR_LOG.getName()));
        pageDataStream.addSink(CustomKafkaUtil.getKafkaProducerExactly(DwdTopicEnum.DWD_TRAFFIC_DISPLAY_LOG.getName()));
        pageDataStream.addSink(CustomKafkaUtil.getKafkaProducerExactly(DwdTopicEnum.DWD_TRAFFIC_ACTION_LOG.getName()));


        env.execute("base_app_log");
    }
}
