package cn.vinlee.app.dim;

import cn.vinlee.app.func.DimPhoenixSinkFun;
import cn.vinlee.app.func.TableProcessConfigBroadcastFunction;
import cn.vinlee.bean.TableProcessConfig;
import cn.vinlee.common.CustomKafkaUtil;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 获取维度数据.
 *
 * @author Vinlee Xiao
 * @className DimApp
 * @date 2022/7/14 21:03:15
 **/
public class DimApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //kafka每个主题分为三个分区
        env.setParallelism(3);

        //2.开启checkpoint
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(100000L);
        env.getCheckpointConfig().setCheckpointTimeout(150000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/gmall/ck");


        //3.读取kafka中的数据
        FlinkKafkaConsumer<String> kafkaConsumer = CustomKafkaUtil.getKafkaConsumer("ods_base_db",
                "dim_app1");
        //读取最开始的数据
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> kafkaSourceStream
                = env.addSource(kafkaConsumer);
        //取出脏数据答应
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        //4.过滤掉非Json数据
        SingleOutputStreamOperator<JSONObject> processStream = kafkaSourceStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(outputTag, s);
                }

            }
        });
        //5.取出脏数据打印
        DataStream<String> sideOutput = processStream.getSideOutput(outputTag);
        sideOutput.print("Dirty data");

        //6.使用FlinkCDC读取MySQL中的配置信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop2")
                .port(3306)
                .username("root")
                .password("Kevin1401597760")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql source");
        mysqlSource.print();

        //6.将配置信息流处理成广播流
        MapStateDescriptor<String, TableProcessConfig> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcessConfig.class);
        //7.广播流
        BroadcastStream<String> broadcastStream = mysqlSource.broadcast(mapStateDescriptor);

        //8.连接广播流和主流
        BroadcastConnectedStream<JSONObject, String> connectStream = processStream.connect(broadcastStream);

        //9.根据广播流处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDs = connectStream.process(new TableProcessConfigBroadcastFunction(mapStateDescriptor));

        hbaseDs.print("得到数据");
        hbaseDs.addSink(new DimPhoenixSinkFun());

        env.execute();
    }
}
