package cn.vinlee;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Vinlee Xiao
 * @className FlinkCDC
 * @description 通过FlinkCDC采集MySQL业务数据
 * @date 2022/7/10 12:25:44
 **/
public class FlinkCdc {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.设置checkpoint状态后端存储的位置

        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/gmall/ck");
//        //2.启动检查点间隔时间 10s
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //同时只能有一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        env.enableCheckpointing(5000);
//
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop2")
                .port(3306)
                .databaseList("gmall")
//                .tableList("gmall.a*")
                .username("root")
                .password("Kevin1401597760")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .scanNewlyAddedTableEnabled(true)
                .serverTimeZone("Asia/Shanghai")
                .build();


        DataStreamSource<String> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");


        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
