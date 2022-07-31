package cn.vinlee.app.dws;

import cn.vinlee.app.func.dws.DwsKeyWordPagePhoenixSinkFun;
import cn.vinlee.app.func.dws.SplitFunction;
import cn.vinlee.app.sql.dws.DwsSourceKeyWordPageViewSql;
import cn.vinlee.bean.KeywordBean;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.MyClickHouseUtil;
import cn.vinlee.utils.PrintTableUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 网页页面浏览关键词浏览量.
 *
 * @author Vinlee Xiao
 * @className DwdTrafficSourceKeyWordPageView
 * @date 2022/7/29 20:15:08
 **/
public class DwsTrafficSourceKeyWordPageView {
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

        //4.从Kafka对应的主题中加载数据
        String sourceDdl = CustomKafkaUtil.getKafkaDdl(SOURCE_TOPIC, "dwd_interaction_favor_add_consumer_group");
        String sourceSql = DwsSourceKeyWordPageViewSql.SOURCE_TOPIC_DDL + sourceDdl;
        tEnv.executeSql(sourceSql);

        //5.过滤出是搜索字段的数据
        Table keywordTable = tEnv.sqlQuery(DwsSourceKeyWordPageViewSql.FILTER_SEARCH_SQL);
        tEnv.createTemporaryView("key_word_table", keywordTable);

        //6.使用自定义函数，进行分词处理
        tEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tEnv.sqlQuery("select word,rt from key_word_table,LATERAL TABLE(SplitFunction(key_word))");
        tEnv.createTemporaryView("split_table", splitTable);

        //7.分组开窗聚合
        Table resultTable = tEnv.sqlQuery(DwsSourceKeyWordPageViewSql.AGEGATE_SQL).as("source", "startTime", "endTime",
                "keyword", "keywordCount", "ts");
        tEnv.createTemporaryView("result_table", resultTable);


        PrintTableUtil.printTable(tEnv, resultTable, "结果表");

        DataStream<KeywordBean> keywordBeanDataStream = tEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print();


        keywordBeanDataStream.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into gmall.dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        String sinkSql = "upsert into GMALL_REALTIME.dws_traffic_source_keyword_page_view_window (start_time,end_time,source," +
                "keyword,keyword_count,ts) values (?,?,?,?,?,?)";
//
        keywordBeanDataStream.addSink(new DwsKeyWordPagePhoenixSinkFun(sinkSql));


        env.execute();
    }
}
