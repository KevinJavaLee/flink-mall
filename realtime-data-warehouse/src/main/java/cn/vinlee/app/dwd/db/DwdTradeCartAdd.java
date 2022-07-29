package cn.vinlee.app.dwd.db;

import cn.vinlee.app.sql.dwd.TradeCartAddSql;
import cn.vinlee.utils.CustomKafkaUtil;
import cn.vinlee.utils.MySqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

/**
 * 交易域加购事实表.
 *
 * @author Vinlee Xiao
 * @className DwdTradeCartAdd
 * @date 2022/7/23 14:18:32
 **/
public class DwdTradeCartAdd {
    static final String TRADE_CRAT_ADD_SINK_TOPIC = "dwd_trade_cart_add_detail";
    static final Logger LOGGER = LoggerFactory.getLogger(DwdTradeCartAdd.class);

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
        //重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));

        //4.从Kafka中读取数据
        String sourceDatabaseDdl = CustomKafkaUtil.getTopicDatabaseDdl("dwd_trade_cart_add_consumer_group");
        tEnv.executeSql(sourceDatabaseDdl);
        //查询语句
        //Table table = tableEnv.sqlQuery("select * from ods_base_db");
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        //rowDataStream.print("ods_base_db");

        //5.过滤掉加购事实表
        Table cartAddTable = tEnv.sqlQuery(TradeCartAddSql.CART_ADD);

        //5.1建立临时表
        tEnv.createTemporaryView("cart_add", cartAddTable);
        //5.2打印查询的数据
//        DataStream<Row> cartAddRowDataStream = tEnv.toAppendStream(cartAddTable, Row.class);
//        cartAddRowDataStream.print("cart");

        //6.连接两个表,查询订单的信息
        //6.1读取Mysql数据
        String baseDicSql = MySqlUtil.getBaseDicLookUpDdl();
        tEnv.executeSql(baseDicSql);

        Table cartAddInfo = tEnv.sqlQuery(TradeCartAddSql.CART_ADD_INFO);
        tEnv.createTemporaryView("cart_add_info", cartAddInfo);

        //6.1打印输出数据
//        DataStream<Row> cartRowDataStream = tEnv.toAppendStream(cartAddInfo, Row.class);
//        cartRowDataStream.print("加购信息");


        //7.将数据写回Kafka
        String kafkaDdl = CustomKafkaUtil.getKafkaDdl(TRADE_CRAT_ADD_SINK_TOPIC, "");
        String sinkDdlSql = TradeCartAddSql.SINK_TRADE_CARD_INFO + kafkaDdl;

        LOGGER.info(" sink sql [ {} ]", sinkDdlSql);
        tEnv.executeSql(sinkDdlSql);
        TableResult tableResult = tEnv.executeSql("insert into trade_cart_add select * from cart_add_info");
        tableResult.print();

        env.execute();

    }
}
