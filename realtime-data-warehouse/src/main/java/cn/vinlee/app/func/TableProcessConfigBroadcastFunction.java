package cn.vinlee.app.func;

import cn.vinlee.bean.TableProcessConfig;
import cn.vinlee.common.PhoenixConfigEnum;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 根据广播流处理主流函数
 *
 * @author Vinlee Xiao
 * @className TableProcessConfigBroadcastFunction
 * @date 2022/7/15 20:28:05
 **/
public class TableProcessConfigBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableProcessConfigBroadcastFunction.class);

    private final MapStateDescriptor<String, TableProcessConfig> stateDescriptor;
    private transient Connection connection;


    public TableProcessConfigBroadcastFunction(MapStateDescriptor<String, TableProcessConfig> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(PhoenixConfigEnum.PHOENIX_SERVER.getName());
    }

    /**
     * jsonObject: {"database":"gmall","table":"cart_info","type":"update".
     * "data":{"id":100924,"user_id":"93","sku_id":16,"cart_price":4488.00}
     *
     * @param jsonObject
     * @param readOnlyContext
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //1.获取广播流配置数据
        ReadOnlyBroadcastState<String, TableProcessConfig> broadcastState = readOnlyContext.getBroadcastState(stateDescriptor);
        TableProcessConfig tableProcessConfig = broadcastState.get(jsonObject.getString("table"));


        //2.处理主流的数据
        String type = jsonObject.getString("type");

        if (tableProcessConfig != null && (("bootstrap-insert".equals(type)) || "insert".equals(type) || "update".equals(type))) {
            //2.1根据sinkColumns过滤数据
            LOGGER.info("得到的维度数据: [{}]", jsonObject);
            JSONObject data = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcessConfig.getSinkColumns();
            filterDataset(data, sinkColumns);

            //3.补充sinkTable字段
            jsonObject.put("sinkTable", tableProcessConfig.getSinkTable());
            LOGGER.info("得到的维度数据: [{}]", jsonObject);
            collector.collect(jsonObject);

        } else {
//            LOGGER.info("过滤掉的数据: [{}]", jsonObject);
        }


    }

    /**
     * 根据sinkColumns过滤数据.
     *
     * @param data        原始数据.
     * @param sinkColumns 数据字段.
     */
    private void filterDataset(JSONObject data, String sinkColumns) {

        //根据逗号分割
        String[] split = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(split);
        //如果当前data数据中不包含对应的列名，则删除.
        data.entrySet().removeIf(d -> !columnList.contains(d.getKey()));
    }

    /**
     * {"before":null,"after":{"source_table":"source","sink_table":"sink","sink_columns":"name,sex,age","sink_pk":null,"sink_extend":null}
     *
     * @param s
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        //1.将数据解析成JavaBean对象
        JSONObject jsonObject = JSONObject.parseObject(s);
        String data = jsonObject.getString("after");
        JSONObject tableProcessConfigObject = JSON.parseObject(data);

        String sourceTableName = tableProcessConfigObject.getString("source_table");
        String sinkTable = tableProcessConfigObject.getString("sink_table");
        String sinkColumns = tableProcessConfigObject.getString("sink_columns");
        String sinkPk = tableProcessConfigObject.getString("sink_pk");
        String sinkExtend = tableProcessConfigObject.getString("sink_extend");

        TableProcessConfig tableProcessConfig = TableProcessConfig.builder().sinkTable(sinkTable)
                .sourceTable(sourceTableName)
                .sinkColumns(sinkColumns)
                .sinkExtend(sinkExtend)
                .sinkPk(sinkPk).build();

        LOGGER.info("处理广播数据 [{}]", tableProcessConfig);
        //2.校验表是否存在
        checkTableExists(tableProcessConfig.getSinkTable(), tableProcessConfig.getSinkColumns(),
                tableProcessConfig.getSinkPk(), tableProcessConfig.getSinkExtend());

        //3.将数据写入状态
        String sourceTable = tableProcessConfig.getSourceTable();
        BroadcastState<String, TableProcessConfig> broadcastState = context.getBroadcastState(stateDescriptor);
        broadcastState.put(sourceTable, tableProcessConfig);

    }

    /**
     * 在Phoenix 中校验并建表 create table if not exists db.tn(id varchar primary key,name varchar,...) xxx
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTableExists(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }

            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(PhoenixConfigEnum.HBASE_SCHEMA.getName())
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            //遍历每个列名
            String[] columns = sinkColumns.trim().split(",");
            for (int i = 0; i < columns.length; i++) {

                //获取字段
                String column = columns[i];

                //判断是否是主键
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }

                //如果不是最后一个字段，则添加，
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }

            sql.append(")").append(sinkExtend);
            LOGGER.info("SQL语句: [{}]", sql);

            //预编译sql
            preparedStatement = connection.prepareStatement(sql.toString());
            //运行sql
            preparedStatement.execute();
        } catch (SQLException e) {
            LOGGER.warn("建表 [{}] 失败！", sinkTable);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
