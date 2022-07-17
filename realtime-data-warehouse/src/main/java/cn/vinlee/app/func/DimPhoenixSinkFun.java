package cn.vinlee.app.func;

import cn.vinlee.common.PhoenixConfigEnum;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 将数据插入到Phoenix 中
 *
 * @author Vinlee Xiao
 * @className DimPhoenixSinkFun
 * @date 2022/7/16 18:18:16
 **/
public class DimPhoenixSinkFun extends RichSinkFunction<JSONObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DimPhoenixSinkFun.class);
    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(PhoenixConfigEnum.PHOENIX_SERVER.getName());
    }


    /**
     * sinkTable,将修改或者新插入的数据插入到Phoenix中.
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        String upsertSql = getUpsertSql(sinkTable, data);
        LOGGER.info("sql 语句为: [{}]", upsertSql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(upsertSql)) {


            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.warn("数据插入失败: [{}]", upsertSql);
        }
    }


    /**
     * 得到Phoenix sql 插入语句.
     * 数据样例: { "id":"12,"tm_name":kevin}
     * sql 语句 upsert into databaseName.tableName(id,tm_name) values('1','12')
     *
     * @param sinkTable 要插入和修改数据的表
     * @param data      数据对应的值.
     * @return sql字符串
     */
    private String getUpsertSql(String sinkTable, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into " + PhoenixConfigEnum.HBASE_SCHEMA.getName() + "." + sinkTable + "(" + StringUtils.join(columns,
                ",") + ") " +
                "values('"
                + StringUtils.join(values, "','") + "')";
    }
}
