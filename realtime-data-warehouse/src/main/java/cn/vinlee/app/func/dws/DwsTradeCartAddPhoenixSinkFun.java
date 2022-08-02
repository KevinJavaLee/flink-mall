package cn.vinlee.app.func.dws;

import cn.vinlee.bean.CartAddUniqueBean;
import cn.vinlee.common.PhoenixConfigEnum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Vinlee Xiao
 * @className DwsPhoenixSinkFun
 * @date 2022/7/30 11:35:18
 **/
public class DwsTradeCartAddPhoenixSinkFun extends RichSinkFunction<CartAddUniqueBean> {

    private final String upsertSql;

    public DwsTradeCartAddPhoenixSinkFun(String upsertSql) {
        this.upsertSql = upsertSql;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DwsTradeCartAddPhoenixSinkFun.class);
    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 1. 加载驱动
        Class.forName(PhoenixDriver.class.getName());
        // 2. 获取JDBC连接
        Properties props = new Properties();
        connection = DriverManager.getConnection(PhoenixConfigEnum.PHOENIX_SERVER.getName(), props);
    }

    @Override
    public void invoke(CartAddUniqueBean value, Context context) throws Exception {

        try (PreparedStatement preparedStatement = connection.prepareStatement(upsertSql)) {

            preparedStatement.setObject(1, value.getStartTime());
            preparedStatement.setObject(2, value.getEndTime());
            preparedStatement.setObject(3, value.getCartAddUniqueCount());
            preparedStatement.setObject(4, value.getTs());
            preparedStatement.execute();

            connection.commit();
        } catch (SQLException e) {
            LOGGER.warn("数据插入失败: [{}]", upsertSql);
        }
    }

}
