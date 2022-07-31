package cn.vinlee.app.func.dws;

import cn.vinlee.bean.TrafficPageViewBean;
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
public class DwsPageViewVisitorCntWindownPhoenixSinkFun extends RichSinkFunction<TrafficPageViewBean> {

    private final String upsertSql;

    public DwsPageViewVisitorCntWindownPhoenixSinkFun(String upsertSql) {
        this.upsertSql = upsertSql;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DwsPageViewVisitorCntWindownPhoenixSinkFun.class);
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
    public void invoke(TrafficPageViewBean value, Context context) throws Exception {

        try (PreparedStatement preparedStatement = connection.prepareStatement(upsertSql)) {

            preparedStatement.setObject(1, value.getStartTime());
            preparedStatement.setObject(2, value.getEndTime());
            preparedStatement.setObject(3, value.getVersionChannel());
            preparedStatement.setObject(4, value.getChannel());
            preparedStatement.setObject(5, value.getRegion());
            preparedStatement.setObject(6, value.getIsNew());
            preparedStatement.setObject(7, value.getUniqueVisitorCount());
            preparedStatement.setObject(8, value.getSessionVisitorCount());
            preparedStatement.setObject(9, value.getPageVisitorCount());
            preparedStatement.setObject(10, value.getDuringSum());
            preparedStatement.setObject(11, value.getUserJumpCount());
            preparedStatement.setObject(12, value.getTs());
            preparedStatement.execute();

            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.warn("数据插入失败: [{}]", upsertSql);
        }
    }

}
