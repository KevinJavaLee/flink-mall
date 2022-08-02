package cn.vinlee.app.func.dws;

import cn.vinlee.bean.TransientSink;
import cn.vinlee.common.PhoenixConfigEnum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Phoenix保存数据工具类.
 *
 * @author Vinlee Xiao
 * @className SinkFunction
 * @date 2022/7/30 12:31:53
 **/
public class MyPhoenixSinkFunc<T> extends RichSinkFunction<T> {


    private final String upsertSql;

    public MyPhoenixSinkFunc(String upsertSql) {
        this.upsertSql = upsertSql;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MyPhoenixSinkFunc.class);
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
    public void invoke(T value, Context context) throws Exception {
        connection.setAutoCommit(false);
        try (PreparedStatement preparedStatement = connection.prepareStatement(upsertSql);) {
            Class<?> aClass = value.getClass();

            //得到所有的属性
            Field[] fields = aClass.getDeclaredFields();

            int skipCount = 0;

            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);

                TransientSink transientSink = field.getAnnotation(TransientSink.class);
                if (transientSink != null) {
                    skipCount++;
                    continue;
                }
                Object v = null;
                try {
                    v = field.get(value);
                    LOGGER.info("[{}]= [{}]", i + 1 - skipCount, v);
                } catch (IllegalAccessException e) {
                    LOGGER.error("获得属性出错!");

                }
                LOGGER.info(" preparedStatement index= [{}] , value= [{}]", i + 1 - skipCount, v);
                preparedStatement.setObject(i + 1 - skipCount, v);
                LOGGER.info("[{}]", preparedStatement);
            }
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.warn("数据插入失败: [{}]", upsertSql);
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
