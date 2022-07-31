package cn.vinlee.utils;

import cn.vinlee.bean.TransientSink;
import cn.vinlee.common.ClickHouseConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Clickhouse工具类.
 *
 * @author Vinlee Xiao
 * @className MyClickHouseUtil
 * @date 2022/7/29 21:14:32
 **/
public class MyClickHouseUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyClickHouseUtil.class);

    private MyClickHouseUtil() {

    }

    /**
     * 得到Jdbc连接驱动
     *
     * @return JdbcConnectionOptions
     */
    public static JdbcConnectionOptions getJdbcConnectionOptions() {

        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(ClickHouseConfig.CLICKHOUSE_URL)
                .withDriverName(ClickHouseConfig.CLICKHOUSE_DRIVER)
                .withConnectionCheckTimeoutSeconds(ClickHouseConfig.TIME_OUT_SECONDS)
                .withUsername(ClickHouseConfig.USER_NAME)
                .withPassword(ClickHouseConfig.PASSWORD).build();
    }


    /**
     * 得到Jdbc 运行属性的设置.
     *
     * @return JdbcExecutionOptions
     */
    public static JdbcExecutionOptions getJdbcExecutionOptions() {

        return new JdbcExecutionOptions.Builder().withBatchSize(1).withMaxRetries(3).withBatchIntervalMs(1000L).build();
    }


    public static <T> JdbcStatementBuilder<T> getJdbcStatementBuilder() {

        return new JdbcStatementBuilder<T>() {

            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                //使用反射提取字段
                Class<?> tClass = t.getClass();

                //得到所有的属性
                Field[] fields = tClass.getDeclaredFields();

                int skipCount = 0;

                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[i];
                    field.setAccessible(true);

                    TransientSink transientSink = field.getAnnotation(TransientSink.class);
                    if (transientSink != null) {
                        skipCount++;
                        continue;
                    }
                    Object value = null;
                    try {
                        value = field.get(t);
                    } catch (IllegalAccessException e) {
                        LOGGER.error("获得属性出错!");

                    }
                    preparedStatement.setObject(i + 1 - skipCount, value);
                }
            }
        };
    }

    public static <T> SinkFunction<T> getClickHouseSink(String sql) {
        LOGGER.info("插入Clickhouse的语句 [{}]", sql);
        return JdbcSink.<T>sink(sql, getJdbcStatementBuilder(), getJdbcExecutionOptions(), getJdbcConnectionOptions());
    }
}
