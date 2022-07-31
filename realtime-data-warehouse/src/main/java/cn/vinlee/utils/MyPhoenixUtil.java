package cn.vinlee.utils;

import cn.vinlee.bean.TransientSink;
import cn.vinlee.common.ClickHouseConfig;
import cn.vinlee.common.PhoenixConfigEnum;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * Phoenix连接工具类.
 *
 * @author Vinlee Xiao
 * @className MyPhoenixUtil
 * @date 2022/7/29 22:45:59
 **/
public class MyPhoenixUtil {

    private MyPhoenixUtil() {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MyPhoenixUtil.class);

    /**
     * 得到Jdbc连接驱动
     *
     * @return JdbcConnectionOptions
     */
    public static JdbcConnectionOptions getJdbcConnectionOptions() {

        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(PhoenixConfigEnum.PHOENIX_SERVER.getName())
                .withDriverName(PhoenixConfigEnum.PHOENIX_DRIVER.getName())
                .withConnectionCheckTimeoutSeconds(ClickHouseConfig.TIME_OUT_SECONDS)
                .build();
    }

    /**
     * 得到Jdbc 运行属性的设置.
     *
     * @return JdbcExecutionOptions
     */
    public static JdbcExecutionOptions getJdbcExecutionOptions() {

        return new JdbcExecutionOptions.Builder().withBatchSize(1).withMaxRetries(3).withBatchIntervalMs(100L).build();
    }

    public static <T> JdbcStatementBuilder<T> getJdbcStatementBuilder() {

        return (preparedStatement, t) -> {

            LOGGER.info("进入反射");
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
        };
    }

    public static <T> SinkFunction<T> getPhoenixSink(String sql) {
        LOGGER.info("插入Hbase的语句 [{}]", sql);
        return JdbcSink.<T>sink(sql, getJdbcStatementBuilder(), getJdbcExecutionOptions(), getJdbcConnectionOptions());
    }
}
