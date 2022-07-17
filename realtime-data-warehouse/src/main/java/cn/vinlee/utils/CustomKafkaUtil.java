package cn.vinlee.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * Kafka连接工具类.
 *
 * @author Vinlee Xiao
 * @className CustomKafkaUtil
 * @date 2022/7/14 20:42:59
 **/
public class CustomKafkaUtil {

    private CustomKafkaUtil() {
        
    }

    private static final Properties PROPERTIES = new Properties();
    private static final String BOOTSTRAP_SERVERS = "hadoop1:9092";

    //静态代码块
    static {
        PROPERTIES.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }

    /**
     * 返回Kafka消费者.
     *
     * @param topic   主题
     * @param groupId 消费者id.
     * @return 返回Kafka消费者对象.
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {

        PROPERTIES.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        PROPERTIES.setProperty(ConsumerConfig.E)
        return new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

                if (consumerRecord == null || consumerRecord.value() == null) {
                    return null;
                } else {
                    return new String(consumerRecord.value());
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }, PROPERTIES);

    }


    /**
     * 获取Kafka生产者.
     *
     * @param topic 主题
     * @return @FlinkKafkaProducer
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), PROPERTIES);
    }
}
