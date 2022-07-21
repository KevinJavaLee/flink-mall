package cn.vinlee.common;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE;

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

        //生产者的事务超时属性 使用EXACTLY_ONCE需要增加
        PROPERTIES.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(1000 * 60 * 5));

        //设置事务ID,这里用了类名做唯一ID
        PROPERTIES.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, CustomKafkaUtil.class.getSimpleName());

        //开启幂等性
        PROPERTIES.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //
        PROPERTIES.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
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

    public static FlinkKafkaProducer<String> getKafkaProducerExactly(String topic) {
        //FlinkKafkaProducer 默认不读取全局配置而是写死默认值AT_LEAST_ONCE 在创建KafkaProducer时要指定时间语义 详见: new FlinkKafkaProducer<>()
        Optional<FlinkFixedPartitioner<String>> customPartitioner = Optional.of(new FlinkFixedPartitioner<>());
        return new FlinkKafkaProducer<>(topic,
                new SimpleStringSchema(),
                PROPERTIES,
                customPartitioner.orElse(null),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
                DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);


    }

}
