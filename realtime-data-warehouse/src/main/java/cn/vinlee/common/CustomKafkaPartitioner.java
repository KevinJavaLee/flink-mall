package cn.vinlee.common;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * 自定义kafka分区规则.
 *
 * @author Vinlee Xiao
 * @className CustomKafkaPartitioner
 * @date 2022/7/19 22:12:04
 **/
public class CustomKafkaPartitioner extends FlinkKafkaPartitioner<String> {

    /**
     * @param s           正常记录
     * @param key         KeyedSerializationSchema中配置的key
     * @param value       KeyedSerializationSchema中配置的value
     * @param targetTopic targetTopic
     * @param partitions  partition列表[0, 1, 2]
     * @return 返回分区.
     */
    @Override
    public int partition(String s, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return Math.abs(new String(key).hashCode() % partitions.length);

    }
}
