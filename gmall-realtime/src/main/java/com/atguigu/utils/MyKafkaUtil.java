package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author Hereuex
 * @date 2021/2/20 16:10
 */
public class MyKafkaUtil {

    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static Properties properties = new Properties();
    private static String  DEFAULT_TOPIC= "dwd_default_topic";

    static {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    }

    /**
     * 获取KafkaSource的方法
     *
     * @param topic   主题
     * @param groupId 消费者组
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        //给配置信息对象添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //获取KafkaSource
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,5*60*1000+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

}
