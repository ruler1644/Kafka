package com.my03_comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

//自动提交offset
public class Code_01_consumer {
    public static void main(String[] args) {

        //创建消费者配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "test");

        //配置自动提交offset信息，1秒提交一次
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        //反序列化key和value
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //重置消费者的offset，设置每次从头开始消费
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //消费者订阅topic
        consumer.subscribe(Arrays.asList("first", "second"));

        //循环拉取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            //遍历拉取的数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset());
                System.out.println(record.key() + "==>" + record.value());
            }
        }
    }
}
