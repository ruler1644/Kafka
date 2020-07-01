package com.my03_comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Auther wu
 * @Date 2019/7/17  10:07
 */

//手动提交offset，同步的方式
public class Code_02_consumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "atguigu");

        //关闭自动提交offset
        props.put("enable.auto.commit", "false");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(props);

        consumer.subscribe(Arrays.asList("first"));

        while (true) {

            //消费消息
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset());
                System.out.println(record.key());
                System.out.println(record.value());
            }

            //同步提交，当前线程会阻塞直到offset提交成功
            consumer.commitSync();
        }
    }
}