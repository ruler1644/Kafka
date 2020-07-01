package com.my03_comsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

//手动提交offset，异步的方式
public class Code_03_consumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "atguigu");

        //关闭自动提交offset
        props.put("enable.auto.commit", "false");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("first"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
            }

            //手动异步提交offset
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.out.println("offset" + offsets + " 提交失败");
                    }
                }
            });
        }
    }
}