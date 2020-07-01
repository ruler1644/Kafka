package com.my03_comsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

//自定义储存offset
public class Code_04_consumer {

    private static Map<TopicPartition, Long> currentOffset = new HashMap<TopicPartition, Long>();
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "atguigu");

        //关闭自动提交offset
        props.put("enable.auto.commit", "false");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);

        //消费者订阅主题，监听Rebalance
        consumer.subscribe(Arrays.asList("zhangsan"), new ConsumerRebalanceListener() {

            //该方法会在Rebalance之前调用，正常提交offset
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            //该方法会在Rebalance之后调用
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {

                    //定位到最近提交的offset位置继续消费，维护offset
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(
                        record.topic(), record.partition()), record.offset());
            }

            commitOffset(currentOffset);
        }

    }

    //获取某分区的最新offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    //提交该消费者所有分区的offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
