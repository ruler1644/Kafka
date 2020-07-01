package com.my01_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//异步发送消息，不带回调函数
public class Code_01_producer {
    public static void main(String[] args) {

        //kafka生产者配置信息，ProducerConfig
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者对象，用来发送数据
        Producer producer = new KafkaProducer(props);

        //循环发送数据
        for (int i = 0; i < 10; i++) {
            ProducerRecord producerRecord = new ProducerRecord("first",
                    Integer.toString(i), "data: " + Integer.toString(i));
            producer.send(producerRecord);
        }

        producer.close();
    }
}