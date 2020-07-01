package com.my01_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

//同步发送API
public class Code_03_producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

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
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 5; i < 10; i++) {

            ProducerRecord record = new ProducerRecord("first", Integer.toString(i));
            Future future = producer.send(record);

            //用到Future对象实现同步，get处理完之前，会阻塞其它线程，类似于join
            //kafka只保证每个分区数据有序
            //使用一个分区 + 同步机制，来保证全局有序，但是效率太低下了
            future.get();

        }
        producer.close();
    }
}
