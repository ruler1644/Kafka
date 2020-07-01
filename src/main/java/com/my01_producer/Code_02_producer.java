package com.my01_producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

//异步发送消息，带回调函数
public class Code_02_producer {
    public static void main(String[] args) {

        //kafka生产者配置信息，ProducerConfig
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者对象，用来发送数据
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 6; i++) {
            ProducerRecord record = new ProducerRecord("first", Integer.toString(i));

            producer.send(record, new Callback() {

                //回调函数，在producer收到ack应答信息时调用，为异步调用
                //消息发送失败会自动重试，不需要在回调函数中手动重试
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    //消息写入成功，打印分区和offset
                    if (exception == null) {
                        System.out.println(metadata.partition() + "-->" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }
}