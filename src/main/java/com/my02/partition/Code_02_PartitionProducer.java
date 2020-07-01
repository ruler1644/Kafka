package com.my02.partition;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Code_02_PartitionProducer {
    public static void main(String[] args) {


        Properties pros = new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        pros.put(ProducerConfig.ACKS_CONFIG, "all");
        pros.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pros.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //关联自定义的分区器
        pros.put("partitioner.class", "com.partition.Code_01_MyPartitioner");

        //创建生产者
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(pros);

        //循环发送数据
        for (int i = 0; i < 10; i++) {
            ProducerRecord producerRecord = new ProducerRecord("first", "data : " + Integer.toString(i));
            producer.send(producerRecord, new Callback() {

                @Override
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
