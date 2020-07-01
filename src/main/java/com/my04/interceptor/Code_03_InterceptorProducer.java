package com.my04.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Auther wu
 * @Date 2019/7/18  0:44
 */
public class Code_03_InterceptorProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //构建拦截器链
        ArrayList<String> interceptors = new ArrayList<String>();
        interceptors.add("com.my04.interceptor.Code_01_TimeInterceptor");
        interceptors.add("com.my04.interceptor.Code_02_CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        //创建生产者对象，用来发送数据
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 6; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", "message" + i);
            producer.send(record);
        }

        //一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }
}
