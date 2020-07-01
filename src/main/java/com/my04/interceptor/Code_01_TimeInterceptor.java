package com.my04.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Auther wu
 * @Date 2019/7/18  0:24
 */

//时间拦截器，在每条消息内容前拼接上时间戳
public class Code_01_TimeInterceptor implements ProducerInterceptor<String, String> {

    public void configure(Map<String, ?> configs) {

    }

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        long timeMillis = System.currentTimeMillis();

        //创建一个新的ProducerRecord，并在每条消息内容前拼接上时间戳
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>(
                        record.topic(),
                        record.partition(),
                        record.key(),
                        timeMillis + "," + record.value()
                );
        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }
}
