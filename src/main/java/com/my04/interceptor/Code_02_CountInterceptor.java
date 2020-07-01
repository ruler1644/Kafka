package com.my04.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Auther wu
 * @Date 2019/7/18  0:25
 */

//统计发送消息成功和失败数目的计数拦截器
public class Code_02_CountInterceptor implements ProducerInterceptor<String, String> {

    private int success = 0;
    private int error = 0;

    public void configure(Map<String, ?> configs) {

    }

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        //统计成功和失败的次数
        if (metadata != null) {
            success++;
        } else {
            error++;
        }
    }

    public void close() {
        System.out.println("success:" + success);
        System.out.println("error:" + error);
    }


}
