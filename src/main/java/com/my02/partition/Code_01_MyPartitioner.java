package com.my02.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

//自定义分区规则
public class Code_01_MyPartitioner implements Partitioner {


    //数据没有类型了，说明已经序列化了
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        //Integer count = cluster.partitionCountForTopic(topic);
        //所有数据都放到1号分区
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
