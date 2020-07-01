package com.my05.kafkaSink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


//flume拦截器，配合kafka Sink，使用一个channel将Event发往不同Topic
public class Code_01_FlumeInterceptor implements Interceptor {


    private List<Event> addHeaderEvents;

    public void initialize() {
        addHeaderEvents = new ArrayList<Event>();
    }

    //单个事件拦截
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String str = new String(body);

        //据body中是否含有单词hello，添加不同的头信息，最终发往不同的Topic
        if (str.contains("hello")) {
            headers.put("topic", "first");
        } else {
            headers.put("topic", "second");
        }
        return event;
    }

    //批量事件拦截
    public List<Event> intercept(List<Event> events) {

        //清空集合
        addHeaderEvents.clear();

        for (Event event : events) {
            addHeaderEvents.add(intercept(event));
        }
        return addHeaderEvents;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new Code_01_FlumeInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
