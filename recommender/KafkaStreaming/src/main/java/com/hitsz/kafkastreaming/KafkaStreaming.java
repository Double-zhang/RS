package com.hitsz.kafkastreaming;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;


public class KafkaStreaming {

    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";

        // 定义输入和输出的topic
        String from = "log";  //flume直接采集日志文件，发送到kafka中的log分区
        String to = "recommender";

        // 定义kafka stream 配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        // 创建kafka stream 配置对象
        StreamsConfig config = new StreamsConfig(settings);

        // 定义拓扑构建器
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", from)  //得到数据
                .addProcessor("PROCESSOR", ()->new LogProcessor(), "SOURCE")  //进行处理
                .addSink("SINK", to, "PROCESSOR");  //进行输出

        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams( builder, config );

        streams.start();
        System.out.println("kafka stream started!");
    }
}
