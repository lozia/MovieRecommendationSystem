package edu.neu.csye7200.kafkastream;



import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import java.util.Properties;


public class Application {
    public static void main(String[] args){
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";
        // define the topic of input and output
        String from = "log";
        String to = "recommender";
        // Define the configuration of Kafka streaming
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);
        //Create kafkastream configuration object
        StreamsConfig config = new StreamsConfig(settings);
        // Topology builder
        TopologyBuilder builder = new TopologyBuilder();
        // Define the topology of stream processing
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        System.out.println("Kafka stream started!>>>>>>>>>>>");
    }
}