/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm_hive_streaming_example;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author hkropp
 */
public class KafkaStockProducer {

    public static void main(String... args) throws IOException, InterruptedException {
        // Kafka Properties
        Properties props = new Properties();
        // HDP uses 6667 as the broker port. Sometimes the binding is not resolved as expected, therefor this list.
        props.setProperty(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.110.181.41:6667");//kafka地址
        props.setProperty(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "0");//是否等待kafka的响应；0为不等待，1等待本机，all等待kafka集群同步完成
        props.setProperty(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, "1");//消息发送失败后重新发送次数
        props.setProperty(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//键序列化
        props.setProperty(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//值序列化
        
        // Topic Name
        String topic = "stock_topic";
        
        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        
        // Read Data from File Line by Line
        List<String> stockPrices = IOUtils.readLines(
                KafkaStockProducer.class.getResourceAsStream("stock.csv"),
                Charset.forName("UTF-8")
        );
        // Send Each Line to Kafka Producer and Sleep
        for(String line : stockPrices){
            System.out.println(line);
            if(line.startsWith("Date")) continue;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, line);
            try {
                producer.send(record).get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            Thread.sleep(50L);
        }
        producer.close();
    }

}
