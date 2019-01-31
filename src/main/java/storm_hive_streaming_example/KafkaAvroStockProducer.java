package storm_hive_streaming_example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;

import org.apache.kafka.clients.producer.*;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.StringSerializer;
import storm_hive_streaming_example.model.Stock;

/**
 * Created by hkropp on 23/09/15.
 */
public class KafkaAvroStockProducer {
    public static void main(String... args) throws IOException, InterruptedException {
        // Kafka Properties
        Properties props = new Properties();
        // HDP uses 6667 as the broker port. Sometimes the binding is not resolved as expected, therefor this list.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.110.181.41:6667");//kafka地址
        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");//是否等待kafka的响应；0为不等待，1等待本机，all等待kafka集群同步完成
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "1");//消息发送失败后重新发送次数
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//键序列化
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//值序列化
        // Topic Name
        String topic = "stock_topic";

        // Create Kafka Producer
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

        // Read Data from File Line by Line
        List<String> stockPrices = IOUtils.readLines(
                                                    KafkaStockProducer.class.getResourceAsStream("stock.csv"),
                                                    Charset.forName("UTF-8")
        );
        // Send Each Line to Kafka Producer and Sleep
        for(String line : stockPrices){
            System.out.println(line);
            if(line.startsWith("Date")) continue;
            String[] stockData = line.split(",");
            // Date,Open,High,Low,Close,Volume,Adj Close,Name
            Stock stock = new Stock();
            stock.setDate(stockData[0]);
            stock.setOpen(Float.parseFloat(stockData[1]));
            stock.setHigh(Float.parseFloat(stockData[2]));
            stock.setLow(Float.parseFloat(stockData[3]));
            stock.setClose(Float.parseFloat(stockData[4]));
            stock.setVolume(Integer.parseInt(stockData[5]));
            stock.setAdjClose(Float.parseFloat(stockData[6]));
            stock.setName(stockData[7]);
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, serialize(stock));
            try {
                producer.send(record).get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            Thread.sleep(50L);
        }
        producer.close();
    }

    public static <T extends SpecificRecordBase> byte[] serialize(T record) throws IOException {
        try {
            DatumWriter<T> writer = new SpecificDatumWriter<T>(record.getSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            IOUtils.closeQuietly(out);
            return out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
