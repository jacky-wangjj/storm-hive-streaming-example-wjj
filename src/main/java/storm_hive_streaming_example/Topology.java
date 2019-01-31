/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm_hive_streaming_example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

/**
 *
 * @author hkropp
 */
public class Topology {

    public static final String KAFKA_SPOUT_ID = "kafka-spout";
    public static final String STOCK_PROCESS_BOLT_ID = "stock-process-bolt";
    public static final String HIVE_BOLT_ID = "hive-stock-price-bolt";

    public static void main(String... args) {
        Topology app = new Topology();
        app.run(args);
    }
    
    public void run(String... args){
        String kafkaTopic = "stock_topic";

        String zkhost = "10.110.181.39:2181,10.110.181.40:2181,10.110.181.41:2181";
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(zkhost),
                kafkaTopic, "/kafka_storm", "StormSpout");
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
        spoutConfig.startOffsetTime = System.currentTimeMillis();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        
        // Hive connection configuration
        String metaStoreURI = "thrift://10.110.181.40:9083";
        String dbName = "default";
        String tblName = "stock_prices";
        // Fields for possible partition
        String[] partNames = {"name"};
        // Fields for possible column data
        String[] colNames = {"day", "open", "high", "low", "close", "volume","adj_close"};
        // Record Writer configuration
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames))
                .withPartitionFields(new Fields(partNames));

        HiveOptions hiveOptions;
        hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper)
                .withTxnsPerBatch(2)
                .withBatchSize(100)
                .withIdleTimeout(10)
                .withCallTimeout(10000000);
                //.withKerberosKeytab(path_to_keytab)
                //.withKerberosPrincipal(krb_principal);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
        builder.setBolt(STOCK_PROCESS_BOLT_ID, new StockDataBolt()).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(HIVE_BOLT_ID, new HiveBolt(hiveOptions)).shuffleGrouping(STOCK_PROCESS_BOLT_ID);
        
        String topologyName = "StormHiveStreamingTopo";
        Config config = new Config();
        config.setNumWorkers(1);
        config.setMessageTimeoutSecs(60);
        try {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
