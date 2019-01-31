/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm_hive_streaming_example;

import org.slf4j.LoggerFactory;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm_hive_streaming_example.model.Stock;

/**
 *
 * @author hkropp
 */
public class AvroStockDataBolt extends BaseBasicBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AvroStockDataBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
        ofDeclarer.declare(new Fields(FieldNames.STOCK_FIELD));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        try {
            Stock stock = (Stock) tuple.getValueByField(FieldNames.STOCK_FIELD);
            outputCollector.emit(new Values(stock));
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new FailedException(ex.toString());
        }
    }
}
