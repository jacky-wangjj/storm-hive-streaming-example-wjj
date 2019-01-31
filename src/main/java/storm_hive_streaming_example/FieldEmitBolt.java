package storm_hive_streaming_example;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

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
 * Created by hkropp on 25/09/15.
 */
public class FieldEmitBolt extends BaseBasicBolt {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AvroStockDataBolt.class);

    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
        ofDeclarer.declare(new Fields("day", "open", "high", "low", "close", "volume", "adj_close", "name"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        try {
            Stock stock = (Stock) tuple.getValueByField(FieldNames.STOCK_FIELD);
            Values values = new Values(
                                      stock.getDate(),
                                      stock.getOpen(),
                                      stock.getHigh(),
                                      stock.getLow(),
                                      stock.getClose(),
                                      stock.getVolume(),
                                      stock.getAdjClose(),
                                      stock.getName());
            outputCollector.emit(values);
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            throw new FailedException(ex.toString());
        }
    }
}
