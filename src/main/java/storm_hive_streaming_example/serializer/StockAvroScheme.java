package storm_hive_streaming_example.serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import com.esotericsoftware.kryo.io.Input;
import storm_hive_streaming_example.FieldNames;
import storm_hive_streaming_example.model.Stock;

/**
 * Created by hkropp on 25/09/15.
 */
public class
StockAvroScheme implements Scheme {

    private static final Logger LOG = LoggerFactory.getLogger(Stock.class);

    public List<Object> deserialize(byte[] pojoBytes) {
        StockAvroSerializer serializer = new StockAvroSerializer();
        Stock stock = serializer.read(null, new Input(pojoBytes), Stock.class);
        List<Object> values = new ArrayList<>();
        values.add(0, stock);
        return values;
    }

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        return null;
    }

    public Fields getOutputFields() {
        return new Fields(new String[]{ FieldNames.STOCK_FIELD });
    }
}
