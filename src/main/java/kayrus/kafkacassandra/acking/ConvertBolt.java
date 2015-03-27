package kayrus.kafkacassandra.acking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.io.UnsupportedEncodingException;

import org.json.JSONObject;
import org.json.JSONArray;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ConvertBolt extends BaseRichBolt {

  private static final long serialVersionUID = 6102304822420418016L;
  
  private Map<String, Long> counts;
  private OutputCollector collector;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector outCollector) {
    collector = outCollector;
//    counts = new HashMap<String, Long>();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("timestamp","id", "P_1", "P_2", "P_3", "Q_1", "Q_2", "Q_3"));
  }

  @Override
  public void execute(Tuple tuple) {
//    byte[] valueBytes = (byte[]) tuple.getValueByField("bytes");
    Object value = tuple.getValue(0);
//    Tuple tup = '{}: {}'.format(*tuple)

    String sentence = null;
    if (value instanceof String) {
      sentence = (String) value;
//      System.out.println("1");
    } else {
      // Kafka returns bytes
      byte[] bytes = (byte[]) value;
      try {
        sentence = new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
//      System.out.println("2");
    }

//    System.out.println("hello_world: " + sentence);


    JSONObject jObj = new JSONObject(sentence);
//    JSONArray the_json_array = jObj.getJSONArray("");
//    System.out.println("hello_world: " + jObj.getString("id"));
    long timestamp = jObj.getLong("timestamp");
//    String id = jObj.getString("id");
    UUID id = UUID.fromString(jObj.getString("id"));
    double P_1 = jObj.getDouble("P_1");
    double P_2 = jObj.getDouble("P_2");
    double P_3 = jObj.getDouble("P_3");
    double Q_1 = jObj.getDouble("Q_1");
    double Q_2 = jObj.getDouble("Q_2");
    double Q_3 = jObj.getDouble("Q_3");

//    String word = tuple.getString(0);
//    Long count = tuple.getLong(1);

//    counts.put(word, count);


    collector.emit(tuple, new Values(timestamp, id, P_1, P_2, P_3, Q_1, Q_2, Q_3));


    collector.ack(tuple);
  }
}
