package kayrus.kafkacassandra.acking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.UnsupportedEncodingException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {

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
    // terminal bolt = does not emit anything 
  }

  @Override
  public void execute(Tuple tuple) {
//    byte[] valueBytes = (byte[]) tuple.getValueByField("bytes");
    Object value = tuple.getValue(0);
//    Tuple tup = '{}: {}'.format(*tuple)

    long sentence = 0;
    if (value instanceof Long) {
      sentence = (long) value;
      System.out.println("1");
    } else {
      System.out.println("2");
    }

    System.out.println("hello_world: " + Long.toString(sentence));

//    String word = tuple.getString(0);
//    Long count = tuple.getLong(1);

//    counts.put(word, count);
    collector.ack(tuple);
  }

  @Override
  public void cleanup() {
    System.out.println("--- FINAL COUNTS ---");
    List<String> keys = new ArrayList<String>();
    keys.addAll(counts.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      System.out.println(key + " : " + counts.get(key));
    }
    System.out.println("--------------");    
  }
}
