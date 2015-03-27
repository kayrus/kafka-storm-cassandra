package kayrus.kafkacassandra.acking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    Object value = tuple.getValue(0);
    long timestamp = 0;
    if (value instanceof Long) {
      timestamp = (long) value;
    }

    value = tuple.getValue(1);
    UUID id = new UUID(0L, 0L);
    if (value instanceof UUID) {
      id = (UUID) value;
    }

    value = tuple.getValue(2);
    double P_1 = 0;
    if (value instanceof Double) {
      P_1 = (double) value;
    }

    value = tuple.getValue(3);
    double P_2 = 0;
    if (value instanceof Double) {
      P_2 = (double) value;
    }

    value = tuple.getValue(4);
    double P_3 = 0;
    if (value instanceof Double) {
      P_3 = (double) value;
    }

    value = tuple.getValue(5);
    double Q_1 = 0;
    if (value instanceof Double) {
      Q_1 = (double) value;
    }

    value = tuple.getValue(6);
    double Q_2 = 0;
    if (value instanceof Double) {
      Q_2 = (double) value;
    }

    value = tuple.getValue(7);
    double Q_3 = 0;
    if (value instanceof Double) {
      Q_3 = (double) value;
    }

    System.out.println(Long.toString(timestamp) + " " + id.toString() + " " + Double.toString(P_1) + " " + Double.toString(P_2) + " " + Double.toString(P_3) + " " + Double.toString(Q_1) + " " + Double.toString(Q_2) + " " + Double.toString(Q_3));

    collector.ack(tuple);
  }
}
