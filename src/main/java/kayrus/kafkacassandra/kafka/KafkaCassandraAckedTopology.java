package kayrus.kafkacassandra.kafka;

import java.util.Arrays;
import java.util.HashMap;

import kayrus.kafkacassandra.acking.ConvertBolt;
import kayrus.kafkacassandra.acking.ReportBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
//import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.spout.RawMultiScheme;

//cassandra classes
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;

public class KafkaCassandraAckedTopology {

  private static final String KAFKA_SPOUT_ID = "kafka-sentence-spout";
  private static final String REPORT_BOLT_ID = "acking-report-bolt";
  private static final String CONVERT_BOLT_ID = "convert-bolt";
  private static final String TOPOLOGY_NAME = "acking-kafka-cassandra-topology";
  private static final String CASSANDRA_BOLT = "WORD_COUNT_CASSANDRA_BOLT";

  public static void main(String[] args) throws Exception {
    int numSpoutExecutors = 1;

    KafkaSpout kspout = buildKafkaSentenceSpout();
    ReportBolt reportBolt = new ReportBolt();
    ConvertBolt convertBolt = new ConvertBolt();

// local run
    LocalCluster cluster = new LocalCluster();
    
    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout(KAFKA_SPOUT_ID, kspout, numSpoutExecutors);
    builder.setBolt(CONVERT_BOLT_ID, convertBolt).globalGrouping(KAFKA_SPOUT_ID);
    builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(CONVERT_BOLT_ID);

// configure cassandra 
    Config cfg = new Config();
    String configKey = "cassandra-config";
    HashMap<String, Object> clientConfig = new HashMap<String, Object>();
    clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "172.17.8.101:9160");
    clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {"testkeyspace"}));
    cfg.put(configKey, clientConfig);

// create a CassandraBolt that writes to the "stormcf" column
// family and uses the Tuple field "word" as the row key
    CassandraBatchingBolt<String, String, String> cassandraBolt = new CassandraBatchingBolt<String, String, String>(configKey, new DefaultTupleMapper("testkeyspace", "meter_data", "word"));
    cassandraBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);

//    builder.setBolt(CASSANDRA_BOLT, cassandraBolt).shuffleGrouping(REPORT_BOLT_ID);

//    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
// local run
    cluster.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
  }

  private static KafkaSpout buildKafkaSentenceSpout() {
    String zkHostPorts = "172.17.8.101:2181,172.17.8.102:2181,172.17.8.103:2181";
    String topic = "topic";

    String zkRoot = "/acking-kafka-sentence-spout";
    String zkSpoutId = "acking-sentence-spout";
    ZkHosts zkHosts = new ZkHosts(zkHostPorts);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
//    spoutCfg.scheme = new RawMultiScheme();
    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
