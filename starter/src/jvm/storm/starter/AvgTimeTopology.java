package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.RandomSentenceSpout;
import storm.starter.spout.SendTcpFlowDataSpout;
import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class AvgTimeTopology {
	

  /*
   * 这个bolt用来把spout传递过来的字符串数据按照tab分割转换成数组，传递给后续的bolt进行分析
   */
  public static class StringToArrayBolt extends BaseBasicBolt  {

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	      String word = tuple.getString(0);
	      String[] words = word.split("	");
	      
	      collector.emit(new Values(words[16],words[6],words[0]));
	      
	      
	}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("http_req_host","tcp_syn_rtt","report_timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class CalcAvgTimeBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
  


  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new SendTcpFlowDataSpout(), 5);

    builder.setBolt("StringToArray", new StringToArrayBolt(), 8).shuffleGrouping("spout");
    //builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);
      cluster.shutdown();
    }
}
}
