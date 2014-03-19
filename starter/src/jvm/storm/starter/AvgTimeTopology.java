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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	      String[] words = word.split(" ");
	      
	      collector.emit(new Values(words[16],words[6],words[0],words[1]));
	      
	      
	}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("http_req_host","tcp_syn_rtt","report_timestamp","report_svr_ip"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  
  
  /*
   * 用来计算延时的bolt，目前先按照ip和五分钟时间为纬度进行计算
   */
  public static class CalcAvgTimeBolt extends BaseBasicBolt {
    Map<String, Map<String,List<String>>> data_container = new HashMap<String, Map<String,List<String>>>();
    LinkedHashSet<String> timestampSet = new LinkedHashSet<String>();  
    
    //分段的时间参数，单位是s
    int sectionTime = 1;
    //间隔时间，间隔多长时间之后开始发送数据
    int waitTime = 1;
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String report_svr_ip = tuple.getString(3);
      String report_timestamp = timestampSection(tuple.getString(2),sectionTime);
      String tcp_syn_rtt = tuple.getString(1);
      
      List<String> tempListString = null;
      //判断ip是否在这个map中
      if (data_container.containsKey(report_timestamp))
      {
    	  
    	  //如果在的话，在判断对应的时间戳是不是在对应的value map中
    	  Map<String,List<String>> tempTimestampMap = data_container.get(report_svr_ip);
    	  if(tempTimestampMap.containsKey(report_svr_ip))
    	  {
    		  //如果在的话，把延时写入对应的list中去
    		  tempListString = tempTimestampMap.get(report_svr_ip);
    		  tempListString.add(tcp_syn_rtt);
    	  }
    	  else
    	  {
    		  //初始化list对象，然后把当前数据作为第一个写入进去
    		  tempListString = new ArrayList<String>();
    		  tempListString.add(tcp_syn_rtt);
    		  tempTimestampMap.put(report_svr_ip,tempListString);
    	  }
      }else
      {
    	  //如果不存在的话，初始化对应的map
    	  tempListString = new ArrayList<String>();
    	  tempListString.add(tcp_syn_rtt);

    	  Map<String,List<String>> tempTimestampMap = new HashMap<String,List<String>>();
    	  tempTimestampMap.put(report_svr_ip, tempListString);
    	  data_container.put(report_timestamp, tempTimestampMap);
    	  
      }
      
      timestampSet.add(report_timestamp);
      
      //判断当前timestampSet中的时间戳是不是在需要发射的范围里面
      Iterator<String> iterator = timestampSet.iterator();
      while (iterator.hasNext()) {
    	  String nextObj = iterator.next();
    	  int tempTime = Integer.parseInt( nextObj);
    	  
    	  
    	  int currentTime = (int) System.currentTimeMillis();

    	  //如果时间戳到当前的时间差90s的话，就开始发射
    	  if((currentTime-tempTime) >= (sectionTime+waitTime) )
    	  {
    		  collector.emit(new Values(report_timestamp, report_svr_ip));
    		  System.out.println(tempListString);
    	  }
    	  
    	  
    	  //发射之后从timestampSet和data_container删除时间戳和对应的list
    	  timestampSet.remove(nextObj);
    	  data_container.remove(report_timestamp);
      }
      
      
     
      
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("report_timestamp", "report_svr_ip"));
    }
    
    
    /*
     * 根据参数param去划分timestamp的时间区段，单位是s
     * timestamp是时间戳
     */
    private String timestampSection(String timestamp,int param)
    {
		int time = Integer.parseInt(timestamp);
		time = time - time%param;
		return Integer.toString(time);
    	
    }
  }
  


  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new SendTcpFlowDataSpout(), 5);

    //builder.setBolt("StringToArray", new StringToArrayBolt(), 8).shuffleGrouping("spout");
    //builder.setBolt("count", new CalcAvgTimeBolt(), 12).fieldsGrouping("StringToArray", new Fields("report_svr_ip"));
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
