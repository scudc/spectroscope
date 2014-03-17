package storm.starter.spout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Random;

public class SendTcpFlowDataSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;

  String[]  sentences ;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    
    
    /*读取本地文件进行数据分析*/
    sentences = new String[1389];
	String pathName = "C:\\Users\\hobodong\\workspace\\starter\\data\\report.log";
	File filename = new File(pathName);
	InputStreamReader reader = null;
	try {
		reader = new InputStreamReader(  
		        new FileInputStream(filename));
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    BufferedReader br = new BufferedReader(reader);
    String line = "";
    try {
		line = br.readLine();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
    
    int i = 0;
    sentences[i] = new String(line);
    
    while (line != null) {  
        try {
			line = br.readLine();
			sentences[++i] = line;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }  
    
  }

  @Override
  public void nextTuple() {
	  
	
    Utils.sleep(100);

    String sentence = sentences[_rand.nextInt(sentences.length)];
    
    _collector.emit(new Values(sentence));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}