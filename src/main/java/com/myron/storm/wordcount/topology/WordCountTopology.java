package com.myron.storm.wordcount.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.myron.storm.wordcount.bolt.ReportBolt;
import com.myron.storm.wordcount.bolt.SplitSentenceBolt;
import com.myron.storm.wordcount.bolt.WordCountBolt;
import com.myron.storm.wordcount.spout.SentenceSpout;

/**
 * Topology定义计算所需要的spout和bolt
 * 
 * @author Administrator
 *
 */
public class WordCountTopology {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "topology-bolt";
	
	public static void main(String[] args) throws Exception {
		//实例化spout和bolt
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		//TopologyBuilder提供流式接口风格API定义topology组件之间的数据流
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		
		// SentenceSpout --> SplitSentenceBolt
		// BoltDeclarer.shuffleGrouping()：告诉Storm,sentenceSpout 发射的tuple随机均匀分发给SplitBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
		
		// SplitSentenceBolt --> WordCountBolt
		// BoltDeclarer.fieldsGrouping()：告诉Storm,所有"word"字段值的tuple被路由到同一个WordCountBolt
		builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
	
		// WordCountBolt --> ReportBolt
		// BoltDeclarer.globalGrouping()：告诉Storm,所有的WordCountBolt 路由到唯一ReportBolt任务中
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
		
		// Config对象代表了对topology所有组件全局生效的配置参数集合
		Config config = new Config();
		
		//Storm本地模式模拟一个完整的集群
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Thread.sleep(50000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}
	
}
