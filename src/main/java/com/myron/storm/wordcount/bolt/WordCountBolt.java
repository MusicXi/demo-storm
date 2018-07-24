package com.myron.storm.wordcount.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	//存储单词和对应计数
	private HashMap<String, Long> counts = null;
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));

	}

}
