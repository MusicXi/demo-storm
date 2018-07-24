package com.myron.storm.wordcount.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private HashMap<String, Long> counts = null;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.counts.put(word, count);
	}

	/**
	 * ReportBolt位于末端的Bolt,只接受tuple,不发射任何数据流
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit anything
		
	}

	/**
	 * Storm终止一个bolt之前会调用这个方法,通常cleanup()方法用来释放bolt占用的资源
	 * 如打开文件句柄或者数据库连接
	 * 注意：IBolt.cleanup()不可靠,不能保证执行
	 */
	@Override
	public void cleanup() {
		System.out.println("----FINAL COUNT----");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.counts.get(key));
		}
		System.out.println("------------------");
	}
	
	

}
