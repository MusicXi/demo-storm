package com.myron.storm.wordcount.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
/**
 * <ul>
 * 		<li>bolt /bəult/ 中文释义:闪电</li>
 * 		<li>bolt可以理解为计算程序中的运算或者函数,将一个或者多个数据流作为输入,对数据实施运算后，选择性的输出一个或者多个数据流</li>
 * 		<li>bolt可以订阅多个由spout或者其他的bolt发射的数据流,可以建立复杂的数据流网络</li>
 * 		<li>bolt的功能:
 * 			<ul>
 *				<li>过滤tuple</li>
 *				<li>连接(join)和聚合操作(aggregation)</li>
 *				<li>计算</li>
 *				<li>数据库读写</li>
 *			</ul>
 * 		</li>
 * </ul>
 * @author Administrator
 *
 */
public class SplitSentenceBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	/**
	 * <ul>
	 * <li>说明:IBolt接口定义,bolt组件初始化调用,可以用来准备bolt用到的资源，比如数据库连接</li>
	 * <li>config 配置信息map</li>
	 * <li>TopologyContext 提供topology中组件的信息, TopologyContext提供 发射tuple的方法</li>
	 * </ul>
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	/**
	 * <ul>
	 * <li>说明:ISpout接口定义,SplitSentenceBolt核心功能,每当从订阅数据流中接受一个tuple,都会调用</li>
	 * </ul>
	 */
	@Override
	public void execute(Tuple tuple) {
		//sentence "my dog has fleas"
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		//每个单词都向后面的输出流发射一个tuple
		for (String  word : words) {
			this.collector.emit(new Values(word));
			//word my
			//word dog
			//word has
			//word fleas
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//声明一个输出流
		declarer.declare(new Fields("word"));
		
	}

}
