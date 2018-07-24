package com.myron.storm.wordcount.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


/**
 * <ul>
 * <li>spout /spaut/ 中文解释 (喷涌的)"水柱"  </li>
 * <li>spoutd代表了一个Storm topology的主要数据入口,充当采集器的角色,连接到数据源,将数据转换为一个个tuple,
 *   作为数据流发射</li>
 * <li>主要工作:编写到代码从 数据源或API消费数据</li>
 * <li>BaseRichSpout 是ISpout接口和IComponet接口的简单实现</li>
 * </ul>
 * @author Administrator
 *
 */
public class SentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private Integer index = 0;
	
	//模拟数据源,每句话作为单值得 tuple相后循环发射
	private String[] sentences = {
		"my dog has fleas",
		"i like cold beverages",
		"the dog ate my homework",
		"don't have a cow man",
		"i don't think i like fleas"
	};
	
	/**
	 * nextTuple()方法是所有spout实现的核心所在
	 * 向输出的collector发射tuple
	 */
	@Override
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index].trim().toLowerCase()));
		//sentence "my dog has fleas"
		//sentence "i like cold beverages"
		
		//递增索引指向下一个语句
		index++;
		//重复静态语句模拟数据源
		if (index >= this.sentences.length) {
			index = 0;
		}		
	}

	/**
	 * <ul>
	 * <li>说明:ISpout接口定义,Spout组件初始化调用</li>
	 * <li>config 配置信息map</li>
	 * <li>TopologyContext 提供topology中组件的信息, TopologyContext提供 发射tuple的方法</li>
	 * </ul>
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector colector) {
		this.collector = colector;
		
	}

	/**
	 * <ul>
	 * <li>说明：declareOutputFields 是IComponent接口中定义的,所有storm组件(spout/bolt)都必须实现</li>
	 * <li>作用：告诉storm 该组件会发射哪些数据流,每个数据流的tuple包含哪些字段</li>
	 * </ul>
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	
	}

}
