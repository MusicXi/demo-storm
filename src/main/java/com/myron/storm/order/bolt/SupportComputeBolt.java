package com.myron.storm.order.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.myron.storm.order.common.FieldNames;
import com.myron.storm.order.common.ItemPair;

/**
 * 频繁组合的支持度 (组合出现的频率) 注意商品组合总次在变化,特定的组合的总数也在变化
 * @author Administrator
 *
 */
public class SupportComputeBolt extends BaseRichBolt {

	private static final long serialVersionUID = 4665416512308681909L;
	private OutputCollector collector;
	
	private Map<ItemPair, Integer> pairCounts;
	private Integer pairTotalCount;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.pairCounts = new HashMap<>();
		this.pairTotalCount = 0;

	}

	@Override
	public void execute(Tuple tuple) {
		//第一个字段为组合总数  表示来自PairTocalCountBolt的数据流 
		if (FieldNames.TOTAL_COUNT.equals(tuple.getFields().get(0))) {
			this.pairTotalCount = tuple.getIntegerByField(FieldNames.TOTAL_COUNT);
		} else if (tuple.getFields().size() == 3) {
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			Integer pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);
			this.pairCounts.put(new ItemPair(item1, item2), pairCount);
		} else if (FieldNames.COMMAND.equals(tuple.getFields().get(0))) {
			//计算每个组合的支持度
			for (ItemPair itemPair: this.pairCounts.keySet()) {
				double itemSurpport = (double) this.pairCounts.get(itemPair) / this.pairTotalCount;
				//商品组合及支持度 作为元组发送
				this.collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemSurpport));
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				FieldNames.ITEM1,
				FieldNames.ITEM2,
				FieldNames.SUPPORT
		));

	}

}
