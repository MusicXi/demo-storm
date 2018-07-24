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
 * 计算每种商品组合出现的次数
 * @author Administrator
 *
 */
public class PairCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 5892138075224572121L;
	private OutputCollector collector;
	private Map<ItemPair, Integer> pairCounts;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		pairCounts = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple) {
		String item1 = tuple.getStringByField(FieldNames.ITEM1);
		String item2 = tuple.getStringByField(FieldNames.ITEM2);
		ItemPair itemPair = new ItemPair(item1, item2);
		int pairCount = 0;
		if (this.pairCounts.containsKey(itemPair)) {
			pairCount = this.pairCounts.get(itemPair);
		}
		pairCount ++;
		pairCounts.put(itemPair, pairCount);
		this.collector.emit(new Values(item1, item2, pairCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.PAIR_COUNT));
	}

}
