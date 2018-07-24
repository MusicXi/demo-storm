package com.myron.storm.order.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.myron.storm.order.common.FieldNames;

/**
 * <订单，商品>数据流中，拆分订单中所有商品的组合,发送<商品1,商品2>
 * @author Administrator
 *
 */
public class SplitBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Map<String, List<String>> orderItems; //订单 - 商品列表的映射关系
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.orderItems = new HashMap<>();

	}

	@Override
	public void execute(Tuple tuple) {
		String id = tuple.getStringByField(FieldNames.ID);
		String newItem = tuple.getStringByField(FieldNames.NAME);
		//初次获取到该订单,创建商品集合并添加商品
		if (! this.orderItems.containsKey(id)) {
			List<String> items = new ArrayList<String>();
			items.add(newItem);
			this.orderItems.put(id, items);
			return;
		}
		
		List<String> items = this.orderItems.get(id);
		//发送订单中所有可能的商品组合
		for (String existItem : items) {
			this.collector.emit(this.createPair(newItem, existItem));
		}
		//新的商品加入到订单的商品集合中
		items.add(newItem);
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2));

	}

	/**
	 * 商品名称的字典顺序作为商品组合的顺序,这样订单中商品出现的顺序不会影响组合最终结果
	 * @param item1
	 * @param item2
	 * @return
	 */
	private List<Object> createPair(String item1, String item2) {
		if (item1.compareTo(item2) > 0) { //字典顺序大作为商品组合的第一项
			return new Values(item1, item2);
		}
		return new Values(item2, item1);
	}

}
