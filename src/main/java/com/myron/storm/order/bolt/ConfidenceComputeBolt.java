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

import redis.clients.jedis.Jedis;

import com.myron.storm.order.common.ConfKeys;
import com.myron.storm.order.common.FieldNames;
import com.myron.storm.order.common.ItemPair;

/**
 * 计算置信度
 * @author Administrator
 *
 */
public class ConfidenceComputeBolt extends BaseRichBolt {
	private static final long serialVersionUID = -253730965401483851L;
	private OutputCollector collector;
	private Jedis jedis;
	private String host;
	private int port;	
	private Map<ItemPair, Integer> pairCounts;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		//从storm属性配置文件中读取redis连接属性
		this.host = stormConf.get(ConfKeys.REDSI_HOST).toString();
		this.port = Integer.parseInt(stormConf.get(ConfKeys.REDSI_PORT).toString());
		this.pairCounts = new HashMap<>();
		connectToRedis();
	}



	@Override
	public void execute(Tuple tuple) {
		// 根据元组的字段做出不同的响应
		if (tuple.getFields().size() == 3) {
			String item1 = tuple.getStringByField(FieldNames.ITEM1);
			String item2 = tuple.getStringByField(FieldNames.ITEM2);
			Integer pairCount = tuple.getIntegerByField(FieldNames.PAIR_COUNT);
			this.pairCounts.put(new ItemPair(item1, item2), pairCount);
		} else if (FieldNames.COMMAND.equals(tuple.getFields().get(0))) {
			//商品组合出现的次数 除以商品出现的次数
			for (ItemPair itemPair: this.pairCounts.keySet()) {
				int item1Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem1()));
				int item2Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem2()));
				double itemConfidence = pairCounts.get(itemPair).intValue();
				
				//这里求置信度 a/b发生情况下， ab事件发生的概率
				if ( item1Count < item2Count) {
					itemConfidence /= item1Count;
				} else {
					itemConfidence /= item2Count;
				}
				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemConfidence));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.CONFIDENCE));

	}

	private void connectToRedis() {
		this.jedis= new Jedis(this.host, this.port);
		this.jedis.connect();
		
	}
}
