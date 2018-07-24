package com.myron.storm.order.bolt;

import java.util.Map;

import org.apache.storm.shade.org.json.simple.JSONObject;
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
 * 过滤器 过滤超过一定阈值的支持度和置信度
 * @author Administrator
 *
 */
public class FilterBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8976170132525186877L;
	private static final double SUPPORT_THRESHOLD = 0.01;
	private static final double CONFIDENCE_THRESHOLD = 0.01;
	
	private OutputCollector collector;
	private String host;
	private int port;	
	private Jedis jedis; //过滤结果输出到redis数据库
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		//从storm属性配置文件中读取redis连接属性
		this.host = stormConf.get(ConfKeys.REDSI_HOST).toString();
		this.port = Integer.parseInt(stormConf.get(ConfKeys.REDSI_PORT).toString());
		connectToRedis();

	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
		String item1 = tuple.getStringByField(FieldNames.ITEM1);
		String item2 = tuple.getStringByField(FieldNames.ITEM2);
		ItemPair itemPair = new ItemPair(item1, item2);
		double support = 0;
		double confidence = 0;
		//对商品组合的支持度和置信度持久化操作
		if (FieldNames.SUPPORT.equals(tuple.getFields().get(2).toString())) {
			support = tuple.getDoubleByField(FieldNames.SUPPORT);
			jedis.hset("supports", itemPair.toString(), String.valueOf(support));
		} else if (FieldNames.CONFIDENCE.equals(tuple.getFields().get(2).toString())) {
			confidence = tuple.getDoubleByField(FieldNames.CONFIDENCE);
			jedis.hset("confidences", itemPair.toString(), String.valueOf(confidence));
		}
	
		if (! isSupportAndConfidenceOk(itemPair)) {
			return;
		}
		
		support = Double.parseDouble(jedis.hget("supports", itemPair.toString()));
		confidence = Double.parseDouble(jedis.hget("confidences", itemPair.toString()));
		if (support >= SUPPORT_THRESHOLD && confidence >= CONFIDENCE_THRESHOLD) {
			JSONObject pairValue = new JSONObject();
			pairValue.put(FieldNames.SUPPORT, support);
			pairValue.put(FieldNames.CONFIDENCE, support);
			//记录推荐商品的组合
			jedis.hset("recommendedPair", itemPair.toString(), pairValue.toJSONString());
			//为了方便其他bolt处理节点 可能要使用该信息,依然将元组信息发送出去
			this.collector.emit(new Values(item1, item2, support, confidence));
		} else {
			if (jedis.hexists("recommendedPair", itemPair.toString())) {
				jedis.hdel("recommendedPair", itemPair.toString());				
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.SUPPORT, FieldNames.CONFIDENCE));

	}

	private void connectToRedis() {
		this.jedis= new Jedis(this.host, this.port);
		this.jedis.connect();
	}
	
	/**
	 * 判断某商品组合的支持度和置信度数据是否记录记录成功
	 * @param itemPair
	 * @return
	 */
	private boolean isSupportAndConfidenceOk(ItemPair itemPair) {
		return jedis.hexists("supports", itemPair.toString()) 
					&& jedis.hexists("confidences", itemPair.toString());
				
	}
}
