package com.myron.storm.order.bolt;

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
 * 计算所有商品项出现次数的总和
 * @author Administrator
 *
 */
public class PairTocalcountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 576662040885175248L;
	private OutputCollector collector;
	private int totalCount;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.totalCount = 0;
	}

	@Override
	public void execute(Tuple tuple) {
		this.totalCount ++;
		this.collector.emit(new Values(totalCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.TOTAL_COUNT));

	}

}
