package com.myron.storm.order.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

import com.myron.storm.order.common.ConfKeys;

import redis.clients.jedis.Jedis;

public class OrderSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	private Jedis jedis;
	private String host;
	private int port;
	private SpoutOutputCollector collector;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.host = conf.get(ConfKeys.REDSI_HOST).toString();
		this.port = Integer.parseInt(conf.get(ConfKeys.REDSI_HOST).toString());
		connectToRedis();
	}


	@Override
	public void nextTuple() {
		String content = this.jedis.rpop("orders");
		if (content == null || "nil".equals(content)) {
			
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	

	private void connectToRedis() {
		this.jedis = new Jedis(this.host, this.port);
		this.jedis.connect();
	}

}
