package com.myron.storm.order.common;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;

import redis.clients.jedis.Jedis;

public class OrderGenerator {
	private static final String REDIS_HOST = "localhost";
	private static final int REDIS_PORT = 6379;
	private static final int ORDER_COUNT = 30;
	private static Jedis jedis;
	private static  Random random;
	
	private static final String[] ITEMS_NAME = new String[] {
		"milk", "coffee", "egg", "flower", "icecream",  "wine", "water"
	};
	

	public static void main(String[] args) {
		prepareRandom();
		connectToRedis();
		pushTuples();
		disconnectFromRedis();
	}
	private static void prepareRandom() {
		random = new Random(1000);
		
	}
	private static void disconnectFromRedis() {
		jedis.disconnect();
		
	}
	@SuppressWarnings("unchecked")
	private static void pushTuples() {
		for(int i = 0; i < ORDER_COUNT; i++) {
			//订单
			JSONObject orderTuple = new JSONObject();
			//商品
			JSONArray items = new JSONArray();
			//排除订单中已选择的商品
			Set<String> selectedItems = new HashSet<String>();
			//每个订单创建四个商品
			for (int j = 0; j < 4; j++) {
				JSONObject item = new JSONObject();
				//随机选商品名称
				while (true) {
					int itemIndex = random.nextInt(ITEMS_NAME.length);
					String itemName = ITEMS_NAME[itemIndex];
					
					if (!selectedItems.contains(itemName)) {
						item.put(FieldNames.NAME, itemName);
						item.put(FieldNames.COUNT, random.nextInt(1000));
						items.add(item);
						selectedItems.add(itemName);
						break;
					}
				}
			}
			
			orderTuple.put(FieldNames.ID, UUID.randomUUID().toString());
			orderTuple.put(FieldNames.ITEMS, items);
			String jsonText = orderTuple.toJSONString();
			jedis.rpoplpush("orders", jsonText);
		}
		
	}
	private static void connectToRedis() {
		jedis = new Jedis(REDIS_HOST, REDIS_PORT);
		jedis.connect();
		
	}
}
