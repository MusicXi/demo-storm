package com.myron.storm.outbreak.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.trident.state.map.IBackingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Administrator
 *
 */
public class OutbreakTrendBackingMap implements IBackingMap<Long> {
	private static final Logger LOG = LoggerFactory.getLogger(OutbreakTrendBackingMap.class);
	private Map<String, Long> storage = new ConcurrentHashMap<String, Long>(); //模拟持久层实现保存数据
	
	@Override
	public List<Long> multiGet(List<List<Object>> keys) {
		List<Long> values = new ArrayList<Long>();
		for (List<Object>  key : keys) {
			Long value = storage.get(key.get(0));
			if (value == null) {
				values.add(new Long(0));
			} else {
				values.add(value);
			}
		}
		return values;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<Long> vals) {
		for  (int i = 0; i < keys.size(); i++) {
			LOG.info("Persisting [{}] ==> [{}]", keys.get(i).get(0), vals.get(i));
			this.storage.put((String) keys.get(i).get(0), vals.get(i));
		}
		
	}

}
