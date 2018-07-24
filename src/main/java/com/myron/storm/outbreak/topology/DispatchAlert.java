package com.myron.storm.outbreak.topology;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchAlert extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DispatchAlert.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String alert = (String) tuple.getValue(0);
		LOG.error("ALERT RECEIVED [" + alert + "]");
		LOG.error("Dispatch the national guard!");
		System.exit(0);
		
		
	}

}
