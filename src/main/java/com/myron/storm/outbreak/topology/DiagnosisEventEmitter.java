package com.myron.storm.outbreak.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;

public class DiagnosisEventEmitter implements Emitter<Long>, Serializable {

	private static final long serialVersionUID = 1L;
	private AtomicInteger successfulTransactions = new AtomicInteger(0);
	
	@Override
	public void emitBatch(TransactionAttempt tx, Long coordinatorMeta,
			TridentCollector collector) {
		// 本例使用随机分配一个经纬度,大体保持美国范围内
		for (int i = 0; i < 1000; i++) {
			List<Object> events  = new ArrayList<Object>();
			double lat = new Double(-30 + (int) (Math.random() * 75));
			double lng = new Double(-30 + (int) (Math.random() * 75));
			long time = System.currentTimeMillis();
			// 模拟疾病代码
			String diag = new Integer(320 + (int) (Math.random() * 7))
					.toString();
			DiagnosisEvent event = new DiagnosisEvent(lat, lng, time, diag);
			events.add(event);
			collector.emit(events);
		}
		
	}

	@Override
	public void success(TransactionAttempt tx) {
		successfulTransactions.incrementAndGet();
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
