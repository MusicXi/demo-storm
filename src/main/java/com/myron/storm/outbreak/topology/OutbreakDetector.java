package com.myron.storm.outbreak.topology;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * 疾病监测
 * 注意! 这个function实际上是扮演过滤器的角色,但是却作为 一个function的形式实现,
 * 因为需要在tuple中添加字段，因为filter不能改变tuple,当我们既想过滤又想添加字段时必须使用function
 * @author Administrator
 *
 */
public class OutbreakDetector extends BaseFunction {

	private static final long serialVersionUID = 1L;
	public static final int THRESHOLD = 10000;
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = (String) tuple.getValue(0);
		Long count = (Long) tuple.getValue(1);
		
		//检查计数是否超过阈值
		if (count > THRESHOLD) {
			List<Object> values = new ArrayList<>();
			values.add("Outbreak detected for [" + key + "]!");
			collector.emit(values);
		}
		
	}

}
