package com.myron.storm.outbreak.topology;

import org.apache.storm.trident.state.map.NonTransactionalMap;

/**
 * 生成环境需要传入 IBackingMap持久层的实现
 * @author Administrator
 *
 */
public class OutbreakTrendState extends NonTransactionalMap<Long>{

	protected OutbreakTrendState(OutbreakTrendBackingMap backing) {
		super(backing);
		// TODO Auto-generated constructor stub
	}

}
