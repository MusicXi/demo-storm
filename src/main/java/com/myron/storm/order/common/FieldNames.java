package com.myron.storm.order.common;

/**
 * 订单数据流字段属性
 * @author Administrator
 *
 */
public class FieldNames {
	public static final String ID = "id";//订单id
	public static final String ITEMS = "items";//订单商品列表
	public static final String NAME = "name";//商品名称
	public static final String COUNT = "count";//商品数量

	public static final String ITEM1 = "item1";//商品组合的第一项
	public static final String ITEM2 = "item2";//商品组合的第二项
	
	public static final String PAIR_COUNT = "pair_count";//每个商品组合的出现次数
	public static final String TOTAL_COUNT = "total_count";//商品二项组合总数
	
	public static final String SUPPORT = "support";//组合的支持度
	public static final String CONFIDENCE = "confidence";//组合的置信度
	
	public static final String COMMAND = "command";//命令字段
	
	
}
