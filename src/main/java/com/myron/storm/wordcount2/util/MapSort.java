package com.myron.storm.wordcount2.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 对map排序的工具类
 * 
 * @author sxf
 *
 */
public class MapSort {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Map<String, Integer> sortByValue(Map<String, Integer> map) {

		if (map == null) {
			return null;
		}

		List list = new LinkedList(map.entrySet());
		// List list = new ArrayList(map.entrySet());
		// Java集合框架使用 Collections类中的静态工具方法来排序、查找和打乱线性表、及查找最大元素和最小
		Collections.sort(list, new Comparator() {

			public int compare(Object o1, Object o2) {
				Comparable sort1 = (Comparable) ((Map.Entry) o1).getValue();
				Comparable sort2 = (Comparable) ((Map.Entry) o2).getValue();
				return sort2.compareTo(sort1);
			}

		});

		Map result = new LinkedHashMap();

		for (Iterator it = list.iterator(); it.hasNext();) {

			Map.Entry entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());

		}

		return result;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue2(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		
		return result;
	}

	public static void main(String[] args) {

		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("test", 3);
		map.put("hcy", 12);
		map.put("put", 24);
		map.put("ruw", 2);
		map.put("jkx", 21);

		map = sortByValue(map);
		//map = sortByValue2(map);
		
		for (String key : map.keySet()) {
			System.out.println(key + " ==> " + map.get(key));
		}
	}

}