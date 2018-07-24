package com.myron.storm.order.common;

public class ItemPair {
	private String item1;
	private String item2;
	

	public ItemPair(String item1, String item2) {
		super();
		this.item1 = item1;
		this.item2 = item2;
	}
	public String getItem1() {
		return item1;
	}
	public void setItem1(String item1) {
		this.item1 = item1;
	}
	public String getItem2() {
		return item2;
	}
	public void setItem2(String item2) {
		this.item2 = item2;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((item1 == null) ? 0 : item1.hashCode());
		result = prime * result + ((item2 == null) ? 0 : item2.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemPair other = (ItemPair) obj;
		if (item1 == null) {
			if (other.item1 != null)
				return false;
		} else if (!item1.equals(other.item1))
			return false;
		if (item2 == null) {
			if (other.item2 != null)
				return false;
		} else if (!item2.equals(other.item2))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "ItemPair [item1=" + item1 + ", item2=" + item2 + "]";
	}
	
	
	
	
}
