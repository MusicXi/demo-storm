package com.myron.storm.kafka.topology;

import java.io.Serializable;

/**
 * EWMA(Exponentially Weighted Moving Average)指数加权移动平均，是一种常用的序列数据处理方式。
 * @author Administrator
 *
 */
public class EWMA implements Serializable{

	private static final long serialVersionUID = 1L;
	// Unix load average-style alpha constants
	public static final double 	ONE_MUNUTE_ALPHA = 1 -Math.exp(-5d / 60d / 1d);
	public static final double 	FIVE_MUNUTE_ALPHA = 1 -Math.exp(-5d / 60d / 5d);
	public static final double 	FIFTEEN_MUNUTE_ALPHA = 1 -Math.exp(-5d / 60d / 5d);
	
	
	private long window;
	private long alphaWindow;
	private long last;
	private double average;
	private double alpha = -1D;
	private boolean sliding = false;
	
	public static enum Time {
		MILLISECOND(1), 
		SECONDS(1000), 
		MINUTES(SECONDS.getTime() * 60),
		HOURS(MINUTES.getTime() * 60),
		DAYS(HOURS.getTime() * 24),
		WEEKS(DAYS.getTime() * 7);
		
		private long millis;
		
		private Time(long millis) {
			this.millis = millis;
		}
		
		public long getTime() {
			return this.millis;
		}
	}
	
	public EWMA sliding(double count, Time time) {
		return this.sliding((long)(time.getTime() * count));
	}
	
	public EWMA sliding(long window) {
		this.sliding = true;
		this.window = window;
		return this;
	}

}
