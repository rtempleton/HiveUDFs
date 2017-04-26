package com.github.rtempleton.HiveUDFs;

import java.sql.Timestamp;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;
import org.joda.time.DateTime;

/**
 * 
 * @author rtempleton
 * 
 *Hive UDF for arbitrarily manipulating timestamp values realtive to the timestamp value that is passed in. 
 *
 */
public class TimeMachine extends UDF {

	public TimeMachine() {
		// TODO Auto-generated constructor stub
	}

	public TimeMachine(UDFMethodResolver rslv) {
		super(rslv);
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * 
	 * @param input - The timestamp field/value for the basis of this function
	 * @param bucketSize - number of minutes of the hour this will round off to
	 * @param bucketCount - the number of buckets in the past or future this will round off to.
	 * @return new Timestamp value
	 * 
	 * When calculating aggregations over windows of time, it's critical that the data being seelcted lands on a time boundry that represents the aggregation accurately.
	 * 
	 * Using the input timestamp value, roll the timestamp back to the most recent divisible minute marker as defined by the bucket size.
	 * From that point increase/decreate the timestamp minutes by the bucketCount times the bucketSize
	 * 
	 * Example testTime = "2017-04-17 16:26:13.334"
	 * 
	 * Zero bucket count is the current bucket
	 * (testTime, 5, 0) = "2017-04-17 16:20:00"
	 * 
	 * negative bucketCounts go back in time
	 * (testTime, 5, -6) = "2017-04-17 15:55:00"
	 * 
	 * positive bucketCounts go forward in time
	 * (testTime, 5, 2) = "2017-04-17 17:00:00"
	 * 
	 */
	public Timestamp evaluate(final Timestamp input, final int bucketSize, final int bucketCount){
		DateTime dt = new DateTime(input.getTime());
		dt = dt.withSecondOfMinute(0);
		dt = dt.withMillisOfSecond(0);
		dt = dt.plusMinutes(-(dt.getMinuteOfHour()%bucketSize) + bucketCount*bucketSize);
		
		return new Timestamp(dt.getMillis());

	}
	

}
