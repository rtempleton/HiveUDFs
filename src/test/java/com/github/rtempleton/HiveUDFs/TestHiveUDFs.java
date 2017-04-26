package com.github.rtempleton.HiveUDFs;

import java.sql.Timestamp;

import org.junit.Assert;
import org.junit.Test;

public class TestHiveUDFs {
	
	private static final String testTime = "2017-04-17 16:26:13.334";

	@Test
	public void TestBucketCutoff(){
		TimeMachine udf = new TimeMachine();
		Assert.assertEquals(Timestamp.valueOf("2017-04-17 16:20:00").getTime(), udf.evaluate(Timestamp.valueOf(testTime), 5, -1).getTime());
		
		Assert.assertEquals(Timestamp.valueOf("2017-04-17 16:10:00").getTime(), udf.evaluate(Timestamp.valueOf(testTime), 5, -3).getTime());
		
		Assert.assertEquals(Timestamp.valueOf("2017-04-17 16:00:00").getTime(), udf.evaluate(Timestamp.valueOf(testTime), 5, -5).getTime());
		
		Assert.assertEquals(Timestamp.valueOf("2017-04-17 15:55:00").getTime(), udf.evaluate(Timestamp.valueOf(testTime), 5, -6).getTime());
		
		Assert.assertEquals(Timestamp.valueOf("2017-04-17 16:15:00").getTime(), udf.evaluate(Timestamp.valueOf(testTime), 15, 0).getTime());
		
		Assert.assertEquals(Timestamp.valueOf("2017-04-17 17:00:00").getTime(), udf.evaluate(Timestamp.valueOf(testTime), 20, 2).getTime());

	}

}
