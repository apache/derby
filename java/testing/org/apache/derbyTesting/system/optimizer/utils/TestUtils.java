/*
 
 Derby - Class org.apache.derbyTesting.system.langtest.utils.TestUtils
 
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 
 */
package org.apache.derbyTesting.system.optimizer.utils;
/**
 * 
 * Class TestUtils: Utility class for measuring query times
 *
 */
public class TestUtils {
	static int MILLISECONDS_IN_SEC=1000;
	static int SECONDS_IN_MIN=60;
	static int MINUTES_IN_HR=60;
	
	public static String getTime(long timeInMs)
	{
		StringBuffer stringBuff = new StringBuffer(32);
		//get Hours
		int hours = (int)timeInMs /( MINUTES_IN_HR * SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		if (hours > 0) {
			stringBuff.append(hours);
			stringBuff.append(" hr");
		}
		//get Minutes
		int remainHours = (int)timeInMs % (MINUTES_IN_HR * SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		int minutes = remainHours / (SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		if (minutes > 0) {
			stringBuff.append(minutes);
			stringBuff.append(" min ");
		}
		//get Seconds
		int remainMinutes = remainHours % (SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		int seconds = remainMinutes / MILLISECONDS_IN_SEC;
		int milliseconds = remainMinutes % MILLISECONDS_IN_SEC;

		stringBuff.append(seconds);
		if (hours == 0 && minutes < 5)
		{
			stringBuff.append('.');
			if (milliseconds < 10)
				stringBuff.append('0');
			if (milliseconds < 100)
				stringBuff.append('0');
			stringBuff.append(milliseconds);
		}
		stringBuff.append(" secs ");
		return stringBuff.toString();
	
	}

}
