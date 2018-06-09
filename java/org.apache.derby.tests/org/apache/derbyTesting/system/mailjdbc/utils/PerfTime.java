/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.utils.PerfTime
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *  
 */
package org.apache.derbyTesting.system.mailjdbc.utils;
//utility class that prints out the time in a formatted way
public class PerfTime {
	public static String readableTime(long t) {
		//Returns the time in h.mm.s format
		long hours = t / (60L * 60L * 1000L);
		long hoursRemainder = t % (60L * 60L * 1000L);
		long mins = hoursRemainder / (60L * 1000L);
		long minsRemainder = hoursRemainder % (60L * 1000L);
		long secs = minsRemainder / 1000L;
		long ms = minsRemainder % 1000L;

		StringBuffer sb = new StringBuffer(20);
		if (hours > 0) {
			sb.append(hours);
			sb.append('h');
		}
		if (mins > 0) {
			sb.append(mins);
			sb.append('m');
		}
		sb.append(secs);
		if (hours == 0 && mins < 5) {
			sb.append('.');
			if (ms < 10)
				sb.append('0');
			if (ms < 100)
				sb.append('0');
			sb.append(ms);
		}
		sb.append("s");
		return sb.toString();

	}
}
