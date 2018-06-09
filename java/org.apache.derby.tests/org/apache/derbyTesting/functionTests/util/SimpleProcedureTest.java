/*

Derby - Class org.apache.derbyTesting.functionTests.util.SimpleProcedureTest

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
package org.apache.derbyTesting.functionTests.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Procedures to be used with J2ME/CDC/FP and JSR169
 */

public class SimpleProcedureTest {

	/*
	** Procedures for parameter mapping testing.
	*/

	public static void pmap(short in, short[] inout, short[] out) {

		inout[0] += 6;
		out[0] = 77;
	}
	public static void pmap(int in, int[] inout, int[] out) {
		inout[0] += 9;
		out[0] = 88;

	}
	public static void pmap(long in, long[] inout, long[] out) {
		inout[0] += 8;
		out[0] = 99;
	}
	public static void pmap(float in, float[] inout, float[] out) {
		inout[0] += 9.9f;
		out[0] = 88.8f;
	}
	public static void pmap(double in, double[] inout, double[] out) {
		inout[0] += 3.9;
		out[0] = 66.8;
	}
	public static void pmap(byte[] in, byte[][] inout, byte[][] out) {

		inout[0][2] = 0x56;
		out[0] = new byte[4];
		out[0][0] = (byte) 0x09;
		out[0][1] = (byte) 0xfe;
		out[0][2] = (byte) 0xed;
		out[0][3] = (byte) 0x02;

	}
	public static void pmap(Date in, Date[] inout, Date[] out) {

		inout[0] = java.sql.Date.valueOf("2004-03-08");
		out[0] = java.sql.Date.valueOf("2005-03-08");

	}
	public static void pmap(Time in, Time[] inout, Time[] out) {
		inout[0] = java.sql.Time.valueOf("19:44:42");
		out[0] = java.sql.Time.valueOf("20:44:42");
	}
	public static void pmap(Timestamp in, Timestamp[] inout, Timestamp[] out) {

		inout[0] = java.sql.Timestamp.valueOf("2004-03-12 21:14:24.938222433");
		out[0] = java.sql.Timestamp.valueOf("2004-04-12 04:25:26.462983731");
	}
	public static void pmap(String in, String[] inout, String[] out) {
		inout[0] = inout[0].trim().concat("P2-PMAP");
		out[0] = "P3-PMAP";
	}
	
}
