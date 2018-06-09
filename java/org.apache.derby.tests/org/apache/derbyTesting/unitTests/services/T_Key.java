/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_Key

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

package org.apache.derbyTesting.unitTests.services;

/**

	Key for these objects is an array of objects

	value - Integer or String - implies what object should be used in the cache.
	waitms - time to wait in ms on a set or create (simulates the object being loaded into the cache).
	canFind - true of the object can be found on a set, false if it can't. (simulates a request for a non-existent object)
	raiseException - true if an exception should be raised during set or create identity


*/
public class T_Key  {

	private Object	value;
	private long		waitms;
	private boolean   canFind;
	private boolean   raiseException;

	public static T_Key		simpleInt(int value) {
		return new T_Key(value, 0, true, false);
	}
	public static T_Key		dontFindInt(int value) {
		return new T_Key(value, 0, false, false);
	}
	public static T_Key		exceptionInt(int value) {
		return new T_Key(value, 0, true, true);
	}
	
	/**
		48%/48%/4% chance of Int/String/invalid key
		90%/5%/5% chance of can find / can't find / raise exception
	*/
	public static T_Key randomKey() {

		double rand = Math.random();
		T_Key tkey = new T_Key();

		if (rand < 0.48)
			tkey.value = (int) (100.0 * rand);
		else if (rand < 0.96)
			tkey.value = (int) (100.0 * rand);
		else
			tkey.value = Boolean.FALSE;

		rand = Math.random();

		if (rand < 0.90)
			tkey.canFind = true;
		else if (rand < 0.95)
			tkey.canFind = false;
		else {
			tkey.canFind = true;
			tkey.raiseException = false;
		}

		rand = Math.random();

		if (rand < 0.30) {
			tkey.waitms = (long) (rand * 1000.0); // Range 0 - 0.3 secs
		}

		return tkey;
	}

	private T_Key() {
	}


	private T_Key(Object value, long waitms, boolean canFind, boolean raiseException) {

		this.value = value;
		this.waitms = waitms;
		this.canFind = canFind;
		this.raiseException = raiseException;
	}

	public Object getValue() {
		return value;
	}

	public long getWait() {
		return waitms;
	}

	public boolean canFind() {
		return canFind;
	}

	public boolean raiseException() {
		return raiseException;
	}

	public boolean equals(Object other) {
		if (other instanceof T_Key) {
			return value.equals(((T_Key) other).value);
		}
		return false;
	}

	public int hashCode() {
		return value.hashCode();
	}

	public String toString() {
		return value + " " + waitms + " " + canFind + " " + raiseException;
	}
}

