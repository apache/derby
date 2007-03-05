/*
 
 Derby - Class org.apache.derbyTesting.system.nstest.utils.MemCheck
 
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

package org.apache.derbyTesting.system.nstest.utils;

import java.util.Date;

/**
 * MemCheck - a background thread that prints current memory usage
 */
public class MemCheck extends Thread {

	int delay = 200000;

	public boolean stopNow = false;

	public MemCheck() {
	}

	public MemCheck(int num) {
		delay = num;
	}

	/*
	 * Implementation of run() method to check memory
	 * 
	 */
	public void run() {
		while (stopNow == false) {
			try {
				showmem();
				sleep(delay);
			} catch (java.lang.InterruptedException ie) {
				System.out.println("memcheck: unexpected error in sleep");
			}
		}
	}

	/*
	 * Print the current memory status
	 */
	public static void showmem() {
		Runtime rt = null;
		Date d = null;
		rt = Runtime.getRuntime();
		d = new Date();
		System.out.println("total memory: " + rt.totalMemory() + " free: "
				+ rt.freeMemory() + " " + d.toString());

	}

	public static void main(String argv[]) {
		System.out.println("memCheck starting");
		MemCheck mc = new MemCheck();
		mc.run();
	}
}
