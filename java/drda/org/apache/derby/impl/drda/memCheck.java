/*

   Derby - Class org.apache.derby.impl.drda.memCheck

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.drda;

import java.util.Date;

public class memCheck extends Thread {
	int delay = 200000;
	boolean stopNow = false;

public memCheck () {}

public  memCheck (int num) {
	delay = num;
}

public void run () {
	while (stopNow == false) {
		try {
			showmem();
			sleep(delay);
		} catch (java.lang.InterruptedException ie) {
			System.out.println("memcheck interrupted");
			stopNow = true;
		}
	}
}

	public static String getMemInfo() {
	Runtime rt = null;
	rt = Runtime.getRuntime();
	rt.gc();
	return "total memory: " 
		+ rt.totalMemory()
		+ " free: "
		+ rt.freeMemory();
	
	}

	public static long totalMemory() {
		Runtime rt = Runtime.getRuntime();
		return rt.totalMemory();
	}

	public static long freeMemory() {
		
		Runtime rt =  Runtime.getRuntime();
		rt.gc();
		return rt.freeMemory();
	}

	public static void showmem() {
	Date d = null;
	d = new Date();
	System.out.println(getMemInfo() + " " + d.toString());

}

public static void main (String argv[]) {
	System.out.println("memCheck starting");
	memCheck mc = new memCheck();
	mc.run();
}
}
