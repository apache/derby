/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.drda
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.drda;

import java.util.Date;
	/**
		IBM Copyright &copy notice.
	*/


public class memCheck extends Thread { private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;
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
