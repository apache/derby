/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util;

public class SubInterfaceClass extends ManyMethods
	implements NoMethodInterface, ExtendingInterface {

	public SubInterfaceClass(int value) {
		super(value);
	}

	/*
	** Methods of Runnable (from ExtendingInterface)
	*/

	public void run() {
	}

	/*
	** Methods of ExtendingInterface
	*/

	public void wait(int a, long b) {}

	public Object eimethod(Object a) {
		return a;
	}
}
