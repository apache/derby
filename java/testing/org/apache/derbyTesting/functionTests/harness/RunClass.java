/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.lang.reflect.Method;


// the purpose of this class is to run Java-based test cases in a separate thread
public class RunClass implements Runnable
{

	/**
		param args the arguments to pass into ij
	*/
	public RunClass(Class theClass, Method methodToCall, Object args[])
	{
		mainMethod = methodToCall;
		arguments=args;	
		testClass = theClass;
	}

	Object arguments[];
	Method mainMethod;
	Class testClass;

	public void run()
	{
        synchronized (this)
        {
		    try
		    {
			    mainMethod.invoke(testClass.newInstance(), arguments);				
		    }
		    catch (InstantiationException ie)
		    {
		        System.out.println("Class could not be instantiated: " + ie);
		        System.exit(1);
		    }
		    catch (IllegalAccessException iae)
		    {
		        System.out.println("RunClass: " + iae + " make sure the test class is public.");
		        System.exit(1);
		    }
		    catch (Exception e)
		    {
			    System.out.println("RunClass --> " + e);
			    e.printStackTrace();
		    }
		}
	}
}
