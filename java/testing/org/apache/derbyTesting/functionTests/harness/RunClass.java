/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.RunClass

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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
