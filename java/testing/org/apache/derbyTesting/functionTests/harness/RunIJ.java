/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.RunIJ

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


// the purpose of this class is to run IJ in a separate thread
public class RunIJ implements Runnable
{

	/**
		param args the arguments to pass into ij
	*/
	public RunIJ(String args[])
	{
		ijArgs=args;	
	}

	String ijArgs[];

	public void run()
	{
	    synchronized (this)
	    {
		    try
		    {
			    org.apache.derby.tools.ij.main(ijArgs);
		    }
		    catch (Exception e)
		    {
			    System.out.println("RunIJ --> " + e);
			    e.printStackTrace();
		    }
		}
	}
}
