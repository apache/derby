/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;


// the purpose of this class is to run IJ in a separate thread
public class RunIJ implements Runnable
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

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
