/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

public class BackgroundStreamSaver implements Runnable {
 
	protected InputStream in;
	protected OutputStream out;
	protected boolean finished;
	protected IOException ioe;

	public BackgroundStreamSaver(InputStream in, OutputStream out) 
	{
		this.in = in;
		this.out = out;
		
		Thread myThread = new Thread(this, getClass().getName());
		myThread.setPriority(Thread.MIN_PRIORITY);
		myThread.start();
	}

	public void run() 
	{
		try 
		{
			byte[] ca = new byte[1024];
			int valid;
			while ((valid = in.read(ca, 0, ca.length)) != -1) 
			{
				out.write(ca, 0, valid);
			}
			out.flush();
		} catch (IOException ioe) 
		{
			this.ioe = ioe;
		}

		synchronized (this) 
		{
			finished = true;
			notifyAll();
		}
	}

	public void finish() throws IOException 
	{
		if (ioe != null)
			throw ioe;

		synchronized (this) 
		{
			try 
			{
				while (!finished) 
				{
					wait();
				}
			} catch (InterruptedException ie) 
			{
				throw new IOException(ie.toString());
			}
			//out.close();
		}
	}
}
