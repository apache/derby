/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.io.*;


public class ProcessStreamDrainer implements Runnable
{

	protected ByteArrayOutputStream data;
	protected InputStream in;
	protected FileOutputStream fos;
	protected BufferedOutputStream bos;
	protected boolean finished;
	protected IOException ioe;

	public ProcessStreamDrainer(InputStream in, File tmpOutFile)
	    throws IOException, InterruptedException
	{
		data = new ByteArrayOutputStream();
		this.in = in;
        this.fos = new FileOutputStream(tmpOutFile);
        this.bos = new BufferedOutputStream(fos, 4096);
		Thread myThread = new Thread(this, getClass().getName());

		myThread.setPriority(Thread.MIN_PRIORITY);
		//System.out.println("ProcessStreamDrainer calling start...");
		myThread.start();
	}

	public synchronized void run()
	{
        //System.out.println("Thread run...");
        if ( in == null )
        {
            System.out.println("The inputstream is null");
            System.exit(1);
        }

		try
		{
			byte[] ca = new byte[4096];
			int valid;
			while ((valid = in.read(ca, 0, ca.length)) != -1)
			{
			    //System.out.println(ca);
    			bos.write(ca, 0, valid);
    			bos.flush();
			}
			bos.flush();
		}
		catch (IOException ioe)
		{
			System.out.println(ioe);
		}

		synchronized (this)
		{
			finished = true;
			notifyAll();
		}
	}

	public void Wait() throws IOException
	{
	    synchronized(this)
	    {
	        try
	        {
	            while (!finished)
	            {
	                wait();
	            }
	        }
	        catch (InterruptedException ie)
	        {
	            System.out.println("Interrupted: " + ie.toString());
	        }
	    }
	    bos.close();
	    return;
	}
}
