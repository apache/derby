/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.io.*;
import java.sql.Timestamp;


public class ProcessStreamResult implements Runnable
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	protected InputStream in;
	protected BufferedOutputStream bos;
	protected boolean finished;
	protected IOException ioe;
	protected Thread myThread;
	protected long startTime;
	protected boolean interrupted;
	protected int timeout;

	public ProcessStreamResult(InputStream in, BufferedOutputStream bos,
	    String timemin) throws IOException, InterruptedException
	{
		this.in = in;
        this.bos = bos;
        this.startTime = System.currentTimeMillis();
        if (timemin != null)
        {
            Integer i = new Integer(timemin);
            timeout = i.intValue();
        }
        else
            timeout = 0;
		myThread = new Thread(this);
		myThread.setPriority(Thread.MIN_PRIORITY);
		myThread.start();
	}

	public void run()
	{
        //System.out.println("Thread run... " + tname);
        if ( in == null )
        {
            System.out.println("The inputstream is null");
            System.exit(1);
        }
        
		try
		{
			byte[] ba = new byte[1024];
			int valid;
			interrupted = false;
			while ((valid = in.read(ba, 0, ba.length)) != -1)
			{
			    //System.out.println("Still reading thread: " + tname);
/*				if (timeout > 0) {
					long millis = System.currentTimeMillis();

					long diff = millis - startTime;

					int mins = (int) (diff / (1000 * 60));

					if (mins > timeout) {
						System.out.println("Timeout, kill the thread... ");
						//myThread.dumpStack();
						synchronized (this)
						{
							interrupted = true;
							finished = true;
							notifyAll();
							return;
						}
					}
			    }
*/    			bos.write(ba, 0, valid);
    			bos.flush();
			}
		}
		catch (IOException ioe)
		{
			//System.out.println(ioe);
			//ioe.printStackTrace();
		}

		synchronized (this)
		{
			finished = true;
			notifyAll();
		}
	}

	public boolean Wait() throws IOException
	{
	    synchronized(this)
	    {
			if (timeout > 0) {
				long millis = System.currentTimeMillis();

				long diff = millis - startTime;

				int mins = (int) (diff / (1000 * 60));

				if (mins > timeout)
				{
					return interrupted;
				}
			}
			try
			{
				while (!finished && !interrupted)
				{
					wait();
				}
			}
			catch (InterruptedException ie)
			{
				System.out.println("Interrupted: " + ie.toString());
			}
	    }
	    return interrupted;
	}
}
