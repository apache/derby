/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.io.*;
import java.sql.Timestamp;

public class BackgroundStreamDrainer implements Runnable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	protected ByteArrayOutputStream data;
	protected InputStream in;
	protected boolean finished;
	protected IOException ioe;
	protected long startTime;
	protected Thread myThread;
	protected int timeout;

	public BackgroundStreamDrainer(InputStream in, String timemin)
	{
		data = new ByteArrayOutputStream();
		this.in = in;
        this.startTime = System.currentTimeMillis();;
        if (timemin != null)
        {
            Integer i = new Integer(timemin);
            timeout = i.intValue();
        }
        else
            timeout = 0;
        //System.out.println("timeout set to: " + timeout);

		myThread = new Thread(this, getClass().getName());
		myThread.setPriority(Thread.MIN_PRIORITY);
		myThread.start();
	}

	public void run()
	{
        if ( in == null )
        {
            System.out.println("The inputstream is null");
            System.exit(1);
        }

		try
		{
			byte[] ca = new byte[1024];
			int valid;
			while ((valid = in.read(ca, 0, ca.length)) != -1)
			{
                if (timeout > 0)
			    {
					long millis = System.currentTimeMillis();

					long diff = millis - startTime;

					int mins = (int) (diff / (1000 * 60));

					if (mins > timeout) {

						System.out.println("kill stderr thread...");
						synchronized (this)
						{
							finished = true;
							break;
						}
					}
			    }
			    //System.out.println("Bytes read to write data: " + valid);
				data.write(ca, 0, valid);
			}
		}
		catch (IOException ioe)
		{
			this.ioe = ioe;
			System.out.println(ioe.getMessage());
		}

		synchronized (this)
		{
			finished = true;
			notifyAll();
		}
	}

	public InputStream getData() throws IOException
	{
	    // FIXME: On Netware, the last read throws an IOException,
	    // which prevents the test output from getting written
		//if (ioe != null)
		//{
			//throw ioe;
        //}

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
			    System.out.println("IOException: " + ie);
				throw new IOException(ie.toString());
			}
		}
		return new ByteArrayInputStream(data.toByteArray());
	}
}
