/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.ProcessStreamResult

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.io.*;
import java.sql.Timestamp;


public class ProcessStreamResult implements Runnable
{

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
