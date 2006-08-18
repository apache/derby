/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.ProcessStreamResult

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
	protected OutputStreamWriter outStream;
	// Encoding to be used to read output of test jvm process
	protected String encoding;
	protected boolean finished;
	protected IOException ioe;
	protected Thread myThread;
	protected long startTime;
	protected boolean interrupted;
	protected int timeout;

	public ProcessStreamResult(InputStream in, BufferedOutputStream bos,
		    String timemin) throws IOException, InterruptedException
	{
		this(in, bos, timemin, null, null);
	}

	public ProcessStreamResult(InputStream in, BufferedOutputStream bos,
	  String timemin, String inEncoding, String outEncoding)
		throws IOException, InterruptedException
	{
		this.in = in;
        if (outEncoding == null) {
            this.outStream = new OutputStreamWriter(bos);
        } else {
            this.outStream = new OutputStreamWriter(bos, outEncoding);
        }
        this.encoding = inEncoding;
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
			char[] ca = new char[1024];
			int valid;
			interrupted = false;
			
			// Create an InputStreamReader with encoding, if specified. 
			// Otherwise, use default.
			InputStreamReader inStream;
			if(encoding != null)
        		inStream = new InputStreamReader(in, encoding);
        	else
        		inStream = new InputStreamReader(in);
			
			while ((valid = inStream.read(ca, 0, ca.length)) != -1)
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
*/    			outStream.write(ca, 0, valid);
    			outStream.flush();
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
