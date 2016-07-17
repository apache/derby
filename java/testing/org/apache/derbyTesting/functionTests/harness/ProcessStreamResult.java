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

    /**
     * Flag to find out if the work was finished 
     * successfully without being interrupted 
     * in between because of a timeout setting
     */
	protected boolean finished;
	protected IOException ioe;
	protected Thread myThread;
	protected long startTime;
    
    /**
     * Flag to keep state of whether the myThread has timed out.
     * When interrupted is true, the myThread will exit 
     * from its work. 
     */
	protected boolean interrupted;
    
    /**
     * time in minutes for myThread to timeout in case it 
     * has not finished its work before that.
     * timeout handling only comes into effect only when Wait()
     * is called.
     */
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
            Integer i = Integer.valueOf(timemin);
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
			
            // keep reading from the stream as long as we have not 
            // timed out
			while (((valid = inStream.read(ca, 0, ca.length)) != -1) &&
                    !interrupted)
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

        // if we timed out, then just leave
        if ( interrupted )
            return;
        
		synchronized (this)
		{
            // successfully finished the work, notifyAll and leave.
			finished = true;
			notifyAll();
		}
	}

    /**
     * Wait till the myThread has finished its work or incase a timeout was set on this 
     * object, then to set a flag to indicate the myThread to leave at the end of the 
     * timeout period.
     * 
     * Behavior is as follows:
     * 1) If timeout is set to a valid value (&gt;0) - in this case, if myThread has not
     * finished its work by the time this method was called, then it will wait
     * till the timeout has elapsed or if the myThread has finished its work.
     * 
     * 2)If timeout is not set ( &lt;= 0) - in this case, if myThread has not
     * finished its work by the time this method was called, then it will wait
     * till myThread has finished its work.
     * 
     * If timeout is set to a valid value, and the timeout amount of time has elapsed, 
     * then the interrupted  flag is set to true to indicate that it is time for the 
     * myThread to stop its work and leave.
     *
     * @return true if the timeout happened before myThread work was finished
     *         else false
     * @throws IOException
     */
	public boolean Wait() throws IOException
	{
	    synchronized(this)
	    {
            // It is possible that we have finished the work 
            // by the time this method Wait() was called,
            // so need to check if that is the case, before we
            // go into a wait.
            if ( finished )
                return interrupted;
            
			if (timeout > 0) {
				long millis = System.currentTimeMillis();

				long diff = millis - startTime;

				int mins = (int) (diff / (1000 * 60));

				if (mins > timeout)
				{
                    interrupted = true;
					return interrupted;
				}
			}
			try
			{
                // find timeout in milliseconds
                long timeoutms = timeout * 60 *1000L;
                
                if ( timeout > 0 )
                    // wait till notified or till timeoutms has elapsed
                    wait(timeoutms);
                else
                    wait(); // wait till notified
                
                // if myThread didnt finish its work and we reached
                // here, that means we just timedout. 
                // In that case, indicate that we were interrupted and leave.
                // myThread will read the value of interrupted and 
                // stop its work and leave.
    		    if ( !finished )
                    interrupted = true;
            }
			catch (InterruptedException ie)
			{
                interrupted = true;
				System.out.println("Interrupted: " + ie.toString());
			}
	    }
	    return interrupted;
	}
}
