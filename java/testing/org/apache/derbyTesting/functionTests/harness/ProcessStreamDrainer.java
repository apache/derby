/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.ProcessStreamDrainer

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
