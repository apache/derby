/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.BackgroundStreamSaver

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
