/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.TimedProcess

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

/**
 * This class is a wrapper of Process to provide a waitFor() method
 * that forcibly terminates the process if it does not
 * complete within the specified time.
 *
 * @author Phil Lopez
 * 
 */
public class TimedProcess
{

  private Process process;

  public TimedProcess(Process process)
  {
    this.process = process;
  }

  public int waitFor(int sec)
  {
    int exitValue = -1;

    // Create a thread to wait for the process to die
    WaitForProcess t = new WaitForProcess(process);
    t.start();
    
    // Give the process sec seconds to terminate
    try
    {
      t.join(sec * 1000);

      // Otherwise, interrupt the thread...
      if (t.isAlive())
      {
        t.interrupt();
        
        System.err.println("Server Process did not complete in time. Destroying...");
        // ...and destroy the process with gusto
        process.destroy();
      }
      else
      {
        // process shut down, so it is right to get the exit value from it
        exitValue = t.getProcessExitValue();
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  
    return exitValue;
  }
} // public class TimedProcess


class WaitForProcess
  extends Thread
{
  private Process process;
  private int processExitValue;
  
  public WaitForProcess(Process process)
  {
    this.process = process;
  }

  public int getProcessExitValue()
  {
    return processExitValue;
  }

  public void run()
  {
    // Our whole goal in life here is to waitFor() the process.
    // However, we're actually going to catch the InterruptedException for it!
    try
    {
      processExitValue = process.waitFor();
    }
    catch (InterruptedException e)
    {
      // Don't do anything here; the thread will die of natural causes
    }
  }
} // class WaitForProcess

