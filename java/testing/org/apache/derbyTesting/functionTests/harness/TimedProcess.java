/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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

