/*
 *
 * Derby - Class org.apache.derbyTesting.junit.SpawnedProcess
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * Utility code that wraps a spawned process (Java Process object).
 * Handles the output streams (stderr and stdout) written
 * by the process by spawning off background threads to read
 * them into byte arrays. The class provides access to the
 * output, typically called once the process is complete.
 */
public final class SpawnedProcess {

    private final String name;

    private final Process javaProcess;

    private final StreamSaver errSaver;

    private final StreamSaver outSaver;

    public SpawnedProcess(Process javaProcess, String name) {
        this.javaProcess = javaProcess;
        this.name = name;

        errSaver = streamSaver(javaProcess.getErrorStream(), name
                .concat(":System.err"));
        outSaver = streamSaver(javaProcess.getInputStream(), name
                .concat(":System.out"));
    }

    /**
     * Get the Java Process object
     */
    public Process getProcess() {
        return javaProcess;
    }
    
    /**
     * <p>
     * Get the full server output (stdout) as a string using the default
     * encoding which is assumed is how it was originally written.
     * </p>
     *
     * <p>
     * This method should only be called after the process has completed.
     * That is, {@link #complete(boolean)} or {@link #complete(boolean, long)}
     * should be called first.
     * </p>
     */
    public String getFullServerOutput() throws Exception {
        // First wait until we've read all the output.
        outSaver.thread.join();

        synchronized (this) {
            return outSaver.stream.toString();
        }
    }
    
    /**
     * Position offset for getNextServerOutput().
     */
    int stdOutReadOffset;
    /**
     * Get the next set of server output (stdout) as a string using the default
     * encoding which is assumed is how it was orginally
     * written. Assumes a single caller is executing the calls
     * to this method.
     */
    public String getNextServerOutput() throws Exception
    {
        byte[] fullData;
        synchronized (this) {
            fullData = outSaver.stream.toByteArray();
        }
        
        String output = new String(fullData, stdOutReadOffset,
                fullData.length - stdOutReadOffset);
        stdOutReadOffset = fullData.length;
        return output;
    }
    /**
     * Get a fail message that is the passed in reason plus
     * the stderr and stdout for any output written. Allows
     * easier debugging if the reason the process failed is there!
     */
    public String getFailMessage(String reason) throws InterruptedException
    {
        Thread.sleep(500);
        StringBuffer sb = new StringBuffer();
        sb.append(reason);
        sb.append(":Spawned ");
        sb.append(name);
        sb.append(" exitCode=");
        try {
            sb.append(javaProcess.exitValue());
        } catch (IllegalThreadStateException e) {
            sb.append("running");
        }

        ByteArrayOutputStream err = errSaver.stream;
        ByteArrayOutputStream out = outSaver.stream;

        synchronized (this) {
            if (err.size() != 0)
            {
                sb.append("\nSTDERR:\n");
                sb.append(err.toString());          
            }
            if (out.size() != 0)
            {
                sb.append("\nSTDOUT:\n");
                sb.append(out.toString());          
            }
       }
       return sb.toString();
    }

    /**
     * Complete the process.
     * @param destroy true to destroy it, false to wait indefinitely to complete 
     */
    public int complete(boolean destroy) throws InterruptedException, IOException {
        return complete(destroy, -1L);
    }
    
    /**
     * Complete the process.
     * @param destroy True to destroy it, false to wait for it to complete 
     * based on timeout.
     *  
     * @param timeout milliseconds to wait until finished or else destroy.
     * -1 don't timeout
     *  
     */
    public int complete(boolean destroy, long timeout) throws InterruptedException, IOException {
        int exitCode;
        if (timeout >= 0 ) {
            long totalwait = -1;
            while (totalwait < timeout) {
               try  { 
               exitCode = javaProcess.exitValue();
               //if no exception thrown, exited normally
               destroy = false;
               break;
               }catch (IllegalThreadStateException ite) {
                   if (totalwait >= timeout) {
                       destroy = true;
                       break;
                   } else {
                       totalwait += 1000;
                       Thread.sleep(1000);
                   }
               }
            }
    	}
        if (destroy)
            javaProcess.destroy();

        exitCode = javaProcess.waitFor();

        // The process has completed. Wait until we've read all output.
        outSaver.thread.join();
        errSaver.thread.join();

        synchronized (this) {

            // Always write the error
            ByteArrayOutputStream err = errSaver.stream;
            if (err.size() != 0) {
                System.err.println("START-SPAWNED:" + name + " ERROR OUTPUT:");
                err.writeTo(System.err);
                System.err.println("END-SPAWNED  :" + name + " ERROR OUTPUT:");
            }

            // Only write the error if it appeared the server
            // failed in some way.
            ByteArrayOutputStream out = outSaver.stream;
            if ((destroy || exitCode != 0) && out.size() != 0) {
                System.out.println("START-SPAWNED:" + name
                        + " STANDARD OUTPUT: exit code=" + exitCode);
                out.writeTo(System.out);
                System.out.println("END-SPAWNED  :" + name
                        + " STANDARD OUTPUT:");
            }
        }
        
        return exitCode;
    }

    /**
     * Class holding references to a stream that receives the output from a
     * process and a thread that reads the process output and passes it on
     * to the stream.
     */
    private static class StreamSaver {
        final ByteArrayOutputStream stream;
        final Thread thread;
        StreamSaver(ByteArrayOutputStream stream, Thread thread) {
            this.stream = stream;
            this.thread = thread;
        }
    }

    private StreamSaver streamSaver(final InputStream in,
            final String name) {

        final ByteArrayOutputStream out = new ByteArrayOutputStream() {
            public void reset() {
                super.reset();
                new Throwable("WWW").printStackTrace(System.out);
            }

        };

        Thread streamReader = new Thread(new Runnable() {

            public void run() {
                try {
                    byte[] buffer = new byte[1024];
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        synchronized (SpawnedProcess.this) {
                            out.write(buffer, 0, read);
                        }
                    }

                } catch (IOException ioe) {
                    ioe.printStackTrace(new PrintStream(out, true));
                }
            }

        }, name);
        streamReader.setDaemon(true);
        streamReader.start();

        return new StreamSaver(out, streamReader);
    }
}
