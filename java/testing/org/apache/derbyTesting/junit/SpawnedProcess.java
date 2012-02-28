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
import java.io.OutputStream;
import java.io.PrintStream;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Utility code that wraps a spawned process (Java Process object).
 * <p>
 * There are three main aspects handled by this class:
 * <ul> <li>Draining the output streams of the process.<br/>
 *          Happens automatically, the output gathered can be accessed with
 *          {@linkplain #getFailMessage}, {@linkplain #getFullServerError},
 *          {@linkplain #getFullServerOutput}, and
 *          {@linkplain #getNextServerOutput}</li>
 *      <li>Waiting for process completion, followed by cleanup (see
 *          {@linkplain #complete()} and {@linkplain #complete(long)})</li>
 *      <li>Forcibly destroying a process that live too long, for instance
 *          if inter-process communication hangs. This happens automatically
 *          if a threshold value is exceeded.</li>
 * </ul>
 * <p>
 * <em>Implementation notes</em>: Active waiting is employed when waiting for
 * the process to complete. This is considered acceptable since the expected
 * usage pattern is to spawn the process, execute a set of tests, and then
 * finally asking the process to shut down. Waiting for the process to
 * complete is the last step, and a process typically lives only for a short
 * period of time anyway (often only for seconds, seldom more than a few
 * minutes).
 * <br/>
 * Forcibly destroying processes that live too long makes the test run
 * continue even when facing inter-process communication hangs. The prime
 * example is when both the client and the server are waiting for the other
 * party to send data. Since the timeout is very high this feature is intended
 * to avoid automated test runs from hanging indefinitely, for instance due to
 * environmental issues affecting the process.
 */
//@NotThreadSafe
public final class SpawnedProcess {

    private static final String TAG = "DEBUG: {SpawnedProcess} ";
    private static Timer KILL_TIMER;

    /**
     * Property allowing the kill threshold to be overridden.
     * <p>
     * Interprets the numeric value as milliseconds, ignored if non-numeric.
     * Overriding this value may be required if the test machine is extremely
     * slow, or you want to kill hung processes earlier for some reason.
     */
    private static final String KILL_THRESHOLD_PROPERTY =
            "derby.tests.process.killThreshold";
    private static final long KILL_THRESHOLD_DEFAULT = 45*60*1000; // 45 minutes
    /** The maximum allowed time for a process to live. */
    private static final long KILL_THRESHOLD;
    static {
        long tmpThreshold = KILL_THRESHOLD_DEFAULT;
        String tmp = BaseTestCase.getSystemProperty(KILL_THRESHOLD_PROPERTY);
        if (tmp != null) {
            try {
                tmpThreshold = Long.parseLong(tmp);
            } catch (NumberFormatException nfe) {
                // Ignore, use the default set previously.
                System.err.println(TAG + "Invalid kill threshold: " + tmp);
            }
        }
        KILL_THRESHOLD = tmpThreshold;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            // Ignore the interrupt. We want to make sure the process
            // terminates before returning, and we don't want to preserve
            // the interrupt flag because it causes Derby to shut down. These
            // are test requirements and don't apply for production code.
            // Print a notice to stdout.
            System.out.println(TAG + "Interrupted while sleeping (ignored)");
        }
    }

    private final String name;

    private final Process javaProcess;

    private final StreamSaver errSaver;

    private final StreamSaver outSaver;

    private boolean suppressOutput;

    private final TimerTask killTask;

    /**
     * Creates a new wrapper to handle the given process.
     *
     * @param javaProcess a (running) process
     * @param name name to associate with the process
     */
    public SpawnedProcess(Process javaProcess, String name) {
        this.javaProcess = javaProcess;
        this.name = name;

        errSaver = startStreamSaver(javaProcess.getErrorStream(), name
                .concat(":System.err"));
        outSaver = startStreamSaver(javaProcess.getInputStream(), name
                .concat(":System.out"));
        killTask = scheduleKill(javaProcess, name);
    }

    /**
     * Schedules a task to kill/terminate the task after a predefined timeout.
     *
     * @param name name of the process
     * @param process the process
     * @return The task object.
     */
    private TimerTask scheduleKill(Process process, String name) {
        synchronized (KILL_THRESHOLD_PROPERTY) {
            if (KILL_TIMER == null) {
                // Can't use 1.5 methods yet due to J2ME. Add name later.
                KILL_TIMER = new Timer(true);
            }        
        }
        TimerTask killer = new ProcessKillerTask(process, name);
        KILL_TIMER.schedule(killer, KILL_THRESHOLD);
        return killer;
    }

    /**
     * Causes output obtained from the process to be suppressed when
     * executing the {@code complete}-methods.
     *
     * @see #getFullServerOutput() to obtain suppressed output from stdout
     * @see #getFullServerError() to obtain suppressed output from stderr
     */
    public void suppressOutputOnComplete() {
        suppressOutput = true;
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
     * That is, {@link #complete()} or {@link #complete(long)}
     * should be called first.
     * </p>
     */
    public String getFullServerOutput() throws InterruptedException {
        // First wait until we've read all the output.
        outSaver.thread.join();

        synchronized (this) {
            return outSaver.stream.toString();
        }
    }
    
    /**
     * Get the full server error output (stderr) as a string using the default
     * encoding which is assumed is how it was originally written.
     * <p>
     * This method should only be called after the process has completed.
     * That is, {@link #complete()} or {@link #complete(long)}
     * should be called first.
     */
    public String getFullServerError() throws InterruptedException {
        // First wait until we've read all the output on stderr.
        errSaver.thread.join();

        synchronized (this) {
            return errSaver.stream.toString();
        }
    }

    /**
     * Position offset for getNextServerOutput().
     */
    int stdOutReadOffset;
    /**
     * Get the next set of server output (stdout) as a string using the default
     * encoding which is assumed is how it was originally
     * written. Assumes a single caller is executing the calls
     * to this method.
     */
    public String getNextServerOutput() {
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
    public String getFailMessage(String reason) {
        sleep(500);
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
     * Waits for the process to terminate.
     * <p>
     * This call will block until one of the following conditions are met:
     * <ul> <li>the process terminates on its own</li>
     *      <li>the hung-process watchdog mechanism forcibly terminates the
     *          process (see {@linkplain #scheduleKill})</li>
     * @return The process exit code.
     * @throws IOException if printing diagnostics fails
     */
    public int complete()
            throws IOException {
        return complete(Long.MAX_VALUE);         
    }

    /**
     * Waits for the process to terminate, forcibly terminating it if it
     * takes longer than the specified timeout.
     * <p>
     * This call will block until one of the following conditions are met:
     * <ul> <li>the process terminates on its own</li>
     *      <li>the timeout is exceeded, at which point the process is
     *          forcibly destroyed</li>
     *      <li>the hung-process watchdog mechanism forcibly terminates the
     *          process (see {@linkplain #scheduleKill})</li>
     * @return The process exit code.
     * @throws IOException if printing diagnostics fails
     */
    public int complete(long timeout)
            throws IOException {
        long start = System.currentTimeMillis();
        Integer exitCode = null;
        while (exitCode == null) {
            try {
                exitCode = new Integer(javaProcess.exitValue());
            } catch (IllegalThreadStateException itse) {
                // This exception means the process is running.
                if (System.currentTimeMillis() - start > timeout) {
                    javaProcess.destroy();
                }
                sleep(500);
            }
        }

        // Clean up
        killTask.cancel();
        joinWith(errSaver.thread);
        joinWith(outSaver.thread);
        cleanupProcess();
        printDiagnostics(exitCode.intValue());
        return exitCode.intValue();
    }
    
    /**
     * Cleans up the process, explicitly closing the streams associated with it.
     */
    private void cleanupProcess() {
        // Doing this is considered best practice.
        closeStream(javaProcess.getOutputStream());
        closeStream(javaProcess.getErrorStream());
        closeStream(javaProcess.getInputStream());
        javaProcess.destroy();
    }

    /**
     * Prints diagnostics to stdout/stderr if the process failed.
     *
     * @param exitCode the exit code of the spawned process
     * @throws IOException if writing to an output stream fails
     * @see #suppressOutput
     */
    private synchronized void printDiagnostics(int exitCode)
            throws IOException {
        // Always write the error, except when suppressed.
        ByteArrayOutputStream err = errSaver.stream;
        if (!suppressOutput && err.size() != 0) {
            System.err.println("START-SPAWNED:" + name + " ERROR OUTPUT:");
            err.writeTo(System.err);
            System.err.println("END-SPAWNED  :" + name + " ERROR OUTPUT:");
        }

        // Only write contents of stdout if it appears the server
        // failed in some way, or output is suppressed.
        ByteArrayOutputStream out = outSaver.stream;
        if (!suppressOutput && exitCode != 0 && out.size() != 0) {
            System.out.println("START-SPAWNED:" + name
                    + " STANDARD OUTPUT: exit code=" + exitCode);
            out.writeTo(System.out);
            System.out.println("END-SPAWNED  :" + name
                    + " STANDARD OUTPUT:");
        }
    }

    /** Joins up with the specified thread. */
    private void joinWith(Thread t) {
        try {
            t.join();
        } catch (InterruptedException ie) {
            // Ignore the interrupt. We want to make sure the process
            // terminates before returning, and we don't want to preserve
            // the interrupt flag because it causes Derby to shut down. These
            // are test requirements and don't apply for production code.
            // Print a notice to stdout.
            System.out.println(TAG + "Interrupted while joining " +
                    "with thread '" + t.toString() + "'");
        }
    }

    /**
     * Closes the specified stream, ignoring any exceptions.
     *
     * @param stream stream to close (may be {@code null})
     */
    private void closeStream(Object stream) {
        if (stream instanceof InputStream) {
            try {
                ((InputStream)stream).close();
            } catch (IOException ioe) {
                // Ignore exception on close
            }
        } else if (stream instanceof OutputStream) {
            try {
                ((OutputStream)stream).close();
            } catch (IOException ioe) {
                // Ignore exception on close
            }
        }
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

    /**
     * Creates and starts a stream saver that reads the specified input stream
     * in a separate stream.
     *
     * @param in input stream to read from
     * @param name name of the thread
     * @return A {@code StreamSaver} object.
     */
    private StreamSaver startStreamSaver(final InputStream in,
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

    /**
     * A task that will kill the specified process.
     *
     * @see #scheduleKill(java.lang.Process, java.lang.String) 
     */
    private static class ProcessKillerTask
        extends TimerTask {

        private final String name;
        private Process process;

        public ProcessKillerTask(Process process, String name) {
            this.process = process;
            this.name = name;
        }

        public synchronized boolean cancel() {
            // Since this task will usually be in the timer queue for a long
            // time, nullify the process reference on cancel to free resources.
            process = null;
            return super.cancel();
        }

        public synchronized void run() {
            // We may have just been cancelled 
            if (process == null) {
                return;
            }

            System.err.println("DEBUG: Destroying process '" + name + "'");
            process.destroy();
            int retriesAllowed = 10;
            while (retriesAllowed > 0) {
                try {
                    int exitCode = process.exitValue();
                    System.err.println("DEBUG: Destroyed process '" + name +
                            "', exit code is " + exitCode);
                    break;
                } catch (IllegalThreadStateException itse) {
                    // Sleep for a second and retry.
                    sleep(1000);
                    retriesAllowed--;
                }
            }
            if (retriesAllowed == 0) {
                System.err.println(
                        "DEBUG: Failed to destroy process '" + name + "'");
            } 
            process = null;
        }
    }
}
