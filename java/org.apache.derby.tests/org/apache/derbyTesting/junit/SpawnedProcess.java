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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import java.util.Timer;
import java.util.TimerTask;
import static junit.framework.Assert.assertTrue;
import static org.apache.derbyTesting.junit.BaseTestCase.execJavaCmd;
import static org.apache.derbyTesting.junit.BaseTestCase.getJavaExecutableName;
import static org.apache.derbyTesting.junit.BaseTestCase.isIBMJVM;
import static org.apache.derbyTesting.junit.BaseTestCase.isWindowsPlatform;

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

//IC see: https://issues.apache.org/jira/browse/DERBY-5617
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5608
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5288
//IC see: https://issues.apache.org/jira/browse/DERBY-5288

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5608

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5288
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
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

//IC see: https://issues.apache.org/jira/browse/DERBY-5288
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
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
     * @param timeout the number of milliseconds to wait for the process
     *                to terminate normally before destroying it
     * @return The process exit code.
     * @throws IOException if printing diagnostics fails
     */
    public int complete(long timeout)
            throws IOException {
        long start = System.currentTimeMillis();
        Integer exitCode = null;
        while (exitCode == null) {
            try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                exitCode = javaProcess.exitValue();
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5608
        if (!suppressOutput && err.size() != 0) {
            System.err.println("START-SPAWNED:" + name + " ERROR OUTPUT:");
            err.writeTo(System.err);
            System.err.println("END-SPAWNED  :" + name + " ERROR OUTPUT:");
        }

        // Only write contents of stdout if it appears the server
        // failed in some way, or output is suppressed.
//IC see: https://issues.apache.org/jira/browse/DERBY-5288
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5288
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

//IC see: https://issues.apache.org/jira/browse/DERBY-5288
        return new StreamSaver(out, streamReader);
    }

    /**
     * A task that will kill the specified process.
     *
     * @see #scheduleKill(java.lang.Process, java.lang.String) 
     */
    private static class ProcessKillerTask
        extends TimerTask {
//IC see: https://issues.apache.org/jira/browse/DERBY-5617

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
                        "DEBUG: Failed to destroy process '" + name + "'");
            } 
            process = null;
        }
    }

    /**
     * Return {@code true} if the subprocess {@code p} has exited within {@code
     * patience} milliseconds. Sleep {@code sleepInterval} between each check}.
     * Note: you still need to call one of the {@link #complete} overloads even
     * if using this method (which is optional). It can be used before trying
     * a {@link #jstack} call.
     *
     * @param patience the maximum milliseconds we want to wait for
     * @param sleepInterval sleep for this amount of milliseconds before trying
     *                      testing again if not already exited the first time
     *                      we check. If patience &lt;= sleepInterval we only
     *                      check once.
     * @return true if the process exited before our patience is up.
     * @throws java.lang.InterruptedException
     */
    @SuppressWarnings("SleepWhileInLoop")
    public boolean waitForExit(long patience, long sleepInterval)
//IC see: https://issues.apache.org/jira/browse/DERBY-6704
            throws InterruptedException {
        boolean completed = false;
        while (!completed && patience > 0) {
            try {
                try {
                    javaProcess.exitValue();
                    completed = true;
                } catch (IllegalThreadStateException e) {
                    // try again after sleeping
                    Thread.sleep(sleepInterval);
                    patience = patience - sleepInterval;
                }
            } catch (InterruptedException e) {
                throw e;
            }
        }
        return completed;
    }


    /**
     * Return the jstack(1) dump of the process if possible.
     * It will only work if we are running with a full JDK, not a simple JRE.
     * It will not work on Windows, and just return an empty string.
     * @return jstack dump if possible
     * @throws PrivilegedActionException
     * @throws InterruptedException
     */
    public String jstack()
            throws PrivilegedActionException, InterruptedException{

        String output = "";

        if (!isWindowsPlatform() && !isIBMJVM()) {
            // Get the pid of the subprocess using reflection. Dirty,
            // for Unix there is a private field pid in the implementing
            // class.
            final int pid = getPid();
            final String execName = getJavaExecutableName().replace(
                    "jre" + File.separator + "bin" + File.separator + "java",
                    "bin" + File.separator + "jstack");
            final String[] arguments =
                    new String[]{Integer.toString(pid)};
            try {
                final Process p2 =
                        execJavaCmd(execName, null, arguments, null, false);
                final SpawnedProcess spawn2 = new SpawnedProcess(p2, "jstack");
                spawn2.suppressOutputOnComplete();
                // Close stdin of the process so that it stops
                // any waiting for it and exits (shouldn't matter for this test)
                p2.getOutputStream().close();
                final int exitCode2 = spawn2.complete(30000); // 30 seconds
                assertTrue(spawn2.getFailMessage("jstack failed: "),
                        exitCode2 == 0);
                output = spawn2.getFullServerOutput();
            } catch (IOException e) {
                output = "Tried to catch jstack of hanging subprocess but it "
                        + "failed (using JDK or JRE?): " + e;
            }
        }

        return output;
    }

    /**
     * Return the pid if on Unixen, or -1 on Windows (can't be obtained).
     * @return pid
     * @throws PrivilegedActionException
     */
    public int getPid() throws PrivilegedActionException {
        if (!isWindowsPlatform() && !isIBMJVM()) {
            return AccessController.doPrivileged(
                new PrivilegedExceptionAction<Integer>() {
                    @Override
                    public Integer run() throws IllegalAccessException,
                            NoSuchFieldException {
                        final Field f = javaProcess.getClass().
                                getDeclaredField("pid");
                        f.setAccessible(true);

                        return f.getInt(javaProcess);
                    }
                });
        } else {
            return -1;
        }
    }

}
