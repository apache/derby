/*
 *
 * Derby - Class BaseTestCase
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

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.SQLException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import java.security.PrivilegedActionException;


/**
 * Base class for JUnit tests.
 */
public abstract class BaseTestCase
    extends TestCase {

    protected final static String ERRORSTACKTRACEFILE = "error-stacktrace.out";
    protected final static String DEFAULT_DB_DIR      = "system";
    protected final static String DERBY_LOG           = "derby.log";
    /**
     * No argument constructor made private to enforce naming of test cases.
     * According to JUnit documentation, this constructor is provided for
     * serialization, which we don't currently use.
     *
     * @see #BaseTestCase(String)
     */
    private BaseTestCase() {}

    /**
     * Create a test case with the given name.
     *
     * @param name name of the test case.
     */
    public BaseTestCase(String name) {
        super(name);
    }
    
    /**
     * Run the test and force installation of a security
     * manager with the default test policy file.
     * Individual tests can run without a security
     * manager or with a different policy file using
     * the decorators obtained from SecurityManagerSetup.
     * <BR>
     * Method is final to ensure security manager is
     * enabled by default. Tests should not need to
     * override runTest, instead use test methods
     * setUp, tearDown methods and decorators.
     */
    public void runBare() throws Throwable {
        TestConfiguration config = getTestConfiguration();
        boolean trace = config.doTrace();
        long startTime = 0;
        if ( trace )
        {
            startTime = System.currentTimeMillis();
            out.println();
            out.print(getName() + " ");
        }

        // install a default security manager if one has not already been
        // installed
        if ( System.getSecurityManager() == null )
        {
            if (config.defaultSecurityManagerSetup())
            {
                assertSecurityManager();
            }
        }

        try {
            super.runBare();   
        }
        // To log the exception to file, copy the derby.log file and copy
        // the database of the failed test.
        catch (Throwable running) {
            PrintWriter stackOut = null;
            try{
                String failPath = PrivilegedFileOpsForTests.getAbsolutePath(getFailureFolder());
                // Write the stack trace of the error/failure to file.
                stackOut = new PrintWriter(
                        PrivilegedFileOpsForTests.getFileOutputStream(
                            new File(failPath, ERRORSTACKTRACEFILE), true));
                stackOut.println("[Error/failure logged at " +
                        new java.util.Date() + "]");
                running.printStackTrace(stackOut);
                stackOut.println(); // Add an extra blank line.
                // Copy the derby.log file.
                File origLog = new File(DEFAULT_DB_DIR, DERBY_LOG);
                File newLog = new File(failPath, DERBY_LOG);
                PrivilegedFileOpsForTests.copy(origLog, newLog);
                // Copy the database.
                String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
                File dbDir = new File(DEFAULT_DB_DIR, dbName );
                File newDbDir = new File(failPath, dbName);
                PrivilegedFileOpsForTests.copy(dbDir,newDbDir);
           }
            catch (IOException ioe) {
                // We need to throw the original exception so if there
                // is an exception saving the db or derby.log we will print it
                // and additionally try to log it to file.
                BaseTestCase.printStackTrace(ioe);
                if (stackOut != null) {
                    stackOut.println("Copying derby.log or database failed:");
                    ioe.printStackTrace(stackOut);
                    stackOut.println();
                }
            }
            finally {
                if (stackOut != null) {
                    stackOut.close();
                }
                throw running;
            }
        }
        finally{
            if ( trace )
            {
                long timeUsed = System.currentTimeMillis() - startTime;
                out.print("used " + timeUsed + " ms ");
            }
        }
    }

    /**
     * Return the current configuration for the test.
     */
    public final TestConfiguration getTestConfiguration()
    {
    	return TestConfiguration.getCurrent();
    }
    
    /**
     * Get the folder where a test leaves any information
     * about its failure.
     * @return Folder to use.
     * @see TestConfiguration#getFailureFolder(TestCase)
     */
    public final File getFailureFolder() {
        return getTestConfiguration().getFailureFolder(this);
    }
    
    /**
     * Print alarm string
     * @param text String to print
     */
    public static void alarm(final String text) {
        out.println("ALARM: " + text);
    }

    /**
     * Print debug string.
     * @param text String to print
     */
    public static void println(final String text) {
        if (TestConfiguration.getCurrent().isVerbose()) {
            out.println("DEBUG: " + text);
        }
    }

    /**
     * Print trace string.
     * @param text String to print
     */
    public static void traceit(final String text) {
        if (TestConfiguration.getCurrent().doTrace()) {
            out.println(text);
        }
    }

    /**
     * Print debug string.
     * @param t Throwable object to print stack trace from
     */
    public static void printStackTrace(Throwable t) 
    {
        while ( t!= null) {
            t.printStackTrace(out);
            out.flush();
            
            if (t instanceof SQLException)  {
                t = ((SQLException) t).getNextException();
            } else {
                break;
            }
        }
    }

    private final static PrintStream out = System.out;
    
    /**
     * Set system property
     *
     * @param name name of the property
     * @param value value of the property
     */
    protected static void setSystemProperty(final String name, 
					    final String value)
    {
	
	AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){
		    
		    public Object run(){
			System.setProperty( name, value);
			return null;
			
		    }
		    
		}
	     );
	
    }
    /**
     * Remove system property
     *
     * @param name name of the property
     */
    protected static void removeSystemProperty(final String name)
	{
	
	AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){
		    
		    public Object run(){
			System.getProperties().remove(name);
			return null;
			
		    }
		    
		}
	     );
	
    }    
    /**
     * Get system property.
     *
     * @param name name of the property
     */
    protected static String getSystemProperty(final String name)
	{

	return (String )AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){

		    public Object run(){
			return System.getProperty(name);

		    }

		}
	     );
    }
    
    /**
     * Obtain the URL for a test resource, e.g. a policy
     * file or a SQL script.
     * @param name Resource name, typically - org.apache.derbyTesing.something
     * @return URL to the resource, null if it does not exist.
     */
    protected static URL getTestResource(final String name)
	{

	return (URL)AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){

		    public Object run(){
			return BaseTestCase.class.getClassLoader().
			    getResource(name);

		    }

		}
	     );
    }  
  
    /**
     * Open the URL for a a test resource, e.g. a policy
     * file or a SQL script.
     * @param url URL obtained from getTestResource
     * @return An open stream
    */
    protected static InputStream openTestResource(final URL url)
        throws PrivilegedActionException
    {
    	return (InputStream)AccessController.doPrivileged
	    (new java.security.PrivilegedExceptionAction(){

		    public Object run() throws IOException{
			return url.openStream();

		    }

		}
	     );    	
    }
    
    /**
     * Assert a security manager is installed.
     *
     */
    public static void assertSecurityManager()
    {
    	assertNotNull("No SecurityManager installed",
    			System.getSecurityManager());
    }

    /**
     * Compare the contents of two streams.
     * The streams are closed after they are exhausted.
     *
     * @param is1 the first stream
     * @param is2 the second stream
     * @throws IOException if reading from the streams fail
     * @throws AssertionFailedError if the stream contents are not equal
     */
    public static void assertEquals(InputStream is1, InputStream is2)
            throws IOException {
        if (is1 == null || is2 == null) {
            assertNull("InputStream is2 is null, is1 is not", is1);
            assertNull("InputStream is1 is null, is2 is not", is2);
            return;
        }
        long index = 0;
        int b1 = is1.read();
        int b2 = is2.read();
        do {
            // Avoid string concatenation for every byte in the stream.
            if (b1 != b2) {
                assertEquals("Streams differ at index " + index, b1, b2);
            }
            index++;
            b1 = is1.read();
            b2 = is2.read();
        } while (b1 != -1 || b2 != -1);
        is1.close();
        is2.close();
    }

    /**
     * Compare the contents of two readers.
     * The readers are closed after they are exhausted.
     *
     * @param r1 the first reader
     * @param r2 the second reader
     * @throws IOException if reading from the streams fail
     * @throws AssertionFailedError if the reader contents are not equal
     */
    public static void assertEquals(Reader r1, Reader r2)
            throws IOException {
        long index = 0;
        if (r1 == null || r2 == null) {
            assertNull("Reader r2 is null, r1 is not", r1);
            assertNull("Reader r1 is null, r2 is not", r2);
            return;
        }
        int c1 = r1.read();
        int c2 = r2.read();
        do {
            // Avoid string concatenation for every char in the stream.
            if (c1 != c2) {
                assertEquals("Streams differ at index " + index, c1, c2);
            }
            index++;
            c1 = r1.read();
            c2 = r2.read();
        } while (c1 != -1 || c2 != -1);
        r1.close();
        r2.close();
    }

    /**
     * Assert that the detailed messages of the 2 passed-in Throwable's are
     * equal (rather than '=='), as well as their class types.
     *
     * @param t1 first throwable to compare
     * @param t2 second throwable to compare
     */
    public static void assertThrowableEquals(Throwable t1,
                                             Throwable t2) {
        // Ensure non-null throwable's are being passed.
        assertNotNull(
            "Passed-in throwable t1 cannot be null to assert detailed message",
            t1);
        assertNotNull(
            "Passed-in throwable t2 cannot be null to assert detailed message",
            t2);

        // Now verify that the passed-in throwable are of the same type
        assertEquals("Throwable class types are different",
                     t1.getClass().getName(), t2.getClass().getName());

        // Here we finally check that the detailed message of both
        // throwable's is the same
        assertEquals("Detailed messages of the throwable's are different",
                     t1.getMessage(), t2.getMessage());
    }
    
    /**
     * Assert that two files in the filesystem are identical.
     * 
     * @param file1 the first file to compare
     * @param file2 the second file to compare
     */
	public static void assertEquals(final File file1, final File file2) {
		AccessController.doPrivileged
        (new PrivilegedAction() {
        	public Object run() {
        		try {
					InputStream f1 = new BufferedInputStream(new FileInputStream(file1));
					InputStream f2 = new BufferedInputStream(new FileInputStream(file2));

					assertEquals(f1, f2);
				} catch (FileNotFoundException e) {
					fail("FileNotFoundException in assertEquals(File,File): " + e.getMessage());
					e.printStackTrace();
				} catch (IOException e) {
					fail("IOException in assertEquals(File, File): " + e.getMessage());
					e.printStackTrace();
				}
				return null;
        	}
        });
	}
    
	/**
	 * Execute command using 'java' executable and verify that it completes
	 * with expected results
	 * @param expectedString String to compare the resulting output with. May be
	 *     null if the output is not expected to be of interest.
	 * @param cmd array of java arguments for command
	 * @param expectedExitValue expected return value from the command
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void assertExecJavaCmdAsExpected(String[] expectedString,
	        String[] cmd, int expectedExitValue) throws InterruptedException,
	        IOException {

	    Process pr = execJavaCmd(cmd);
	    String output = readProcessOutput(pr);
	    int exitValue = pr.exitValue();

	    Assert.assertEquals(expectedExitValue, exitValue);
	    if (expectedString != null) {
	        for (int i = 0; i < expectedString.length; i++) {
	            assertFalse(output.indexOf(expectedString[i]) < 0);
	        }
	    }
	}


	/**
	 * Execute a java command and return the process.
	 * The caller should decide what to do with the process, if anything,
	 * typical activities would be to do a pr.waitFor, or to
	 * get a getInputStream or getErrorStream
	 * Note, that for verifying the output of a Java process, there is
	 * assertExecJavaCmdAsExpected
	 * 
	 * @param cmd array of java arguments for command
	 * @return the process that was started
	 * @throws IOException
	 */
	public Process execJavaCmd(String[] cmd) throws IOException {
	    int totalSize = 3 + cmd.length;
	    String[] tcmd = new String[totalSize];
	    tcmd[0] = getJavaExecutableName();
	    tcmd[1] = "-classpath";
	    tcmd[2] = BaseTestCase.getSystemProperty("java.class.path");

	    System.arraycopy(cmd, 0, tcmd, 3, cmd.length);

	    final String[] command = tcmd;

	    Process pr = null;
	    try {
	        pr = (Process) AccessController
	        .doPrivileged(new PrivilegedExceptionAction() {
	            public Object run() throws IOException {
	                Process result = null;
	                result = Runtime.getRuntime().exec(command);
	                return result;
	            }
	        });
	    } catch (PrivilegedActionException pe) {
	        Exception e = pe.getException();
	        if (e instanceof IOException)
	            throw (IOException) e;
	        else
	            throw (SecurityException) e;
	    }
	    return pr;
	}

    /**
     * Return the executable name for spawning java commands.
     * This will be <path to j9>/j9  for j9 jvms.
     * @return full path to java executable.
     */
    public static final String getJavaExecutableName() {
        String vmname = getSystemProperty("com.ibm.oti.vm.exe");

        if (vmname == null) {
            vmname = getSystemProperty("java.vm.name");

            // Sun phoneME
            if ("CVM".equals(vmname)) {
                vmname = getSystemProperty("java.home") +
                    File.separator + "bin" +
                    File.separator + "cvm";
            } else {
                vmname = getSystemProperty("java.home") +
                    File.separator + "bin" +
                    File.separator + "java";
            }
        }
        return vmname;
    }

    /**
     * @return true if this is a j9 VM
     */
    public static final boolean isJ9Platform() {
        return getSystemProperty("com.ibm.oti.vm.exe") != null;
    }

    public static final boolean isSunJVM() {
        String vendor = getSystemProperty("java.vendor");
        return "Sun Microsystems Inc.".equals(vendor) ||
                "Oracle Corporation".equals(vendor);
    }
    
    public static final boolean isPlatform(String osName)  {

        return getSystemProperty("os.name").equals(osName);
    }

    /**
     * Determine if platform is a Windows variant.
     * <p>
     * Return true if platform is a windows platform.  Just looks for
     * os.name starting with "Windows".  The os.name property
     * can have at least the following values (there are probably more):
     *
     * AIX
     * Digital Unix
     * FreeBSD
     * HP UX
     * Irix
     * Linux
     * Mac OS
     * Mac OS X
     * MPE/iX
     * Netware 4.11
     * OS/2
     * Solaris
     * Windows 2000
     * Windows 95
     * Windows 98
     * Windows NT
     * Windows Vista
     * Windows XP
     * <p>
     *
     * @return true if running on a Windows platform.
     **/
    public static final boolean isWindowsPlatform() {
        return getSystemProperty("os.name").startsWith("Windows");
    }
    
    /**
     * Check if this is java 5
     * @return true if java.version system property starts with 1.5
     */
    public static final boolean isJava5() {
        return getSystemProperty("java.version").startsWith("1.5");
    }
   
    /**
     * Returns the major version of the class specification version supported
     * by the running JVM.
     * <ul>
     *  <li>48 = Java 1.4</li>
     *  <li>49 = Java 1.5</li>
     *  <li>50 = Java 1.6</li>
     *  <li>51 = Java 1.7</li>
     * </ul>
     *
     * @return Major version of class version specification, i.e. 49 for 49.0,
     *      or -1 if the version can't be obtained for some reason.
     */
    public static int getClassVersionMajor() {
        String tmp = getSystemProperty("java.class.version");
        if (tmp == null) {
            println("VM doesn't have property java.class.version");
            return -1;
        }
        // Is String.split safe to use by now?
        int dot = tmp.indexOf('.');
        int major = -1;
        try {
            major = Integer.parseInt(tmp.substring(0, dot));
        } catch (NumberFormatException nfe) {
            // Ignore, return -1.
        }
        return major;
    }


    public static final boolean isIBMJVM() {
        return ("IBM Corporation".equals(
                getSystemProperty("java.vendor")));
    }
    
   /**
    * Reads output from a process and returns it as a string.
    * This will block until the process terminates.
    * 
    * @param pr a running process
    * @return output of the process
    * @throws InterruptedException
    */
   public String readProcessOutput(Process pr) throws InterruptedException {
		InputStream is = pr.getInputStream();
		if (is == null) {
			fail("Unexpectedly receiving no text from the process");
		}

		String output = "";
		try {
		    char[] ca = new char[1024];
		    // Create an InputStreamReader with default encoding; we're hoping
		    // this to be en. If not, we may not match the expected string.
		    InputStreamReader inStream;
		    inStream = new InputStreamReader(is);

		    // keep reading from the stream until all done
		    int charsRead;
		    while ((charsRead = inStream.read(ca, 0, ca.length)) != -1)
		    {
		        output = output + new String(ca, 0, charsRead);
		    }
		} catch (Exception e) {
		    fail("Exception accessing inputstream from process", e);
		}

		// wait until the process exits
		pr.waitFor();
		
		return output;
	}
   
    /**
     * Remove the directory and its contents.
     * @param path Path of the directory
     */
    public static void removeDirectory(String path)
    {
        DropDatabaseSetup.removeDirectory(path);
    }
    /**
     * Remove the directory and its contents.
     * @param dir File of the directory
     */
    public static void removeDirectory(File dir)
    {
        DropDatabaseSetup.removeDirectory(dir);
    }

    /**
     * Fail; attaching an exception for more detail on cause.
     *
     * @param msg message explaining the failure
     * @param t the cause of the failure
     *
     * @exception AssertionFailedError
     */
    public static void fail(String msg, Throwable t)
            throws AssertionFailedError {

        AssertionFailedError ae = new AssertionFailedError(msg);
        ae.initCause(t);
        throw ae;
    }
} // End class BaseTestCase
