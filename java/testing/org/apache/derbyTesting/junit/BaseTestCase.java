/*
 *
 * Derby - Class org.apache.derbyTesting.junit.BaseTestCase
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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.sql.SQLException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import java.security.PrivilegedActionException;
import java.util.ArrayList;


/**
 * Base class for JUnit tests.
 */
public abstract class BaseTestCase
    extends TestCase {

    private static final String JACOCO_AGENT_PROP = "derby.tests.jacoco.agent";

    protected final static String ERRORSTACKTRACEFILE = "error-stacktrace.out";
    protected final static String DEFAULT_DB_DIR      = "system";
    protected final static String DERBY_LOG           = "derby.log";
    
    private static int debugPort; // default 8800
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
        boolean stopAfterFirstFail = config.stopAfterFirstFail();
        long startTime = 0;
        if ( trace )
        {
            startTime = System.currentTimeMillis();
            out.println();
            String  junitClassName = this.getClass().getName();
            junitClassName=Utilities.formatTestClassNames(junitClassName);
            out.print(traceClientType());
            out.print(junitClassName+"."+getName() + " ");
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
                if (stopAfterFirstFail) {
                    // if run with -Dderby.tests.stopAfterFirstFail=true
                    // exit after reporting failure. Useful for debugging
                    // cascading failures or errors that lead to hang.
                    running.printStackTrace(out);
                    System.exit(1);
                }
                else
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
            out.flush();
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
     * Change the value of {@code System.out}.
     *
     * @param out the new stream
     */
    protected void setSystemOut(final PrintStream out) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                System.setOut(out);
                return null;
            }
        });
    }

    /**
     * Change the value of {@code System.err}.
     *
     * @param err the new stream
     */
    protected void setSystemErr(final PrintStream err) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                System.setErr(err);
                return null;
            }
        });
    }

    /**
     * Set system property
     *
     * @param name name of the property
     * @param value value of the property
     */
    protected static void setSystemProperty(final String name, 
					    final String value)
    {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                System.setProperty(name, value);
                return null;
            }
        });
    }

    /**
     * Remove system property
     *
     * @param name name of the property
     */
    public static void removeSystemProperty(final String name)
	{
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                System.getProperties().remove(name);
                return null;
            }
        });
    }

    /**
     * Get system property.
     *
     * @param name name of the property
     */
    protected static String getSystemProperty(final String name)
	{
        return AccessController.doPrivileged(new PrivilegedAction<String>() {
            public String run() {
                return System.getProperty(name);
            }
        });
    }
    
    /**
     * Get files in a directory which contain certain prefix
     * 
     * @param dir
     *        The directory we are checking for files with certain prefix
     * @param prefix
     *        The prefix pattern we are interested.
     * @return The list indicates files with certain prefix.
     */
    protected static String[] getFilesWith(final File dir, String prefix) {
        return AccessController.doPrivileged(new PrivilegedAction<String[]>() {
                    public String[] run() {
                        //create a FilenameFilter and override its accept-method to file
                        //files start with "javacore"*
                        FilenameFilter filefilter = new FilenameFilter() {
                            public boolean accept(File dir, String name) {
                                //if the file has prefix javacore return true, else false
                                return name.startsWith("javacore");
                            }
                        };
                        return dir.list(filefilter);
                    }
                });
    }
    
    /**
     * Obtain the URL for a test resource, e.g. a policy
     * file or a SQL script.
     * @param name Resource name, typically - org.apache.derbyTesing.something
     * @return URL to the resource, null if it does not exist.
     */
    protected static URL getTestResource(final String name)
	{
        return AccessController.doPrivileged(new PrivilegedAction<URL>() {
            public URL run() {
                return BaseTestCase.class.getClassLoader().getResource(name);
            }
        });
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
        return AccessController.doPrivileged(
                new PrivilegedExceptionAction<InputStream>() {
            public InputStream run() throws IOException {
                return url.openStream();
            }
        });
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
     * <p>
     * Assert the equivalence of two byte arrays.
     * </p>
     */
    public  static  void    assertEquals( byte[] expected, byte[] actual )
    {
        if ( assertSameNullness( expected, actual ) ) { return; }
        
        assertEquals( expected.length, actual.length );
        for ( int i = 0; i < expected.length; i++ )
        {
            assertEquals( Integer.toString( i ), expected[ i ], actual[ i ] );
        }
    }

    /**
     * Assert that two objects are either both null or neither null.
     * Returns true if they are null.
     */
    public  static  boolean assertSameNullness( Object expected, Object actual )
    {
        if ( expected ==  null )
        {
            assertNull( actual );
            return true;
        }
        else
        {
            assertNotNull( actual );
            return false;
        }
    }

    /**
     * <p>
     * Assert the equivalence of two int arrays.
     * </p>
     */
    public  static  void    assertEquals( int[] expected, int[] actual )
    {
        if ( assertSameNullness( expected, actual ) ) { return; }
        
        assertEquals( expected.length, actual.length );
        for ( int i = 0; i < expected.length; i++ )
        {
            assertEquals( Integer.toString( i ), expected[ i ], actual[ i ] );
        }
    }

    /**
     * <p>
     * Assert the equivalence of two long arrays.
     * </p>
     */
    public  static  void    assertEquals( long[] expected, long[] actual )
    {
        if ( assertSameNullness( expected, actual ) ) { return; }
        
        assertEquals( expected.length, actual.length );
        for ( int i = 0; i < expected.length; i++ )
        {
            assertEquals( Integer.toString( i ), expected[ i ], actual[ i ] );
        }
    }

    /**
     * Assert that two files in the filesystem are identical.
     * 
     * @param file1 the first file to compare
     * @param file2 the second file to compare
     */
	public static void assertEquals(final File file1, final File file2) {
		AccessController.doPrivileged
        (new PrivilegedAction<Void>() {
        	public Void run() {
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
	public static void assertExecJavaCmdAsExpected(String[] expectedString,
	        String[] cmd, int expectedExitValue) throws InterruptedException,
	        IOException {

	    Process pr = execJavaCmd(cmd);
	    String output = readProcessOutput(pr);
	    int exitValue = pr.exitValue();
	    String expectedStrings = "";
	    for (int i = 0; i < expectedString.length; i++) 
	        expectedStrings += "\t[" +i + "]" + expectedString[i] +  "\n";
	    Assert.assertEquals("expectedExitValue:" + expectedExitValue +
	            " does not match exitValue:" + exitValue +"\n" +
	            "expected output strings:\n" + expectedStrings + 
	            " actual output:" + output,
	            expectedExitValue, exitValue);
	    if (expectedString != null) {
	        for (int i = 0; i < expectedString.length; i++) {
	            assertTrue("Could not find expectedString:" +
	                    expectedString[i] + " in output:" + output,
	                    output.indexOf(expectedString[i]) >= 0);
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
     * @param jvm the path to the java executable, or {@code null} to use
     *            the default executable returned by
     *            {@link #getJavaExecutableName()}
     * @param cp  the classpath for the spawned process, or {@code null} to
     *            inherit the classpath from the parent process
	 * @param cmd array of java arguments for command
     * @param dir working directory for the sub-process, or {@code null} to
     *            run in the same directory as the main test process
	 * @return the process that was started
	 * @throws IOException
	 */
    public static Process execJavaCmd(
        String jvm, String cp, String[] cmd, final File dir)
            throws IOException {

        // Is this an invocation of a jar file with java -jar ...?
        final boolean isJarInvocation = cmd.length > 0 && cmd[0].equals("-jar");

	    ArrayList<String> cmdlist = new ArrayList<String>();
        cmdlist.add(jvm == null ? getJavaExecutableName() : jvm);
	    if (isJ9Platform())
	    {
	        cmdlist.add("-jcl:foun11");
            // also add the setting for emma.active so any tests
            // that fork will work correctly. See DERBY-5558.
            String emmaactive=getSystemProperty("emma.active");
            if (emmaactive != null) {
                cmdlist.add("-Demma.active=" + emmaactive);            
            }
            // Do the same for jacoco.active, see DERBY-6079.
            String jacocoactive = getSystemProperty("jacoco.active");
            if (jacocoactive != null) {
                cmdlist.add("-Djacoco.active=" + jacocoactive);
            }
	    }

        if (isCVM()) {
            // DERBY-5642: The default maximum heap size on CVM is very low.
            // Increase it to prevent OOME in the forked process.
            cmdlist.add("-Xmx32M");
        }

        if (runsWithEmma()) {
            // DERBY-5801: If many processes write to the same file, it may
            // end up corrupted. Let each process have its own file to which
            // it writes coverage data.
            cmdlist.add("-Demma.coverage.out.file=" + getEmmaOutFile());

            // DERBY-5810: Make sure that emma.jar is included on the
            // classpath of the sub-process. (Only needed if a specific
            // classpath has been specified. Otherwise, the sub-process
            // inherits the classpath from the parent process, which
            // already includes emma.jar.)
            if (cp != null) {
                cp += File.pathSeparator + getEmmaJar().getPath();
            }

            // DERBY-5821: When starting a sub-process with java -jar, the
            // classpath argument will be ignored, so we cannot add emma.jar
            // that way. Add it to the boot classpath instead.
            if (isJarInvocation) {
                cmdlist.add("-Xbootclasspath/a:" + getEmmaJar().getPath());
            }
        }

        if (runsWithJaCoCo()) {
            // Property (http://www.eclemma.org/jacoco/trunk/doc/agent.html):
            // -javaagent:[yourpath/]jacocoagent.jar=[opt1]=[val1],[opt2]=[val2]
            String agent = getSystemProperty(JACOCO_AGENT_PROP);
            cmdlist.add(agent + (agent.endsWith("=") ? "": ",") +
                    "destfile=" + getJaCoCoOutFile());
        }

        if (isSunJVM() && Boolean.valueOf(
                    getSystemProperty("derby.test.debugSubprocesses")).
                booleanValue()) {
            setupForDebuggerAttach(cmdlist);
        }
        
        if (isJarInvocation) {
            // If -jar is specified, the Java command will ignore the user's
            // classpath, so don't set it. Fail if an explicit classpath has
            // been set in addition to -jar, as that's probably a mistake in
            // the calling code.
            assertNull("Both -jar and classpath specified", cp);
        } else {
            cmdlist.add("-classpath");
            cmdlist.add(cp == null ? getSystemProperty("java.class.path") : cp);
        }

	    for (int i =0; i < cmd.length;i++) {
	        cmdlist.add(cmd[i]);
	    }
	    final String[] command = (String[]) cmdlist.toArray(cmd);
	    println("execute java command:");
	    for (int i = 0; i < command.length; i++) {
	        println("command[" + i + "]" + command[i]);
	    }
	    try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Process>() {
                public Process run() throws IOException {
                    return Runtime.getRuntime().exec(
                            command, (String[]) null, dir);
	            }
	        });
	    } catch (PrivilegedActionException pe) {
            throw (IOException) pe.getException();
	    }
	}

    /**
     * Execute a java command and return the process. The process will run
     * in the same directory as the main test process. This method is a
     * shorthand for {@code execJavaCmd(null, null, cmd, null)}.
     */
    public static Process execJavaCmd(String[] cmd) throws IOException {
        return execJavaCmd(null, null, cmd, null);
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
     * <p>
     * Return the current directory.
     * </p>
     */
    public  static  File    currentDirectory()
    {
        return new File( getSystemProperty( "user.dir" ) );
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

    /**
     * Check if this is a CVM-based VM (like phoneME or Oracle Java ME
     * Embedded Client).
     */
    public static boolean isCVM() {
        return "CVM".equals(getSystemProperty("java.vm.name"));
    }

    /**
     * Check if the VM is phoneME.
     *
     * @return true if it is phoneME
     */
    public static boolean isPhoneME() {
        return isCVM() &&
                getSystemProperty("java.vm.version").startsWith("phoneme");
    }

    /**
     * Determine if there is a platform match with os.name.
     * This method uses an exact equals. Other methods might be useful
     * later for starts with.
     * 
     * @param osName value we want to check against the system property
     *      os.name
     * @return return true if osName is an exact match for osName
     */
    
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
     * SunOS
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
   
    public static final boolean isJava7() {
        return getSystemProperty("java.version").startsWith("1.7");
    }

    public static final boolean isJava8() {
        return getSystemProperty("java.version").startsWith("1.8");
    }

    public static final boolean runsWithEmma() {
        return getSystemProperty("java.class.path").indexOf("emma.jar") != -1;
    }

    public static boolean runsWithJaCoCo() {
        return SecurityManagerSetup.jacocoEnabled;
    }

    /**
     * Counter used to produce unique file names based on process count.
     *
     * @see #getEmmaOutFile()
     * @see #getJaCoCoOutFile()
     */
    private static int spawnedCount = 0;

    /**
     * Get a unique file object that can be used by sub-processes to store
     * JaCoCo code coverage data. Each separate sub-process should have its
     * own file in order to prevent corruption of the coverage data.
     *
     * @return a file to which a sub-process can write code coverage data
     */
    private static synchronized File getJaCoCoOutFile() {
        return new File(currentDirectory(),
                "jacoco.exec." + (++spawnedCount));
    }

    /**
     * Get a unique file object that can be used by sub-processes to store
     * EMMA code coverage data. Each separate sub-process should have its
     * own file in order to prevent corruption of the coverage data.
     *
     * @return a file to which a sub-process can write code coverage data
     */
    private static synchronized File getEmmaOutFile() {
        return new File(currentDirectory(),
                "coverage-" + (++spawnedCount) + ".ec");
    }

    /**
     * Get a URL pointing to {@code emma.jar}, if the tests are running
     * with EMMA code coverage. The method returns {@code null} if the
     * tests are not running with EMMA.
     */
    public static URL getEmmaJar() {
        return SecurityManagerSetup.getURL("com.vladium.emma.EMMAException");
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

    /**
     * Check if we have old style (before Sun Java 1.7) Solaris interruptible
     * IO. On Sun Java 1.5 >= update 22 and Sun Java 1.6 this can be disabled
     * with Java option {@code -XX:-UseVMInterruptibleIO}. On Sun Java 1.7 it
     * is by default disabled.
     *
     * @return true if we have old style interruptible IO
     */
    public static final boolean hasInterruptibleIO() {

        boolean interruptibleIO = false;

        try {
            AccessController.doPrivileged(
                new PrivilegedExceptionAction<Void>() {
                    public Void run() throws
                        IOException, InterruptedIOException {

                        TestConfiguration curr = TestConfiguration.getCurrent();

                        String sysHome = getSystemProperty("derby.system.home");

                        StringBuffer arbitraryRAFFileNameB = new StringBuffer();

                        arbitraryRAFFileNameB.append(sysHome);
                        arbitraryRAFFileNameB.append(File.separatorChar);
                        arbitraryRAFFileNameB.append("derby.log");

                        String arbitraryRAFFileName =
                            arbitraryRAFFileNameB.toString();
                        // Create if it does not exist:
                        new File(sysHome).mkdirs(); // e.g. "system"
                        new File(arbitraryRAFFileName).createNewFile();

                        RandomAccessFile f = new RandomAccessFile(
                            arbitraryRAFFileName, "r");

                        try {
                            Thread.currentThread().interrupt();
                            f.read();
                        } finally {
                            Thread.interrupted(); // clear flag
                            f.close();
                        }

                        return null;
                    }});
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof InterruptedIOException) {
                interruptibleIO = true;
            } else {
                // Better to assume nothing when the test fails. Then, tests
                // will not be skipped and we would not miss that something is
                // amiss.
                println("Could not test for interruptible IO," +
                        " so assuming we don't have it: " + e);
                e.getCause().printStackTrace();
                return false;
            }
        }

        return interruptibleIO;
    }


    public static final boolean isIBMJVM() {
        return ("IBM Corporation".equals(
                getSystemProperty("java.vendor")));
    }

    /**
     * Reads output from a process and returns it as a string.
     * <p>
     * This will block until the process terminates.
     * 
     * @param pr a running process
     * @return Output of the process, both STDOUT and STDERR.
     * @throws InterruptedException if interrupted while waiting for the
     *      subprocess or one of the output collector threads to terminate
     */
    public static String readProcessOutput(Process pr)
            throws InterruptedException {
        SpawnedProcess wrapper = new SpawnedProcess(pr, "readProcessOutput");
        wrapper.suppressOutputOnComplete();
        try {
            wrapper.complete();
        } catch (IOException ioe) {
            fail("process completion method failed", ioe);
        }
        String output = "<STDOUT>" + wrapper.getFullServerOutput() +
                "<END STDOUT>\n";
        output += "<STDERR>" + wrapper.getFullServerError() +
                "<END STDERR>\n";
        return output;
    }

    /**
     * Deletes the specified directory and all its files and subdirectories.
     * <p>
     * This method will attempt to delete all the files inside the root
     * directory, even if one of the delete operations fails.
     * <p>
     * After having tried to delete all files once, any remaining files will be
     * attempted deleted again after a pause. This is repeated, resulting
     * in multiple failed delete attempts for any single file before the method
     * gives up and raises a failure.
     * <p>
     * The approach above will mask any slowness involved in releasing file
     * handles, but should fail if a file handle actually isn't released on a
     * system that doesn't allow deletes on files with open handles (i.e.
     * Windows). It will also mask slowness caused by the JVM, the file system,
     * or the operation system.
     *
     * @param dir the root to start deleting from (root will also be deleted)
     */
    public static void assertDirectoryDeleted(File dir) {
        File[] fl = null;
        int attempts = 0;
        while (attempts < 4) {
            try {
                Thread.sleep(attempts * 2000);
            } catch (InterruptedException ie) {
                // Ignore
            }
            try {
                fl = PrivilegedFileOpsForTests.persistentRecursiveDelete(dir);
                attempts++;
            } catch (FileNotFoundException fnfe) {
                if (attempts == 0) {
                    fail("directory doesn't exist: " +
                            PrivilegedFileOpsForTests.getAbsolutePath(dir));
                } else  {
                    // In the previous iteration we saw remaining files, but
                    // now the root directory is gone. Not what we expected...
                    System.out.println("<assertDirectoryDeleted> root " +
                            "directory unexpectedly gone - delayed, " +
                            "external or concurrent delete?");
                    return;
                }
            }
            if (fl.length == 0) {
                return;
            } else {
                // Print the list of remaining files to stdout for debugging.
                StringBuffer sb = new StringBuffer();
                sb.append("<assertDirectoryDeleted> attempt ").append(attempts).
                    append(" left ").append(fl.length).
                    append(" files/dirs behind:");
                for (int i = 0; i < fl.length; i++) {
                    sb.append(' ').append(i).append('=').append(fl[i]);
                }
                System.out.println(sb);
            }
        }
        // If we failed to delete some of the files, list them and obtain some
        // information about each file.
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < fl.length; i++) {
            File f = fl[i];
            sb.append(PrivilegedFileOpsForTests.getAbsolutePath(f)).append(' ').
                append(PrivilegedFileOpsForTests.getFileInfo(f)).append(", ");
        }
        sb.deleteCharAt(sb.length() - 1).deleteCharAt(sb.length() - 1);
        fail("Failed to delete " + fl.length + " files (root=" +
                PrivilegedFileOpsForTests.getAbsolutePath(dir) + "): " +
                sb.toString());
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
     * Remove all the files in the list
     * @param list the list contains all the files
     */
    public static void removeFiles(String[] list)
    {
        DropDatabaseSetup.removeFiles(list);
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
    
    /**
     * assert a method from an executing test
     * 
     * @param testLaunchMethod
     *            complete pathname of the method to be executed
     * @throws Exception
     */
    public static void assertLaunchedJUnitTestMethod(String testLaunchMethod)
            throws Exception 
    {
        String[] cmd = new String[] { "junit.textui.TestRunner", "-m",
        testLaunchMethod };
        assertExecJavaCmdAsExpected(new String[] { "OK (1 test)" }, cmd, 0);
    }
    
    /**
     * assert a method from an executing test
     *
     * @param testLaunchMethod
     *            complete pathname of the method to be executed
     * @param databaseName
     *            name of the database to be used
     * @throws Exception
     */
    public static void assertLaunchedJUnitTestMethod(String testLaunchMethod,
            String databaseName)
            throws Exception 
    {
        String[] cmd = new String[] { 
                "-Dderby.tests.defaultDatabaseName=" + databaseName, 
                "junit.textui.TestRunner", "-m", testLaunchMethod };
        assertExecJavaCmdAsExpected(new String[] { "OK (1 test)" }, cmd, 0);
    }

    /** Returns once the system timer has advanced at least one tick. */
    public static void sleepAtLeastOneTick() {
        long currentTime = System.currentTimeMillis(); 
        while (System.currentTimeMillis() == currentTime) {
            sleep(1);
        }
    }

    /** Makes the current thread sleep up to {@code ms} milliseconds. */
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            // For now we just print a warning if we are interrupted.
            alarm("sleep interrupted");
        }
    }

    private static String traceClientType() {
       if (TestConfiguration.getCurrent().getJDBCClient().isEmbedded()) {
            return "(emb)";
        } else {
            return "(net)";
        }
    }
    
    private static void setupForDebuggerAttach(ArrayList<String> cmdlist) {
        if (debugPort == 0) {
            // lazy initialization
            String dbp = getSystemProperty("derby.test.debugPortBase");
            debugPort = 8800; // default
            if (dbp != null) {
                try {
                    debugPort = Integer.parseInt(dbp);
                } catch (NumberFormatException e) {
                    // never mind
                }
            }
        }
        
        char suspend = 'y'; // default
        String susp = getSystemProperty("derby.test.debugSuspend");
        if (susp != null && "n".equals(susp.toLowerCase())) {
            suspend = 'n';
        }
        
        cmdlist.add("-Xdebug");
        cmdlist.add("-Xrunjdwp:transport=dt_socket,address=" + (debugPort++) +
                ",server=y,suspend=" + suspend);
    }
} // End class BaseTestCase
