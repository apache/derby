/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.engine.ErrorStreamTest

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

package org.apache.derbyTesting.functionTests.tests.engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Tests related to the Derby error stream.
 *
 * This test has been converted to junit from the old harness tests
 * logStream.java and errorStream.java. The testDefault method is based on
 * logStream.java, the other test* methods are from errorStream.java.
 */
public class ErrorStreamTest extends BaseJDBCTestCase {
    private static final String FILE_PROP   = "derby.stream.error.file";
    private static final String METHOD_PROP = "derby.stream.error.method";
    private static final String FIELD_PROP  = "derby.stream.error.field";
    private static final String STYLE_PROP = "derby.stream.error.style";

    private static final String ROLLING_FILE_STYLE = "rollingFile";
    private static final String ROLLING_FILE_COUNT_PROP = "derby.stream.error.rollingFile.count";
    private static final String ROLLING_FILE_LIMIT_PROP = "derby.stream.error.rollingFile.limit";
    private static final String ROLLING_FILE_PATTERN_PROP = "derby.stream.error.rollingFile.pattern";
    private static final String DERBY_0_LOG = "derby-0.log";
    private static final String DERBYLANGUAGELOG_QUERY_PLAN = "derby.language.logQueryPlan";
    
    private static final String LOGFILESDIR = "logfilesdir";
    
    /**
     * runNo keeps track of which run we are in to generate unique (within a
     * JUnit run) names for files that are used in the test. Has to be static.
     */
    private static int runNo = 0;

    /**
     * File used when FILE_PROP is set, it maps to file
     * <database>-file-<runNo>.log
     */
    private File fileStreamFile;

    /**
     * See doc for getStream() below. Has to be static.
     */
    private static OutputStream methodStream;
    private File methodStreamFile;

    /**
     * Field fieldStream used by Derby when FIELD_PROP is set,
     * so it needs to be public and static.
     * Maps to file <database>-field-<runNo>.log
     */
    public static OutputStream fieldStream;
    private File fieldStreamFile;

    /**
     * Field errStream used as redirection for System.err to be able
     * to checks its (non-)use in the scenarios. We first tried to
     * merge it with System.out and let the harness compare outputs,
     * but this gave intermittent merging differences, so abandoned.
     * Maps to file <database>-err-<runNo>.log
     */
    private OutputStream errStream;
    private File errStreamFile;
    
    public ErrorStreamTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.embeddedSuite(ErrorStreamTest.class);
    }

    public void setUp() throws Exception {
        bootDerby();
        // Shutdown engine so we can change properties for error stream
        getTestConfiguration().shutdownEngine();
        openStreams();
    }

    public void tearDown() throws Exception {
        resetProps();
        closeStreams();
        nullFields();
        super.tearDown();
    }

    /**
     * Test that the error stream file (derby.log) is created at database boot
     * and not deleted when the database is shut down, but can be deleted
     * afterwards.
     */
    public void testDefault() throws IOException, SQLException {
        File derbyLog = new File(getSystemProperty("derby.system.home"),
              "derby.log");
        bootDerby();
        assertIsExisting(derbyLog);
        assertNotDirectory(derbyLog);
        assertNotEmpty(derbyLog);

        println("Shutdown database");
        getTestConfiguration().shutdownDatabase();

        assertIsExisting(derbyLog);
        assertNotDirectory(derbyLog);
        assertNotEmpty(derbyLog);

        println("Shutdown engine");
        getTestConfiguration().shutdownEngine();

        boolean deleted = deleteFile(derbyLog);
        assertTrue("File " + derbyLog + " could not be deleted", deleted);
    }

    /**
     * Test the derby.stream.error.file property.
     */
    public void testFile() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test the derby.stream.error.file property with wrong input.
     */
    public void testWrongFile() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(new File(
              new File(getSystemProperty("derby.system.home"), "foo"),
              makeStreamFilename("file")))); // erroneous path

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertNotEmpty(errStreamFile);
    }

    /**
     * Test the derby.stream.error.method property.
     */
    public void testMethod() throws IOException, SQLException  {
        setSystemProperty(METHOD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertNotEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test the derby.stream.error.method property with wrong input.
     */
    public void testWrongMethod() throws IOException, SQLException {
        setSystemProperty(METHOD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.nonExistingGetStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertNotEmpty(errStreamFile);
    }

    /**
     * Test the derby.stream.error.field property.
     */
    public void testField() throws IOException, SQLException {
        setSystemProperty(FIELD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertNotEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test the derby.stream.error.field property with wrong input.
     */
    public void testWrongField() throws IOException, SQLException {
        setSystemProperty(FIELD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.nonExistingFieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertNotEmpty(errStreamFile);
    }

    /**
     * Test that the derby.stream.error.file property overrides the
     * derby.stream.error.method property.
     */
    public void testFileOverMethod() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));
        setSystemProperty(METHOD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test that the derby.stream.error.file property overrides the
     * derby.stream.error.field property.
     */
    public void testFileOverField() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));
        setSystemProperty(FIELD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test that the derby.stream.error.file property overrides the
     * derby.stream.error.method and the derby.stream.error.field property.
     */
    public void testFileOverMethodAndField() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));
        setSystemProperty(METHOD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");
        setSystemProperty(FIELD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test that the derby.stream.error.field property overrides the
     * derby.stream.error.method property.
     */
    public void testMethodOverField() throws IOException, SQLException {

        setSystemProperty(METHOD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");
        setSystemProperty(FIELD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertNotEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test the derby.stream.error.style=rollingFile property.
     */
    public void testStyleRollingFile() throws IOException, SQLException  {
        setSystemProperty(STYLE_PROP, ROLLING_FILE_STYLE);
        
        File derby0log = new File(getSystemProperty("derby.system.home"), DERBY_0_LOG);
        
        File derby0lck = new File(getSystemProperty("derby.system.home"),
              "derby-0.log.lck");
        
        bootDerby();
        
        assertIsExisting(derby0log);
        assertNotDirectory(derby0log);
        assertNotEmpty(derby0log);

        assertIsExisting(derby0lck);
        assertNotDirectory(derby0lck);
        assertIsEmpty(derby0lck);

        println("Shutdown database");
        getTestConfiguration().shutdownDatabase();

        assertIsExisting(derby0log);
        assertNotDirectory(derby0log);
        assertNotEmpty(derby0log);

        assertIsExisting(derby0lck);
        assertNotDirectory(derby0lck);
        assertIsEmpty(derby0lck);

        println("Shutdown engine");
        getTestConfiguration().shutdownEngine();

        assertNotExisting(derby0lck);

        boolean deleted = deleteFile(derby0log);
        assertTrue("File " + derby0log + " could not be deleted", deleted);
    }

    /**
     * Test the derby.stream.error.style property with wrong style.
     */
    public void testWrongStyle() throws IOException, SQLException {
        setSystemProperty(STYLE_PROP, "unknownStyle");
        
        File derby0log = new File(getSystemProperty("derby.system.home"), DERBY_0_LOG);
        
        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(derby0log);
        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertNotEmpty(errStreamFile);
    }

    /**
     * Test the derby.stream.error.style=rollingFile property with default config
     */
    public void testDefaultRollingDefaultConfig() throws IOException, SQLException {
        setSystemProperty(STYLE_PROP, ROLLING_FILE_STYLE);
        
        // This is set so that we can get enough output into the log files
        setSystemProperty(DERBYLANGUAGELOG_QUERY_PLAN, "true");
                
        bootDerby();
        
        // This will generate enough output to roll through all 10 log files
        for (int i = 0; i < 3699; i++) {
            checkAllConsistency(getConnection());
        }
        // Make sure we remove the system property that is logging the query plan
        removeSystemProperty(DERBYLANGUAGELOG_QUERY_PLAN);
        getTestConfiguration().shutdownEngine();
        
        closeStreams();
        
        // There should be derb-0.log .. derby-9.log files present
        for (int i = 0; i < 10; i++) {
            File derbyLog = new File(getSystemProperty("derby.system.home"),
                "derby-" + i + ".log");
            assertIsExisting(derbyLog);
            assertNotDirectory(derbyLog);
            assertNotEmpty(derbyLog);
            
            // Check the last log file to make sure that it has the default
            //  limit 
            if (i == 9) {
                assertFileSize(derbyLog, 1024000);
            }
            
            boolean deleted = deleteFile(derbyLog);
            assertTrue("File " + derbyLog + " could not be deleted", deleted);
        }
        
        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }   

    /**
     * Test the derby.stream.error.style=rollingFile property with user configuration.
     */
    public void testDefaultRollingUserConfig() throws IOException, SQLException {
        setSystemProperty(STYLE_PROP, ROLLING_FILE_STYLE);
        setSystemProperty(ROLLING_FILE_PATTERN_PROP, "%d/db-%g.log");
        setSystemProperty(ROLLING_FILE_COUNT_PROP, "3");
        setSystemProperty(ROLLING_FILE_LIMIT_PROP, "10000");
        
        // This is set so that we can get enough output into the log files
        setSystemProperty(DERBYLANGUAGELOG_QUERY_PLAN, "true");
                
        bootDerby();
        
        // This will generate enough output to roll through all 3 log files
        for (int i = 0; i < 10; i++) {
            checkAllConsistency(getConnection());
        }
        // Make sure we remove the system property that is logging the query plan
        removeSystemProperty(DERBYLANGUAGELOG_QUERY_PLAN);
        removeSystemProperty(ROLLING_FILE_PATTERN_PROP);
        removeSystemProperty(ROLLING_FILE_COUNT_PROP);
        removeSystemProperty(ROLLING_FILE_LIMIT_PROP);

        getTestConfiguration().shutdownEngine();
        
        closeStreams();
        
        // There should be derb-0.log .. derby-3.log files present
        for (int i = 0; i < 3; i++) {
            File derbyLog = new File(getSystemProperty("derby.system.home"),
                "db-" + i + ".log");
            assertIsExisting(derbyLog);
            assertNotDirectory(derbyLog);
            assertNotEmpty(derbyLog);
            
            // Check the last log file to make sure that it has the correct
            //  limit 
            if (i == 2) {
                assertFileSize(derbyLog, 10000);
            }

            boolean deleted = deleteFile(derbyLog);
            assertTrue("File " + derbyLog + " could not be deleted", deleted);
        }
        
        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }   

    /**
     * Test that the derby.stream.error.style property overrides the
     * derby.stream.error.file property.
     */
    public void testRollingFileStyleOverFile() throws IOException, SQLException {
        setSystemProperty(STYLE_PROP, ROLLING_FILE_STYLE);
        
        File derby0log = new File(getSystemProperty("derby.system.home"), DERBY_0_LOG);
        
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(derby0log);
        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test that the derby.stream.error.style property overrides the
     * derby.stream.error.method property.
     */
    public void testRollingFileStyleOverMethod() throws IOException, SQLException {
        setSystemProperty(STYLE_PROP, ROLLING_FILE_STYLE);
        
        File derby0log = new File(getSystemProperty("derby.system.home"), DERBY_0_LOG);
        
        setSystemProperty(METHOD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(derby0log);
        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);

        boolean deleted = deleteFile(derby0log);
        assertTrue("File " + derby0log + " could not be deleted", deleted);    
     }

    /**
     * Test that the derby.stream.error.style property overrides the
     * derby.stream.error.field property.
     */
    public void testRollingFileStyleOverField() throws IOException, SQLException {
        setSystemProperty(STYLE_PROP, ROLLING_FILE_STYLE);
        
        File derby0log = new File(getSystemProperty("derby.system.home"), DERBY_0_LOG);
        
        setSystemProperty(FIELD_PROP,
              "org.apache.derbyTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(derby0log);
        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);

        boolean deleted = deleteFile(derby0log);
        assertTrue("File " + derby0log + " could not be deleted", deleted);
    }

    /**
     * Method getStream used by Derby when derby.stream.error.method
     * is set.  Maps to file <database>-method-<runNo>.log
     * This method has to be static.
     */
    public static OutputStream getStream() {
        return methodStream;
    }

    private static String makeStreamFilename(String type) {
        return type + "-" + runNo + ".log";
    }

    private void openStreams() throws IOException{
        String systemHome = getSystemProperty("derby.system.home");
        String logFilesHome=systemHome + File.separatorChar + LOGFILESDIR;
        makeDirIfNotExisting(systemHome);
        makeDirIfNotExisting(logFilesHome);

        runNo += 1;

        methodStreamFile = new File(logFilesHome, makeStreamFilename("method"));
        fileStreamFile = new File(logFilesHome, makeStreamFilename("file"));
        fieldStreamFile = new File(logFilesHome, makeStreamFilename("field"));
        errStreamFile = new File(logFilesHome, makeStreamFilename("err"));

        methodStream = newFileOutputStream(methodStreamFile);
        fieldStream = newFileOutputStream(fieldStreamFile);
        errStream = newFileOutputStream(errStreamFile);

        setSystemErr(new PrintStream(errStream));

    }


    private void closeStreams() throws IOException {
        try {
            methodStream.close();
            fieldStream.close();
            errStream.close();

            // reset until next scenario, no expected output
            setSystemErr(System.out);
        } catch (IOException e) {
            println("Could not close stream files");
            throw e;
        }
    }

    private static void assertNotDirectory(final File f) throws IOException {
        try {
            AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Void>() {
                public Void run() throws IOException {
                    assertFalse("assertNotDirectory failed: " +
                          f.getCanonicalPath(), f.isDirectory());
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static void assertIsEmpty(final File f) throws IOException {
        String path = getCanonicalPath(f);
        assertTrue(path + " doesn't exist",
                PrivilegedFileOpsForTests.exists(f));
        assertEquals(path + " is not empty",
                0, PrivilegedFileOpsForTests.length(f));
    }


    private static void assertNotEmpty(final File f) throws IOException {
        try {
            AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Void>() {
                public Void run() throws IOException {
                    assertTrue("assertNotEmpty failed: " + f.getCanonicalPath()
                          + " does not exist.", f.exists());
                    FileInputStream fis = new FileInputStream(f);
                    int result = fis.read();
                    fis.close();
                    assertTrue("assertNotEmpty failed: " + f.getCanonicalPath()
                          + " is empty.", -1 != result);
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static void assertFileSize(final File f, final int size) throws IOException {
        try {
            AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Void>() {
                public Void run() throws IOException {
                    assertEquals("assertFileEize failed for file " + f.getName() + ": ", size, f.length());
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static void assertIsExisting(final File f) throws IOException {
        String path = getCanonicalPath(f);
        assertTrue(path + " doesn't exist",
                PrivilegedFileOpsForTests.exists(f));
    }


    private static void assertNotExisting(final File f) throws IOException {
        String path = getCanonicalPath(f);
        assertFalse(path + " exists",
                PrivilegedFileOpsForTests.exists(f));
    }

    private static boolean deleteFile(final File f) {
        return PrivilegedFileOpsForTests.delete(f);
    }

    private static String getCanonicalPath(final File f) throws IOException {
        try {
            return AccessController.doPrivileged(
                  new PrivilegedExceptionAction<String>() {
                public String run() throws IOException {
                    return f.getCanonicalPath();
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static void makeDirIfNotExisting(final String filename) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                File f = new File(filename);
                if(!f.exists()) {
                    f.mkdir();
                }
                return null;
            }
        });
    }

    private static FileOutputStream newFileOutputStream(final File f)
    throws FileNotFoundException {
        try {
            return AccessController.doPrivileged(
                  new PrivilegedExceptionAction<FileOutputStream>() {
                public FileOutputStream run() throws FileNotFoundException {
                    return new FileOutputStream(f);
                }
            });
            } catch (PrivilegedActionException e) {
                // e.getException() should be a FileNotFoundException.
                throw (FileNotFoundException) e.getException();
            }
    }


    private static void resetProps() {
        removeSystemProperty(FILE_PROP);
        removeSystemProperty(METHOD_PROP);
        removeSystemProperty(FIELD_PROP);
        removeSystemProperty(STYLE_PROP);        
    }

    private void deleteStreamFiles() {
        deleteFile(fileStreamFile);
        deleteFile(methodStreamFile);
        deleteFile(fieldStreamFile);
        deleteFile(errStreamFile);
    }

    private void nullFields() {
        // Nulling fields to let objects be gc'd
        fileStreamFile = null;
        methodStreamFile = null;
        fieldStreamFile = null;
        errStreamFile = null;
        methodStream = null;
        fieldStream = null;
        errStream = null;
    }


    private void bootDerby() throws SQLException {
        /* Connect to the database to make sure that the
         * JDBC Driver class is loaded
         */
        getConnection();
        getConnection().close();
    }

    /**
     * <p>
     * Run the bare test, including {@code setUp()} and {@code tearDown()}.
     * </p>
     *
     * <p>
     * This is overriding BaseJDBCTestCase.runBareOverridable and thereby
     * BaseJDBCTestCase.runBare(), so we can copy any log files created by this
     * test if any of the fixtures fail. 
     * </p>
     */
    public void runBareOverridable() throws Throwable {
        PrintStream out = System.out;
        TestConfiguration config = getTestConfiguration();
        boolean stopAfterFirstFail = config.stopAfterFirstFail();
        try {
            super.runBareOverridable();   
        }
        // To log the exception to file, copy the derby.log file and copy
        // the database of the failed test.
        catch (Throwable running) {
            PrintWriter stackOut = null;
            try{
                copyFileToFail(LOGFILESDIR);
                nullFields();
                deleteFile(LOGFILESDIR);
                // copy files from testStyleRollingFile:
                copyFileToFail("derby-0.log");
                copyFileToFail("derby-0.log.lck");
                // copy files from the testDefaultRollingUserConfig test
                for (int i = 0; i < 3; i++) {
                    copyFileToFail("db-" + i + ".log");
                    deleteFile("db-" + i + ".log");
                }
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
            // attempt to clean up
            // first ensure we have the engine shutdown, or some
            // files cannot be cleaned up.
            getTestConfiguration().shutdownEngine();
            File origLogFilesDir = new File(DEFAULT_DB_DIR, LOGFILESDIR);
            nullFields();
            removeDirectory(origLogFilesDir);
            deleteFile("derby-0.log.lck");
            deleteFile("derby-0.log");
            deleteFile("derby.log");
        }
    }
    
    private void copyFileToFail(String origFileName) throws IOException {
        String failPath = PrivilegedFileOpsForTests.getAbsolutePath(getFailureFolder());
        File origFile = new File (DEFAULT_DB_DIR, origFileName); 
        File newFile = new File (failPath, origFileName);
        PrivilegedFileOpsForTests.copy(origFile,newFile);
    }
    
    // delete a file - used in cleanup when we don't care about the result
    private void deleteFile(String origFileName) throws IOException {
        File origFile = new File (DEFAULT_DB_DIR, origFileName);
        PrivilegedFileOpsForTests.delete(origFile);                
    }
}
