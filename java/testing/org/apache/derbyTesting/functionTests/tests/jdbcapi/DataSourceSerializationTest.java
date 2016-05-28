/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DataSourceSerializationTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * Makes sure that old serialized data sources can be de-serialized with the
 * current version of the data source.
 * <p>
 * Serialized data source from old versions are expected to be found in
 * <tt>testData/serializedDataSources</tt>, with the following filename
 * format CLASSNAME-VERSION.ser, where CLASSNAME is the unqualified name of the
 * data source class, and VERSION is the Derby version. An example:
 * <tt>ClientPooledConnectionDataSource-10_1.ser</tt>
 * <p>
 * A separation between JDBC 4.0 specific classes and the other classes is not
 * made before release 10.10.
 * <p>
 * This test should detect the typical incompatible changes in the current
 * data source implementations, for instance deleting a field or changing its
 * type.
 */
public class DataSourceSerializationTest
        extends BaseJDBCTestCase {

    /** Constant for Derby version 10.0.2.1. */
    private static final String VERSION_10_0_2_1 = "10_0_2_1";
    /** Constant for Derby version 10.1.3.1. */
    private static final String VERSION_10_1_3_1 = "10_1_3_1";
    /** Constant for Derby version 10.2.2.0 */
    private static final String VERSION_10_2_2_0 = "10_2_2_0";
    /** Constant for Derby version 10.3.2.1. */
    private static final String VERSION_10_3_2_1 = "10_3_2_1";
    /** Constant for Derby version 10.10.1.0. */
    private static final String VERSION_10_10_1_0 = "10_10_1_0";
    /** Constant for Derby version 10.11.1.0. */
    private static final String VERSION_10_11_1_0 = "10_11_1_0";
    private final String _40Suffix = "40";

    public DataSourceSerializationTest(String name) {
        super(name);
    }

    /**
     * Tests the de-serialization of the basic embedded data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestEmbeddedDataSource()
            throws Exception {
        if (JDBC.vmSupportsJNDI()) {
            final String EMBEDDED_CLASS = "EmbeddedDataSource";
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_0_2_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_1_3_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_2_2_0, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_3_2_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_10_1_0, true);
            deSerializeDs(EMBEDDED_CLASS + _40Suffix, VERSION_10_10_1_0, true);
        }
        
        final String EMBEDDED_CLASS = "BasicEmbeddedDataSource40";
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_10_1_0, false);
        
    }

    /**
     * Tests the de-serialization of the embedded connection pool data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestEmbeddedConnectionPoolDataSource()
            throws Exception {
        if (JDBC.vmSupportsJNDI()) {
            final String EMBEDDED_CLASS = "EmbeddedConnectionPoolDataSource";
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_0_2_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_1_3_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_2_2_0, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_3_2_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_10_1_0, true);
            deSerializeDs(EMBEDDED_CLASS + _40Suffix, VERSION_10_10_1_0, true);
        }

        final String EMBEDDED_CLASS =
                "BasicEmbeddedConnectionPoolDataSource40";
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_10_1_0, false);
    }

    /**
     * Tests the de-serialization of the embedded XA data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestEmbeddedXADataSource()
            throws Exception {
        if (JDBC.vmSupportsJNDI()) {
            final String EMBEDDED_CLASS = "EmbeddedXADataSource";
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_0_2_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_1_3_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_2_2_0, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_3_2_1, true);
            deSerializeDs(EMBEDDED_CLASS, VERSION_10_10_1_0, true);
            deSerializeDs(EMBEDDED_CLASS + _40Suffix, VERSION_10_10_1_0, true);
        }
        
        final String EMBEDDED_CLASS = "BasicEmbeddedXADataSource40";
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_10_1_0, false);
    }

    /**
     * Tests the de-serialization of the basic client data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestClientDataSource()
            throws Exception {
        if (JDBC.vmSupportsJNDI()) {
            final String CLIENT_CLASS = "ClientDataSource";
            // No client driver for Derby 10.0
            deSerializeDs(CLIENT_CLASS, VERSION_10_1_3_1, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_2_2_0, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_3_2_1, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_10_1_0, true);
            deSerializeDs(CLIENT_CLASS + _40Suffix, VERSION_10_10_1_0, true);
        }
        
        final String CLIENT_CLASS = "BasicClientDataSource40";
        deSerializeDs(CLIENT_CLASS, VERSION_10_10_1_0, false);
    }

    /**
     * Tests the de-serialization of the client connection pool data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestClientConnectionPoolDataSource()
            throws Exception {
        if (JDBC.vmSupportsJNDI()) {
            final String CLIENT_CLASS = "ClientConnectionPoolDataSource";
            // No client driver for Derby 10.0
            deSerializeDs(CLIENT_CLASS, VERSION_10_1_3_1, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_2_2_0, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_3_2_1, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_10_1_0, true);
            deSerializeDs(CLIENT_CLASS + _40Suffix, VERSION_10_10_1_0, true);
        }
         
        final String CLIENT_CLASS = "BasicClientConnectionPoolDataSource40";
        deSerializeDs(CLIENT_CLASS, VERSION_10_10_1_0, false);
    }

    /**
     * Tests the de-serialization of the client XA data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestClientXADataSource()
            throws Exception {
        if (JDBC.vmSupportsJNDI()) {
            final String CLIENT_CLASS = "ClientXADataSource";
            // No client driver for Derby 10.0
            deSerializeDs(CLIENT_CLASS, VERSION_10_1_3_1, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_2_2_0, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_3_2_1, true);
            deSerializeDs(CLIENT_CLASS, VERSION_10_10_1_0, true);
            deSerializeDs(CLIENT_CLASS + _40Suffix, VERSION_10_10_1_0, true);
        }
         
        final String CLIENT_CLASS = "BasicClientXADataSource40";
        deSerializeDs(CLIENT_CLASS, VERSION_10_10_1_0, false);
    }

    /**
     * Attempts to de-serialize a data source object from a file.
     * <p>
     * <ol> <li>Derby version string - UTF</li>
     *      <li>Derby build number - UTF</li>
     *      <li>Derby data source - object</li>
     *      <li>Derby data source reference - object</li>
     * </ol>
     * <p>
     * If the object is successfully instantiated and cast to
     * {@link javax.sql.DataSource}
     *
     * @param className name of the class to de-serialize
     * @param version Derby version
     *
     * @throws Exception on a number of error conditions
     */
    private void deSerializeDs(
            String className, String version, boolean dsHasJNDI)
            throws Exception {

        if (!JDBC.vmSupportsJDBC4() && className.contains("40")) {
            // Running old Java, bail out if JDBC4
            return;
        }

        // Construct the filename
        final StringBuffer fname = new StringBuffer(className);
        fname.append('-');
        fname.append(version);
        fname.append(".ser");
        println( "Deserializing " + fname.toString() );

        // De-serialize the data source.
        InputStream is;
        try {
            is = AccessController.doPrivileged(
                  new PrivilegedExceptionAction<InputStream>() {
                public InputStream run() throws FileNotFoundException {
                    return new FileInputStream(
                            SupportFilesSetup.getReadOnly(fname.toString()));
                }
            });
            } catch (PrivilegedActionException e) {
                // e.getException() should be a FileNotFoundException.
                throw (FileNotFoundException)e.getException();
            }

        assertNotNull("FileInputStream is null", is);
        Object dsObj = null;
        DataSource ds = null;
        Object dsRef = null;
        // Used to preserve original error information in case of exception when 
        // closing the input stream.
        boolean testSequencePassed = false;
        try {
            ObjectInputStream ois = new ObjectInputStream(is);
            String buildVersion = ois.readUTF();
            String buildNumber = ois.readUTF();
            println("Data source " + className + ", version " +
                    buildVersion + ", build " + buildNumber);
            dsObj = ois.readObject();
            assertNotNull("De-serialized data source is null", dsObj);
            assertTrue("Unexpected class instantiated: " +
                    dsObj.getClass().getName(),
                    dsObj.getClass().getName().indexOf(className) > 0);
            ds = (DataSource)dsObj;
            // Just see if the object is usable.
            int newTimeout = ds.getLoginTimeout() +9;
            assertFalse(ds.getLoginTimeout() == newTimeout);
            ds.setLoginTimeout(newTimeout);
            assertEquals(newTimeout, ds.getLoginTimeout());

            if (dsHasJNDI) {
                // Recreate the data source using reference.
                dsRef = ois.readObject();
            }
            ois.close();
            testSequencePassed = true;
        } finally {
            if (testSequencePassed) {
                is.close();
            } else {
                try {
                    is.close();
                } catch (IOException ioe) {
                    // Ignore this to preserve the original exception.
                }
            }
        }

        if (dsHasJNDI) {
            // Recreate ds via the Reference's factory class.  We use
            // reflection here to make the test runnable for non JNDI
            // environments. (Even though this code would not be executed in
            // that environment, the VM will try to load the classes if
            // reference directly in the source. So, resort to reflection...)
            Method getFactoryClassName =
                    Class.forName("javax.naming.Reference").getMethod(
                    "getFactoryClassName", null);
            String factoryClassName =
                    (String)getFactoryClassName.invoke(dsRef, null);
            Class<?> clazz = Class.forName(factoryClassName);
            Object factory = clazz.getConstructor().newInstance();
            Method getObjectInstance =
                    factory.getClass().getMethod("getObjectInstance",
                    new Class[] {
                        Class.forName("java.lang.Object"),
                        Class.forName("javax.naming.Name"),
                        Class.forName("javax.naming.Context"),
                        Class.forName( "java.util.Hashtable")});
            Object recreatedDs =
                    getObjectInstance.invoke(
                    factory, new Object[] {dsRef, null, null, null});
            ds = (DataSource)recreatedDs;
            assertTrue("Unexpected class instantiated by Reference: " +
                    dsObj.getClass().getName(),
                    dsObj.getClass().getName().indexOf(className) > 0);
        }
    }

    /**
     * Returns an appropariate suite of tests to run.
     *
     * @return A test suite.
     */
    public static Test suite() {
        BaseTestSuite suite =
            new BaseTestSuite("DataSourceSerializationTest");

        String filePrefix = "functionTests/testData/serializedDataSources/";
        // De-serialize embedded data sources only if we have the engine code.
        if (Derby.hasEmbedded()) {
            suite.addTest(new DataSourceSerializationTest(
                    "serTestEmbeddedDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestEmbeddedConnectionPoolDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestEmbeddedXADataSource"));
        }

        // De-serialize client data sources only if we have the client code.
        if (Derby.hasClient()) {
            suite.addTest(new DataSourceSerializationTest(
                    "serTestClientDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestClientConnectionPoolDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestClientXADataSource"));
        }

        return new SupportFilesSetup(suite, new String[] {
                // 10.0 resources
                filePrefix + "EmbeddedDataSource-10_0_2_1.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_0_2_1.ser",
                filePrefix + "EmbeddedXADataSource-10_0_2_1.ser",

                // 10.1 resources
                filePrefix + "EmbeddedDataSource-10_1_3_1.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_1_3_1.ser",
                filePrefix + "EmbeddedXADataSource-10_1_3_1.ser",
                filePrefix + "ClientDataSource-10_1_3_1.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_1_3_1.ser",
                filePrefix + "ClientXADataSource-10_1_3_1.ser",

                // 10.2 resources
                filePrefix + "EmbeddedDataSource-10_2_2_0.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_2_2_0.ser",
                filePrefix + "EmbeddedXADataSource-10_2_2_0.ser",
                filePrefix + "ClientDataSource-10_2_2_0.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_2_2_0.ser",
                filePrefix + "ClientXADataSource-10_2_2_0.ser",

                // 10.3 resources
                filePrefix + "EmbeddedDataSource-10_3_2_1.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_3_2_1.ser",
                filePrefix + "EmbeddedXADataSource-10_3_2_1.ser",
                filePrefix + "ClientDataSource-10_3_2_1.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_3_2_1.ser",
                filePrefix + "ClientXADataSource-10_3_2_1.ser",

                // 10.10 resources
                filePrefix + "EmbeddedDataSource-10_10_1_0.ser",
                filePrefix + "EmbeddedDataSource40-10_10_1_0.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_10_1_0.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource40-10_10_1_0.ser",
                filePrefix + "EmbeddedXADataSource-10_10_1_0.ser",
                filePrefix + "EmbeddedXADataSource40-10_10_1_0.ser",
                filePrefix + "ClientDataSource-10_10_1_0.ser",
                filePrefix + "ClientDataSource40-10_10_1_0.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_10_1_0.ser",
                filePrefix + "ClientConnectionPoolDataSource40-10_10_1_0.ser",
                filePrefix + "ClientXADataSource-10_10_1_0.ser",
                filePrefix + "ClientXADataSource40-10_10_1_0.ser",
                filePrefix + "BasicEmbeddedDataSource40-10_10_1_0.ser",
                filePrefix +
                    "BasicEmbeddedConnectionPoolDataSource40-10_10_1_0.ser",
                filePrefix + "BasicEmbeddedXADataSource40-10_10_1_0.ser",
                filePrefix + "BasicClientDataSource40-10_10_1_0.ser",
                filePrefix +
                    "BasicClientConnectionPoolDataSource40-10_10_1_0.ser",
                filePrefix + "BasicClientXADataSource40-10_10_1_0.ser",

                // 10.11 resources
                filePrefix + "EmbeddedDataSource-10_11_1_0.ser",
                filePrefix + "EmbeddedDataSource40-10_11_1_0.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_11_1_0.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource40-10_11_1_0.ser",
                filePrefix + "EmbeddedXADataSource-10_11_1_0.ser",
                filePrefix + "EmbeddedXADataSource40-10_11_1_0.ser",
                filePrefix + "ClientDataSource-10_11_1_0.ser",
                filePrefix + "ClientDataSource40-10_11_1_0.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_11_1_0.ser",
                filePrefix + "ClientConnectionPoolDataSource40-10_11_1_0.ser",
                filePrefix + "ClientXADataSource-10_11_1_0.ser",
                filePrefix + "ClientXADataSource40-10_11_1_0.ser",
                filePrefix + "BasicEmbeddedDataSource40-10_11_1_0.ser",
                filePrefix +
                    "BasicEmbeddedConnectionPoolDataSource40-10_11_1_0.ser",
                filePrefix + "BasicEmbeddedXADataSource40-10_11_1_0.ser",
                filePrefix + "BasicClientDataSource40-10_11_1_0.ser",
                filePrefix +
                    "BasicClientConnectionPoolDataSource40-10_11_1_0.ser",
                filePrefix + "BasicClientXADataSource40-10_11_1_0.ser",
            });
    }
}

