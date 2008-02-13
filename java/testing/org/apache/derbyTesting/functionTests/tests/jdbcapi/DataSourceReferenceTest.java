/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DataSourceReferenceTest

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.spi.ObjectFactory;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Test obtaining a <code>javax.naming.Reference</code> from a Derby data
 * source and recreating a Derby data source from it.
 * <p>
 * Tests that the recreated value has the same value for all the properties
 * the data source supports. The list of properties is obtained
 * dynamically from the getter methods that return int, String, boolean,
 * short and long. Should Derby data sources support any other bean
 * property types then this test should be modified to pick them up and
 * handle them. The test will fail when such a property is added.
 * <p>
 * Default values of the properties are also tested. Default and set
 * values of the properties must be specified by creating a data source
 * descriptor.
 * <p>
 * At no point does this test attempt to connect using these data sources.
 */
public class DataSourceReferenceTest
        extends BaseJDBCTestCase {

    /** Lookup constant for the descriptor array. */
    private static final int BASE_DS = 0;
    /** Lookup constant for the descriptor array. */
    private static final int POOL_DS = 1;
    /** Lookup constant for the descriptor array. */
    private static final int XA_DS = 2;

    /** Descriptor for the basic embedded data source. */
    private static final DataSourceDescriptor BASE_EMBEDDED_DS =
            new DataSourceDescriptor("Basic embedded data source");

    static {
        BASE_EMBEDDED_DS.addProperty("attributesAsPassword", "true", "false");
        BASE_EMBEDDED_DS.addProperty("connectionAttributes",
                                     "XX_connectionAttributes_2135");
        BASE_EMBEDDED_DS.addProperty("createDatabase", "create");
        BASE_EMBEDDED_DS.addProperty("dataSourceName",
                                     "XX_dataSourceName_1420");
        BASE_EMBEDDED_DS.addProperty("databaseName", "XX_databaseName_1206");
        BASE_EMBEDDED_DS.addProperty("description", "XX_description_1188");
        BASE_EMBEDDED_DS.addProperty("loginTimeout", "1280", "0");
        BASE_EMBEDDED_DS.addProperty("password", "XX_password_883");
        BASE_EMBEDDED_DS.addProperty("shutdownDatabase", "shutdown");
        BASE_EMBEDDED_DS.addProperty("user", "XX_user_447");
    }

    /** Descriptor for the basic client data source. */
    private static final DataSourceDescriptor BASE_CLIENT_DS =
            new DataSourceDescriptor("Basic client data source");

    static {
        // Properties with default values
        BASE_CLIENT_DS.addProperty("loginTimeout", "1280", "0");
        BASE_CLIENT_DS.addProperty("portNumber", "1070", "1527");
        BASE_CLIENT_DS.addProperty("retrieveMessageText", "false", "true");
        BASE_CLIENT_DS.addProperty("securityMechanism", "1851", "4");
        BASE_CLIENT_DS.addProperty("serverName", "tmpHostName", "localhost");
        BASE_CLIENT_DS.addProperty("ssl", "basic", "off");
        BASE_CLIENT_DS.addProperty("user", "XX_user_447", "APP");
        // Properties without default values.
        BASE_CLIENT_DS.addProperty("connectionAttributes",
                                   "XX_connectionAttributes_2135");
        BASE_CLIENT_DS.addProperty("createDatabase", "create");
        BASE_CLIENT_DS.addProperty("databaseName", "XX_databaseName_1206");
        BASE_CLIENT_DS.addProperty("dataSourceName", "XX_dataSourceName_1420");
        BASE_CLIENT_DS.addProperty("description", "XX_description_1188");
        BASE_CLIENT_DS.addProperty("password", "XX_password_883");
        BASE_CLIENT_DS.addProperty("shutdownDatabase", "shutdown");
        BASE_CLIENT_DS.addProperty("traceFile", "XX_traceFile_911");
        BASE_CLIENT_DS.addProperty("traceFileAppend", "true", "false");
        BASE_CLIENT_DS.addProperty("traceLevel", "1031", "-1");
        BASE_CLIENT_DS.addProperty("traceDirectory", "XX_traceDirectory_1476");
    }

    /** Descriptor for the client connection pool data source. */
    private static final DataSourceDescriptor POOL_CLIENT_DS =
            new DataSourceDescriptor("Connection pool client data source",
                                     BASE_CLIENT_DS);

    static {
        POOL_CLIENT_DS.addProperty("maxStatements", "10", "0");
    }

    /**
     * Creates a new fixture.
     *
     * @param name fixture name
     */
    public DataSourceReferenceTest(String name) {
        super(name);
    }

    /**
     * Creates a suite with tests for both embedded and client data sources.
     *
     * @return A suite with the appropriate tests.
     */
    public static Test suite() {
       Test suite;
       if (JDBC.vmSupportsJSR169()) {
            // Referenceable is not supported with JSR169
            suite = new TestSuite("DatasourceTest cannot run with JSR169");
        } else {
            suite = TestConfiguration.defaultSuite(
                                                DataSourceReferenceTest.class);
        }
       return suite;
    }

    /**
     * Tests a data source, with focus on serialization/deserialization.
     * <p>
     * For each data source, the following actions are performed:
     * <ol> <li>Create an empty data source from the class name.
     *      <li>Discover and validate the bean property list.
     *      <li>Create a reference and recreate the data source.
     *      <li>Compare the original and the empty recreated data source.
     *      <li>Serialize the data source and recreate.
     *      <li>Compare the original and the deserialized data source.
     *      <li>Set a value for every property of the data source.
     *      <li>Create a reference and recreate the data source.
     *      <li>Compare the populated original and the recreated data source.
     *      <li>Serialize the populated data source and recreate.
     *      <li>Compare the populated original and the deserialized data source.
     * </ol>
     *
     * @throws Exception on a wide variety of error conditions...
     */
    public void testDataSourceReference()
            throws Exception {
        DataSourceDescriptor[] descriptors;
        if (usingDerbyNetClient()) {
            // Specify client data source descriptors.
            descriptors = new DataSourceDescriptor[] {
                BASE_CLIENT_DS, // Base
                POOL_CLIENT_DS, // Pool
                BASE_CLIENT_DS  // XA
            };
        } else {
            // Specify embedded data source descriptors.
            descriptors = new DataSourceDescriptor[] {
                BASE_EMBEDDED_DS, // Base
                BASE_EMBEDDED_DS, // Pool
                BASE_EMBEDDED_DS  // XA
            };
        }
        // Test basic data source.
        String className = JDBCDataSource.getDataSource().getClass().getName();
        println("Testing base data source: " + className);
        assertDataSourceReference(descriptors[BASE_DS], className);

        // Test connection pool data source.
        className =
              J2EEDataSource.getConnectionPoolDataSource().getClass().getName();
        println("Testing connection pool data source: " + className);
        assertDataSourceReference(descriptors[POOL_DS], className);

        // Test XA data source.
        className = J2EEDataSource.getXADataSource().getClass().getName();
        println("Testing XA data source: " + className);
        assertDataSourceReference(descriptors[XA_DS], className);
    }


    /**
     * Performs the test sequence in the data source.
     *
     * @param dsDesc data source descriptor
     * @param className class name of the data source
     * @throws Exception on a wide variety of error conditions...
     *
     * @see #testDataSourceReference
     */
    private void assertDataSourceReference(
                                        DataSourceDescriptor dsDesc,
                                        String className)
        throws Exception {
        // Instantiate a new data source object and get all its properties.
        Object dsObj = Class.forName(className).newInstance();
        String[] properties = getPropertyBeanList(dsObj);
        // Validate property set (existence and naming).
        assertDataSourceProperties(dsDesc, properties);
        // Test recreating the data source
        assertDataSourceReferenceEmpty(dsDesc, className);
        assertDataSourceReferencePopulated(dsDesc, className);
    }

    /**
     * Asserts that the properties that are in the data source descriptor are
     * found in the list of data source properties, and that the data source
     * does not contain properties that are not in the descriptor.
     * <p>
     * No property values are verified in this assert method.
     *
     * @param dsDesc data source descriptor
     * @param properties list of actual data source properties
     */
    private void assertDataSourceProperties(
                                        DataSourceDescriptor dsDesc,
                                        String[] properties) {
        println("Testing data source bean properties.");
        // Validate the identified property names.
        for (int i=0; i < properties.length; i++) {
            assertTrue("Property '" + properties[i] + "' not in descriptor '" +
                    dsDesc.getName() + "'",
                    dsDesc.hasProperty(properties[i]));
        }
        // Check that all keys defined by the descriptor is found, and that
        // there is only one of each in the data source property list.
        Iterator descPropIter = dsDesc.getPropertyIterator();
        while (descPropIter.hasNext()) {
            String descProp = (String)descPropIter.next();
            boolean match = false;
            // Iterate through all the data source properties.
            for (int i=0; i < properties.length; i++) {
                if (properties[i].equals(descProp)) {
                    if (match) {
                        fail("Duplicate entry '" + descProp + "' in data " +
                                "source property list");
                    }
                    // Don't break, continue to look for duplicates.
                    match = true;
                }
            }
            assertTrue("Property '" + descProp + "' not found in data source " +
                    "property list", match);
        }
        // Check if the expected number of properties are found.
        // Do this last to hopefully get a more descriptive failure
        // message which includes the property name above.
        assertEquals(dsDesc.getPropertyCount(), properties.length);
    }

    /**
     * Make sure it is possible to create a new data source using
     * <code>Referencable</code>, that the new instance has the correct
     * default values set for the bean properties and finally that the
     * data source can be serialized/deserialized.
     *
     * @param dsDesc data source descriptor
     * @param className data source class name
     * @throws Exception on a wide variety of error conditions...
     */
    private void assertDataSourceReferenceEmpty(DataSourceDescriptor dsDesc,
                                                String className)
            throws Exception {
        println("Testing recreated empty data source.");
        // Create an empty data source.
        Object ds = Class.forName(className).newInstance();
        Referenceable refDs = (Referenceable)ds;
        Reference dsAsReference = refDs.getReference();
        String factoryClassName = dsAsReference.getFactoryClassName();
        ObjectFactory factory =
            (ObjectFactory)Class.forName(factoryClassName).newInstance();
        Object recreatedDs =
            factory.getObjectInstance(dsAsReference, null, null, null);
        // Empty, recreated data source should not be the same as the one we
        // created earlier on.
        assertNotNull("Recreated datasource is <null>", recreatedDs);
        assertNotSame(recreatedDs, ds);
        compareDataSources(dsDesc, ds, recreatedDs, true);

        // Serialize and recreate data source with default values.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(ds);
        oos.flush();
        oos.close();
        ByteArrayInputStream bais =
                new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        recreatedDs = ois.readObject();
        compareDataSources(dsDesc, ds, recreatedDs, true);
    }

    /**
     * Make sure it is possible to recreate and serialize/deserialize a
     * populated data source.
     * <p>
     * Populated means the various bean properties have non-default
     * values set.
     *
     * @param dsDesc data source descriptor
     * @param className data source class name
     * @throws Exception on a wide variety of error conditions...
     */
    private void assertDataSourceReferencePopulated(
                                                DataSourceDescriptor dsDesc,
                                                String className)
            throws Exception {
        println("Testing recreated populated data source.");
        Object ds = Class.forName(className).newInstance();
        // Populate the data source.
        Iterator propIter = dsDesc.getPropertyIterator();
        while (propIter.hasNext()) {
            String property = (String)propIter.next();
            String value = dsDesc.getPropertyValue(property);
            Method getMethod = getGet(property, ds);
            Method setMethod = getSet(getMethod, ds);
            Class paramType = getMethod.getReturnType();

            if (paramType.equals(Integer.TYPE)) {
                setMethod.invoke(ds, new Object[] {Integer.valueOf(value)});
            } else if (paramType.equals(String.class)) {
                setMethod.invoke(ds, new Object[] {value});
            } else if (paramType.equals(Boolean.TYPE)) {
                setMethod.invoke(ds, new Object[] {Boolean.valueOf(value)});
            } else if (paramType.equals(Short.TYPE)) {
                setMethod.invoke(ds, new Object[] {Short.valueOf(value)});
            } else if (paramType.equals(Long.TYPE)) {
                setMethod.invoke(ds, new Object[] {Long.valueOf(value)});
            } else {
                fail("'" + property + "' not settable - update test!!");
            }
        }

        Referenceable refDs = (Referenceable)ds;
        Reference dsAsReference = refDs.getReference();
        String factoryClassName = dsAsReference.getFactoryClassName();
        ObjectFactory factory =
            (ObjectFactory)Class.forName(factoryClassName).newInstance();
        Object recreatedDs =
            factory.getObjectInstance(dsAsReference, null, null, null);
        // Recreated should not be same instance as original.
        assertNotSame(recreatedDs, ds);
        compareDataSources(dsDesc, ds, recreatedDs, false);

        // Serialize and recreate.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(ds);
        oos.flush();
        oos.close();
        ByteArrayInputStream bais =
                new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        recreatedDs = ois.readObject();
        compareDataSources(dsDesc, ds, recreatedDs, false);
    }

    /**
     * Compares two data sources expected to be equal.
     * <p>
     * The data source descriptor is expected to contain both default values
     * and set values for the relevant bean properties of the data source(s).
     *
     * @param dsDesc data source descriptor
     * @param ds original data source
     * @param rds recreated data source
     * @param useDefaultsForComparison <code>true</code> if the default values
     *      should be verified, <code>false</code> if the set values should be
     *      used for verification
     * @throws Exception on a wide variety of error conditions...
     * @throws AssertionFailedError if the data sources are not equal
     */
    private void compareDataSources(DataSourceDescriptor dsDesc,
                                       Object ds, Object rds,
                                       boolean useDefaultsForComparison)
            throws Exception {
        Iterator propIter = dsDesc.getPropertyIterator();
        while (propIter.hasNext()) {
            String property = (String)propIter.next();
            Method getMethod = getGet(property, ds);

            // Obtain value from original data source, then the recreated one.
            Object dsValue = getMethod.invoke(ds, null);
            Object rdsValue = getMethod.invoke(rds, null);

            if (dsValue == null) {
                assertNull(rdsValue);
            } else {
                assertEquals(dsValue, rdsValue);
            }
            // Make sure the value is correct.
            if (useDefaultsForComparison) {
                if (dsValue != null) {
                    assertEquals("Wrong default value for '" + property + "'",
                            dsDesc.getPropertyDefault(property),
                            dsValue.toString());
                } else {
                    assertNull(dsDesc.getPropertyDefault(property));
                }
            } else if (dsValue != null) {
                    assertEquals("'" + property + "' has incorrect value",
                            dsDesc.getPropertyValue(property),
                            dsValue.toString());
            } else {
                // We got null from the data source, and we should have set all
                // values to something else than null.
                fail("Test does not handle this situation...");
            }
        }
    }

    /**
     * Obtains a list of bean properties through reflection.
     *
     * @param ds the data source to investigate
     * @return A list of bean property names.
     */
    private static String[] getPropertyBeanList(Object ds) {
        Method[] allMethods = ds.getClass().getMethods();
        ArrayList properties = new ArrayList();

        for (int i = 0; i < allMethods.length; i++) {
            Method method = allMethods[i];
            String methodName = method.getName();
            // Need at least getXX
            if (methodName.length() < 5 || !methodName.startsWith("get") ||
                    method.getParameterTypes().length != 0) {
                continue;
            }

            Class rt = method.getReturnType();
            if (rt.equals(Integer.TYPE) || rt.equals(String.class) ||
                    rt.equals(Boolean.TYPE) || rt.equals(Short.TYPE) ||
                    rt.equals(Long.TYPE)) {
                // Valid Java Bean property.
                // Convert name:
                //    getPassword -> password
                //    getRetrieveMessageText -> retrieveMessageText
                String beanName = methodName.substring(3,4).toLowerCase()
                        + methodName.substring(4);

                properties.add(beanName);
            } else {
                assertFalse("Method '" + methodName + "' with primitive " +
                    "return type not supported - update test!!",
                    rt.isPrimitive());
            }
        }

        return (String[])properties.toArray(new String[properties.size()]);
    }

    /**
     * Obtains the specified get method.
     *
     * @param property property/method name
     * @param ds data source object
     * @return A method object.
     *
     * @throws NoSuchMethodException if the method does not exist
     */
    private static Method getGet(String property, Object ds)
            throws NoSuchMethodException {
        String methodName =
            "get" + property.substring(0,1).toUpperCase()
            + property.substring(1);
        Method m = ds.getClass().getMethod(methodName, null);
        return m;
    }

    /**
     * Obtains the specified set method.
     *
     * @param getMethod the corresponding get method
     * @param ds data source object
     * @return A method object.
     *
     * @throws NoSuchMethodException if the method does not exist
     */private static Method getSet(Method getMethod, Object ds)
            throws NoSuchMethodException {
        String methodName = "s" + getMethod.getName().substring(1);
        Method m = ds.getClass().getMethod(
            methodName, new Class[] {getMethod.getReturnType()});
        return m;
    }

    /**
     * A class describing the bean properties of a data source.
     * <p>
     * A data source is a class implementing
     * <code>javax.sql.CommonDataSource</code>.
     * <p>
     * The data source description consists of the following:
     * <ul> <li>A list of property names.
     *      <li>A list of default values for the properties that have a default.
     *      <li>A list of set values for properties.
     * </ul>
     * In addition it has a name for convenience.
     */
    private static class DataSourceDescriptor {

        /** Name of the description. */
        private final String dsName;
        /**
         * Set values for the data source being described.
         * <p>
         * Note that the keys of this property object describe which bean
         * properties exist for the data source.
         */
        private final Properties propertyValues;
        /**
         * Default values for bean properties having a default.
         * <p>
         * Note that not all properties have a default, and the data source
         * may therefore have more properties than there entries in this
         * list of properties.
         */
        private final Properties propertyDefaults;

        /**
         * Creates a new data source description.
         *
         * @param dsName convenience name for the description/source
         */
        DataSourceDescriptor(String dsName) {
            this.dsName = dsName;
            this.propertyValues = new Properties();
            this.propertyDefaults = new Properties();
        }

        /**
         * Creates a new data source description, based off an existing
         * description.
         * <p>
         * All properties and values defined in the existing descriptor will
         * also be defined in the new descriptor.
         *
         * @param dsName convenience name for the description/source
         * @param copyFrom existing descriptor to copy properties/values from
         */
        DataSourceDescriptor(String dsName, DataSourceDescriptor copyFrom) {
            this.dsName = dsName;
            this.propertyValues = new Properties();
            this.propertyValues.putAll(copyFrom.propertyValues);
            this.propertyDefaults = new Properties(copyFrom.propertyDefaults);
            this.propertyDefaults.putAll(copyFrom.propertyDefaults);
        }

        /**
         * Returns the convenience name of this descriptor.
         *
         * @return A convenience name.
         */
        String getName() {
            return this.dsName;
        }

        /**
         * Adds a property to the description, with a value and no associated
         * default value.
         *
         * @param name property name
         * @param value property value
         * @throws NullPointerException if <code>name</code> or
         *      <code>value</code> is <code>null</code>
         */
        void addProperty(String name, String value) {
            this.propertyValues.setProperty(name, value);
        }

        /**
         * Adds a property to the description, with a value and an associated
         * default value.
         *
         * @param name property name
         * @param value property value
         * @param defaultValue default property value
         * @throws NullPointerException if <code>name</code>, <code>value</code>
         *      or <code>defaultValue</code> is <code>null</code>
         */
        void addProperty(String name, String value, String defaultValue) {
            this.propertyValues.setProperty(name, value);
            this.propertyDefaults.setProperty(name, defaultValue);
        }

        /**
         * Returns the value of the specified property.
         *
         * @param name property name
         * @return The value set for this property.
         *
         * @throws NullPointerException if <code>name</code> is
         *      <code>null</code>
         * @throws AssertionFailedError if the property name is not defined by
         *      this descriptor
         */
        String getPropertyValue(String name) {
            if (!this.propertyValues.containsKey(name)) {
                fail("Property '" + name + "' not in data source descriptor '" +
                        dsName + "'");
            }
            return this.propertyValues.getProperty(name);
        }

        /**
         * Returns the default value for the specified property.
         *
         * @param name property name
         * @return The default value if specified, <code>null<code> if a default
         *      value is not specified.
         *
         * @throws NullPointerException if <code>name</code> is
         *      <code>null</code>
         * @throws AssertionFailedError if the property name is not defined by
         *      this descriptor
         */
        String getPropertyDefault(String name) {
            if (!this.propertyValues.containsKey(name)) {
                fail("Property '" + name + "' not in data source descriptor '" +
                        dsName + "'");
            }
            return this.propertyDefaults.getProperty(name, null);
        }

        /**
         * Returns an iterator over all bean property names.
         *
         * @return An iterator.
         */
        Iterator getPropertyIterator() {
            return this.propertyValues.keySet().iterator();
        }

        /**
         * Tells if the specified property is defined by this descriptor.
         *
         * @param name property name
         * @return <code>true</code> if defined, <code>false</code> if not.
         */
        boolean hasProperty(String name) {
            return this.propertyValues.containsKey(name);
        }

        /**
         * Returns the number of bean properties defined by this descriptor.
         *
         * @return The number of bean properties.
         */
        int getPropertyCount() {
            return this.propertyValues.size();
        }
    } // End class DataSourceDescriptor
}
