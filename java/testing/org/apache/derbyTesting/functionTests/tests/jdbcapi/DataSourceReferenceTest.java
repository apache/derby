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
import java.util.Arrays;

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
 * Test obtaining a javax.naming.Reference from a Derby data source
 * and recreating a Derby data source from it. Tests that the recreated
 * value has the same value for all the properties the data source supports.
 * The list of properties is obtained dynamically from the getXXX methods
 * that return int, String, boolean, short, long. Should Derby data sources
 * support any other bean property types then this test should be modified
 * to pick them up and handle them. Hopefully the test should fail when such
 * a property is added.
 * 
 * At no point does this test attempt to connect using these data sources.
 */
public class DataSourceReferenceTest extends BaseJDBCTestCase {

    private static String[][][] expectedValues = {
        // org.apache.derby.jdbc.Embedded*DataSource
        {{"attributesAsPassword", "false"}, null, null, null, null, null, 
         {"loginTimeout", "0"}, null, null, null}, 
        {{"attributesAsPassword", "true"}, 
         {"connectionAttributes", "XX_connectionAttributes_2135"},
         {"createDatabase", "create"},
         {"dataSourceName", "XX_dataSourceName_1420"},
         {"databaseName", "XX_databaseName_1206"},
         {"description", "XX_description_1188"},
         {"loginTimeout", "1280"},
         {"password", "XX_password_883"},
         {"shutdownDatabase", "shutdown"},
         {"user", "XX_user_447"}},
        // org.apache.derby.jdbc.Client*DataSource
        { null, null, null, null, null, {"loginTimeout", "0"}, null, 
         {"portNumber", "tmpportno"},
         {"retrieveMessageText", "true"},
         {"securityMechanism", "4"},
         {"serverName", "tmphostName"}, null, 
         {"ssl","off"}, null, null, 
         {"traceFileAppend", "false"},
         {"traceLevel", "-1"},
         {"user", "tmpUserName"}},
        {{"connectionAttributes", "XX_connectionAttributes_2135"},
         {"createDatabase", "create"},
         {"dataSourceName", "XX_dataSourceName_1420"},
         {"databaseName", "XX_databaseName_1206"},
         {"description", "XX_description_1188"},
         {"loginTimeout", "1280"},
         {"password", "XX_password_883"},
         {"portNumber", "1070"},
         {"retrieveMessageText", "false"},
         {"securityMechanism", "1805"},
         {"serverName", "XX_serverName_1048"},
         {"shutdownDatabase", "shutdown"},
         {"ssl","basic"},
         {"traceDirectory", "XX_traceDirectory_1476"},
         {"traceFile", "XX_traceFile_911"},
         {"traceFileAppend", "true"},
         {"traceLevel", "1031"},
         {"user", "XX_user_447"}}
    };
    
    public DataSourceReferenceTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        if (JDBC.vmSupportsJSR169())
        {
            // Referencable is not supported with JSR169
            TestSuite suite = 
                new TestSuite("DatasourceTest cannot run with JSR169");
            return suite;
        }
        else
        {
            return 
                TestConfiguration.defaultSuite(DataSourceReferenceTest.class);
        }
    }
    
    /**
     * Test a data source
     * <OL>
     * <LI> Create an empty one from the class name
     * <LI> Discover the property list
     * <LI> Create a reference and recreate a data source
     * <LI> Compare the two
     * <LI> Serialize the data source and recreate
     * <LI> Compare the two
     * <LI> Set every property for the data source
     * <LI> Create a reference and recreate a data source
     * <LI> Compare the two
     * </OL>
     * @throws Exception
     */
    public static void testDSReference() throws Exception
    {
        String ds;
        ds = JDBCDataSource.getDataSource().getClass().getName();
        int expectedArray=0;
        if (usingDerbyNetClient())
            expectedArray = 2;
        assertDataSourceReference(expectedArray, ds);
        ds = J2EEDataSource.getConnectionPoolDataSource().getClass().getName();
        assertDataSourceReference(expectedArray, ds);
        ds = J2EEDataSource.getXADataSource().getClass().getName();
        assertDataSourceReference(expectedArray, ds);
    }
        
    public static void assertDataSourceReference(
        int expectedArrayIndex, String dsName) throws Exception {

        if (usingDerbyNetClient())
        {
            expectedValues[expectedArrayIndex][7][1] =
                String.valueOf(TestConfiguration.getCurrent().getPort());
            expectedValues[expectedArrayIndex][10][1] =
                TestConfiguration.getCurrent().getHostName();
            expectedValues[expectedArrayIndex][17][1] =
                TestConfiguration.getCurrent().getUserName();
        }
        
        Object ds = Class.forName(dsName).newInstance();
        
        println("DataSource class " + dsName);
        String[] properties = getPropertyBeanList(ds);
        assertEquals(
            expectedValues[expectedArrayIndex+1].length, properties.length);
        println(" property list");
        
        for (int i = 0; i < properties.length; i++)
        {
            assertEquals(
                expectedValues[expectedArrayIndex+1][i][0], properties[i]);
            println("  " + properties[i]);
        }
        
        Referenceable refDS = (Referenceable) ds;
        
        Reference dsAsReference = refDS.getReference();
        
        String factoryClassName = dsAsReference.getFactoryClassName();
        
        ObjectFactory factory = 
            (ObjectFactory) Class.forName(factoryClassName).newInstance();  
        
        Object recreatedDS = 
            factory.getObjectInstance(dsAsReference, null, null, null);
        
        // DERBY-2559 - with jdk16, this recreatedDS will be null.
        // bailing out
        if (JDBC.vmSupportsJDBC4())
            return;
        
        println(" empty DataSource recreated using Reference as " +
            recreatedDS.getClass().getName());
        // empty DataSource recreated using Reference should not be 
        // the same as the original
        assertNotSame(recreatedDS, ds);
        
        compareDS(expectedArrayIndex, properties, ds, recreatedDS);
        
        // now serialize and recreate
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);  
        oos.writeObject(ds);
        oos.flush();
        oos.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        recreatedDS = ois.readObject();
        println(" empty DataSource recreated using serialization");
        compareDS(expectedArrayIndex, properties, ds, recreatedDS);
        
        // now populate the data source
        for (int i = 0; i < properties.length; i++)
        {
            String property = properties[i];
            Method getMethod = getGet(property, ds);
            
            Method setMethod = getSet(getMethod, ds);
            
            Class pt = getMethod.getReturnType();
            
            // generate a somewhat unique value for a property
            int val = 0;
            for (int j = 0; j < property.length(); j++)
                val += property.charAt(j);
            
            if (pt.equals(Integer.TYPE))
            {
                setMethod.invoke(ds, new Object[] {new Integer(val)});
                continue;
            }
            if (pt.equals(String.class))
            {
                String value;
                if (property.equals("createDatabase"))
                    value = "create";
                else if (property.equals("shutdownDatabase"))
                    value = "shutdown";
                else if (property.equals("ssl"))
                    value = "basic";
                else
                    value = "XX_" + property + "_" + val;
                    
                setMethod.invoke(ds, new Object[] {value});
                continue;
            }
            if (pt.equals(Boolean.TYPE))
            {
                // set the opposite value
                Object gbv = getMethod.invoke(ds, null);
                Boolean sbv = 
                    Boolean.FALSE.equals(gbv) ? Boolean.TRUE : Boolean.FALSE;
                setMethod.invoke(ds, new Object[] {sbv});
                continue;
            }           
            if (pt.equals(Short.TYPE))
            {
                setMethod.invoke(ds, new Object[] {new Short((short)val)});
                continue;
            }
            if (pt.equals(Long.TYPE))
            {
                setMethod.invoke(ds, new Object[] {new Long(val)});
                continue;
            }
            fail ( property + " not settable - update test!!");
        }
        
        dsAsReference = refDS.getReference();
        recreatedDS = 
            factory.getObjectInstance(dsAsReference, null, null, null);
        println(" populated DataSource recreated using Reference as " 
            + recreatedDS.getClass().getName());
        // again, recreated should not be same instance
        assertNotSame(recreatedDS, ds);
        
        compareDS(expectedArrayIndex+1, properties, ds, recreatedDS);     

        // now serialize and recreate
        baos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(baos); 
        oos.writeObject(ds);
        oos.flush();
        oos.close();
        bais = new ByteArrayInputStream(baos.toByteArray());
        ois = new ObjectInputStream(bais);
        recreatedDS = ois.readObject();
        println(" populated DataSource recreated using serialization");
        compareDS(expectedArrayIndex+1, properties, ds, recreatedDS);
    }
    
    private static String[] getPropertyBeanList(Object ds) throws Exception
    {
        Method[] allMethods = ds.getClass().getMethods();
        
        ArrayList properties = new ArrayList();
        for (int i = 0; i < allMethods.length; i++)
        {
            Method m = allMethods[i];
            String methodName = m.getName();
            // Need at least getXX
            if (methodName.length() < 5)
                continue;
            if (!methodName.startsWith("get"))
                continue;
            if (m.getParameterTypes().length != 0)
                continue;

            Class rt = m.getReturnType();
            
            if (rt.equals(Integer.TYPE) || rt.equals(String.class) || 
                rt.equals(Boolean.TYPE) || rt.equals(Short.TYPE) ||
                rt.equals(Long.TYPE))
            {
                // valid Java Bean property
                 String beanName = methodName.substring(3,4).toLowerCase() 
                     + methodName.substring(4);

                properties.add(beanName);
                continue;
            }
        
            
            assertFalse(rt.isPrimitive());
            println("if rt.isPrimitive, method " + methodName + 
                " not supported - update test!!");

        }
        
        String[] propertyList = (String[]) properties.toArray(new String[0]);
        
        Arrays.sort(propertyList);
        
        return propertyList;
    }
    
    private static Method getGet(String property, Object ds) throws Exception
    {
        String methodName =
            "get" + property.substring(0,1).toUpperCase()
            + property.substring(1);
        Method m = ds.getClass().getMethod(methodName, null);
        return m;
    }

    private static Method getSet(Method getMethod, Object ds) throws Exception
    {
        String methodName = "s" + getMethod.getName().substring(1);
        Method m = ds.getClass().getMethod(
            methodName, new Class[] {getMethod.getReturnType()});
        return m;
    }   

    private static void compareDS(int expectedValuesArrayIndex,
        String[] properties, Object ds, Object rds) throws Exception
    {
        println(" Start compare recreated");
        for (int i = 0; i < properties.length; i++)
        {
            Method getMethod = getGet(properties[i], ds);
            
            Object dsValue = getMethod.invoke(ds, null);
            Object rdsValue = getMethod.invoke(rds, null);
            
            if (dsValue == null)
            {
                // properties[i] originally null, should be recreated as null.
                assertNull(rdsValue);
            }
            else
            {
                // properties[i] originally dsValue, should be recreated as
                // rdsValue
                assertEquals(dsValue, rdsValue);
            }
            if (dsValue != null)
            {
                assertEquals(expectedValues[expectedValuesArrayIndex][i][0], 
                    properties[i]);
                assertEquals(expectedValues[expectedValuesArrayIndex][i][1], 
                    dsValue.toString());
            }
        }
    }
}