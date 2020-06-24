/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.XAJNDITest

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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.sql.XADataSource;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class XAJNDITest extends BaseJDBCTestCase {
    private static String ldapServer;
    private static String ldapPort;
    private static String dnString;
    private InitialDirContext ic = getInitialDirContext();

    public XAJNDITest(String name) {
        super(name);
    }

    public static Test suite() {
        // the test requires XADataSource to run, so check for JDBC3 support
        if (JDBC.vmSupportsJDBC3()) {
            ldapServer=getSystemProperty("derbyTesting.ldapServer");

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            if (ldapServer == null || ldapServer.length() < 1) {
                return new BaseTestSuite(
                    "XAJNDITest requires property derbyTesting.ldapServer " +
                    "set, eg: -DderbyTesting.ldapServer=myldapserver." +
                    "myorg.org");
            }

            ldapPort=getSystemProperty("derbyTesting.ldapPort");

            if (ldapPort == null || ldapPort.length() < 1) {
                return new BaseTestSuite(
                    "XAJNDITest requires property derbyTesting.ldapPort " +
                    "set, eg: -DderbyTesting.ldapPort=333");
            }

            dnString=getSystemProperty("derbyTesting.dnString");

            if (dnString == null || dnString.length() < 1) {
                return new BaseTestSuite(
                    "XAJNDITest requires property derbyTesting.dnString " +
                    "for setting o=, eg: -DderbyTesting.dnString=myJNDIstring");
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-3659
            return TestConfiguration.defaultSuite(XAJNDITest.class);
        }
        return new BaseTestSuite("XAJNDITest cannot run without XA support");
    }
    
    public void tearDown() throws Exception {
        ldapServer=null;
        ldapPort=null;
        // need to hold on to dnString value and ic as the fixture runs
        // twice (embedded & networkserver) and they're used inside it
        super.tearDown();
    }

    private InitialDirContext getInitialDirContext()
    {
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            Hashtable<String, String> env = new Hashtable<String, String>();
            // using properties - these will have been passed in.
//IC see: https://issues.apache.org/jira/browse/DERBY-3972
            String ldapContextFactory=getSystemProperty("derbyTesting.ldapContextFactory");
            if (ldapContextFactory == null || ldapContextFactory.length() < 1)
                env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            else
                env.put(Context.INITIAL_CONTEXT_FACTORY, ldapContextFactory);
            env.put(Context.PROVIDER_URL, "ldap://" + ldapServer + ":" + ldapPort);
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            return new InitialDirContext(env);
        } catch (NamingException ne) {
            fail("naming exception ");
            return null;
        }
    }
    
    public void testCompareXADataSourcewithJNDIDataSource()
    throws Exception
    {
            XADataSource xads = J2EEDataSource.getXADataSource();
            String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
            JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
            JDBCDataSource.setBeanProperty(xads, "createDatabase", "create");
            JDBCDataSource.setBeanProperty(xads, "description", "XA DataSource");
            
            ic.rebind("cn=compareDS, o=" + dnString, xads);
            javax.sql.XADataSource ads =
                (javax.sql.XADataSource)ic.lookup("cn=compareDS, o=" + dnString);

            // Embedded data sources implement equals(), so use it to check
            // that the two data sources are equal.
            if (usingEmbedded())
            {
                assertEquals(xads, ads);
            }

            // Client data sources don't implement equals(), so compare each
            // property manually. And by the way, we don't trust that equals()
            // in embedded data sources checks all the properties, so do a
            // full check for embedded as well.
//IC see: https://issues.apache.org/jira/browse/DERBY-3669
            String[] orgprops = getPropertyBeanList(xads);
            String[] bindprops = getPropertyBeanList(ads);
            assertEquals(orgprops.length, bindprops.length);

            // Check that all properties are equal.
            for (int i=0;i<orgprops.length;i++){
                assertEquals(orgprops[i], bindprops[i]);
                assertEquals(
                        JDBCDataSource.getBeanProperty(xads, orgprops[i]),
                        JDBCDataSource.getBeanProperty(ads, bindprops[i]));
            }

            // modify something essential of the original XADataSource
            JDBCDataSource.clearStringBeanProperty(xads, "createDatabase");
            
            // Now the ads is no longer the same
            assertFalse(xads.equals(ads));
    }

    /**
     * Obtains a list of bean properties through reflection.
     * 
     *
     * @param ds the data source to investigate
     * @return A list of bean property names.
     */
    private static String[] getPropertyBeanList(Object ds) {
        Method[] allMethods = ds.getClass().getMethods();
        ArrayList<String> properties = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

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

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return properties.toArray(new String[properties.size()]);
    }
}
