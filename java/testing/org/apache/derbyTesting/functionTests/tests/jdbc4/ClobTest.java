
/*
 
   Derby - Class ClobTest
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

import java.sql.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;

/* This class is used to store the details of the methods that
 * throw a SQLFeatureNotSupportedException in the implementation
 * of java.sql.Clob.
 *
 * It store the following information about the methods
 *
 * a) Name
 * b) Method Parameters
 * c) Whether the method is exempted in the Embedded Sever
 * d) Whether the method is exempted in the NetworkClient
 *
 */
class ExemptClobMD {
    // The Name of the method
    private String methodName_;
    
    // The parameters of the method
    private Class [] params_;
    
    //Whether it is exempted in the 
    //Client or the Embedded framework
    private boolean isClientFramework_;
    private boolean isEmbeddedFramework_;
    
    /**
     * The Constructor for the ExemptClobMD class that
     * initialized the object with the details of the 
     * methods that have been exempted
     *
     * @param methodName          A String that contains the name of the method
     *                            that has been exempted.
     * @param params              A array of Class that contains the parameters 
     *                            of the methods.
     * @param isClientFramework   true if the method is exempted in the 
     *                            Client framework.
     * @param isEmbeddedFramework true if the method is exempted in the 
     *                            Embedded framework.
     *
     */
    public ExemptClobMD(String methodName,Class [] params,
                            boolean isClientFramework,
                            boolean isEmbeddedFramework) {
        methodName_ = methodName;
        params_ = params;
        isClientFramework_ = isClientFramework;
        isEmbeddedFramework_ = isEmbeddedFramework;
    }
    
    /**
     *
     * Returns the name of the method.
     *
     * @return A String containing the name of the method.
     *
     */
    public String getMethodName() { return methodName_; }
    
    /**
     * Returns a array of Class containing the type of the parameters
     * of this method. 
     *
     * @return A array of Class containing the type of the parameters 
     *         of the method.
     */
    public Class [] getParams() { return params_; }
    
    /**
     * Returns if the method is exempted from the Client Framework.
     *
     * @return true if the method is exempted from the Client Framework.
     */
    public boolean getIfClientFramework() { return isClientFramework_; }
    
    /**
     * Returns if the method is exempted from the Embedded Framework.
     *
     * @return true if the method is exempted from the Embedded Framework.
     */
    public boolean getIfEmbeddedFramework() { return isEmbeddedFramework_; }
}

/*
 * Tests of the JDBC 4.0 specific <code>Clob</code> methods.
 */
public class ClobTest
    extends BaseJDBCTestCase {

    /** Default Clob object used by the tests. */
    private Clob clob = null;
    /** Default connection used by the tests. */
    private Connection con = null;
    
    // Initialize with the details of the method that are exempted from 
    //throwing a SQLException when they are called after calling free()
    //on a LOB.
    
    private ExemptClobMD [] emd = new ExemptClobMD [] {
        new ExemptClobMD( "getCharacterStream", new Class[] { long.class, long.class } ,true,true),
        new ExemptClobMD( "setAsciiStream",     new Class[] { long.class } ,false,true),
	new ExemptClobMD( "setCharacterStream", new Class[] { long.class } ,true,true),
	new ExemptClobMD( "setString",          new Class[] { long.class, String.class } ,false,true),
	new ExemptClobMD( "setString",          new Class[] { long.class, String.class, int.class, int.class},false,true),
	new ExemptClobMD( "truncate",           new Class[] { long.class },false,true),
        new ExemptClobMD( "free",               null,true,true)
    };
    
    // An HashMap that is indexed by the Method which facilitated easy
    //search for whether the given method has been exempted from the
    //LOB interface.
    
    private HashMap<Method,ExemptClobMD> excludedMethodSet = 
                            new HashMap<Method,ExemptClobMD>();
    
    /**
     * Create the test with the given name.
     *
     * @param name name of the test.
     */
    public ClobTest(String name) {
        super(name);
    }
    
    public void setUp() 
        throws SQLException {
        con = getConnection();
        clob = BlobClobTestSetup.getSampleClob(con);
        
        //call the buildHashSetMethod to initialize the 
        //HashSet with the method signatures that are exempted 
        //from throwing a SQLException after free has been called
        //on the Clob object.
        buildHashSet();
    }

    public void tearDown()
        throws SQLException {
        clob = null;
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
        con = null;
    }
    
    /**
     * Builds the HashSet which will be used to test whether the given methods
     * can be exempted or not
     */
    void buildHashSet() {
        Class iface = Clob.class;
        for(int i=0;i<emd.length;i++) {
            try {
                Method m = iface.getMethod(emd[i].getMethodName()
                                                ,emd[i].getParams());
                excludedMethodSet.put(m,emd[i]);
            }
            catch(NoSuchMethodException nsme) {
                fail("The method could not be found in the interface");
            }
        }
    }
    
    /**
     * Tests the implementation for the free() method in the
     * Clob interface.
     * 
     * @throws SQLException if an error occurs during releasing
     *         the Clob resources
     *
     */
    public void testFreeandMethodsAfterCallingFree()
        throws SQLException {
            InputStream asciiStream = clob.getAsciiStream();
            Reader charStream  = clob.getCharacterStream();
            clob.free();
            //testing the idempotence of the free() method
            //the method can be called multiple times on
            //the same instance. subsequent calls after 
            //the first are treated as no-ops
            clob.free();
            
            //clob becomes invalid after the first call 
            //to the free method so testing calling
            //a method on this invalid object should throw
            //an SQLException
            buildMethodList(clob);
    }
    
    /*
     * 
     * Enumerate the methods of the Clob interface and 
     * get the list of methods present in the interface
     * @param LOB an instance of the Clob interface implementation
     */
    void buildMethodList(Object LOB) {
        //If the given method throws the correct exception
        //set this to true and add it to the 
        boolean valid = true;
        
        //create a list of the methods that fail the test
        Vector<Method> methodList = new Vector<Method>();
        
        //The class whose methods are to be verified
        Class clazz = Clob.class;
        
        //The list of the methods in the class that need to be invoked
        //and verified
        Method [] methods = clazz.getMethods();
        
        //Check each of the methods to ensure that
        //they throw the required exception
        for(int i=0;i<methods.length;i++) {
            if(!checkIfExempted(methods[i])) {
                valid = checkIfMethodThrowsSQLException(LOB,methods[i]);
                
                //add the method to the list if the method does
                //not throw the required exception
                if(valid == false) methodList.add(methods[i]);
                
                //reset valid
                valid = true;
            }
        }
        
        if(!methodList.isEmpty()) {
            int c=0;
            String failureMessage = "The Following methods don't throw " +
                "required exception - ";
            for (Method m : methodList) {
                c = c + 1;
                if(c == methodList.size() && c != 1) 
                    failureMessage += " & ";
                else if(c != 1)
                    failureMessage += " , ";
                failureMessage += m.getName();
            }
            fail(failureMessage);
        }
    }
    
    /**
     *Checks if the method throws a SQLFeatureNotSupportedException
     *@param m The method object that needs to be verified to see if it 
     *         is exempted
     *@return true if the given method does not throw the required SQLException
     *
     */
    boolean checkIfExempted(Method m) {
        ExemptClobMD md = excludedMethodSet.get(m);
        
        if(md != null && usingDerbyNetClient()) { 
            if(md.getIfClientFramework()) 
                return true;
            else
                return false;
        } 
        if(md != null && usingEmbedded()) {
            if(md.getIfEmbeddedFramework())
                return true;
            else
                return false;
        }
        return false;
    }
    
    /*
     * Checks if the invocation of the method throws a SQLExceptio
     * as expected.
     * @param LOB    the Object that implements the Blob interface
     * @param method the method that needs to be tested to ensure
     *               that it throws the correct exception
     * @return true  If the method throws the SQLException required
     *               after the free method has been called on the
     *               LOB object
     *
     */
    boolean checkIfMethodThrowsSQLException(Object LOB,Method method) {
        try {
            method.invoke(LOB,getNullValues(method.getParameterTypes()));
        } catch(Throwable e) {
            if(e instanceof InvocationTargetException) {
                Throwable cause = e.getCause();
                if (cause instanceof SQLException ) {
                    SQLException sqle = (SQLException)cause;
                    if(sqle.getSQLState().equals("XJ215"))
                        return true;
                    else
                        return false;
                } else {
                    return false;
                }
                
            }
        }
        return false;
    }
    
    /*
     * Return a array of objects containing the default values for
     * the objects passed in as parameters
     * 
     * @param parameterTypes an array containing the types of the parameter 
     *                       to the method
     * @return an array of Objects containing the null values for the 
     *         parameter inputs
     */
    
    Object[] getNullValues(Class<?> [] params) {
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            args[i] = getNullValueForType(params[i]);
        }
        return args;
    }
    
    /*
     * Returns the null value for the specific type
     * 
     * @param type the type of the parameter for which the null
     *             value is required
     * @return the null value for the specific type
     * 
     */
     Object getNullValueForType(Class type)
	{
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == Boolean.TYPE) {
            return Boolean.FALSE;
        }
        if (type == Character.TYPE) {
            return new Character((char) 0);
        }
        if (type == Byte.TYPE) {
            return new Byte((byte) 0);
        }
        if (type == Short.TYPE) {
            return new Short((short) 0);
        }
        if (type == Integer.TYPE) {
            return new Integer(0);
        }
        if (type == Long.TYPE) {
            return new Long(0L);
        }
        if (type == Float.TYPE) {
            return new Float(0f);
        }
        if (type == Double.TYPE) {
            return new Double(0d);
        }
        fail("Don't know how to handle type " + type);
        return null;            // unreachable statement
    }
    
    public void testGetCharacterStreamLongNotImplemented()
        throws SQLException {
        try {
            clob.getCharacterStream(5l, 10l);
            fail("Clob.getCharacterStream(long,long)" +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    /**
     * Create test suite for this test.
     */
    public static Test suite() {
        return new BlobClobTestSetup(new TestSuite(ClobTest.class,
                                                   "ClobTest suite"));
    }

} // End class ClobTest
