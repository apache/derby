
/*
 
   Derby - Class ClobTest
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import junit.framework.*;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

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
    
    // Initialize with the details of the method that are exempted from 
    //throwing a SQLException when they are called after calling free()
    //on a LOB.
    
    private static final ExemptClobMD [] emd = new ExemptClobMD [] {
        new ExemptClobMD( "getCharacterStream", new Class[] { long.class, long.class } ,true,true),
	new ExemptClobMD( "setString",          new Class[] { long.class, String.class } ,false,true),
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
        // Life span of Clob objects are limited by the transaction.  Need
        // autocommit off so Clob objects survive closing of result set.
        getConnection().setAutoCommit(false);

        clob = BlobClobTestSetup.getSampleClob(getConnection());
        
        //call the buildHashSetMethod to initialize the 
        //HashSet with the method signatures that are exempted 
        //from throwing a SQLException after free has been called
        //on the Clob object.
        buildHashSet();
    }

    protected void tearDown() throws Exception {
        clob.free();
        clob = null;
        excludedMethodSet = null;
        super.tearDown();
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
        throws IllegalAccessException, InvocationTargetException, SQLException {
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
    void buildMethodList(Object LOB)
            throws IllegalAccessException, InvocationTargetException {
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
     * Checks if the method is to be exempted from testing or not.
     *
     * @param m the method to check for exemption
     * @return <code>false</code> if the method shall be tested,
     *      <code>true</code> if the method is exempted and shall not be tested.
     */
    boolean checkIfExempted(Method m) {
        ExemptClobMD md = excludedMethodSet.get(m);
        boolean isExempted = false;
        if (md != null) {
            if (usingDerbyNetClient()) {
                isExempted = md.getIfClientFramework();
            } else if (usingEmbedded()) {
                isExempted = md.getIfEmbeddedFramework();
            } else {
                fail("Unknown test environment/framework");
            }
        }
        return isExempted;
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
    boolean checkIfMethodThrowsSQLException(Object LOB,Method method)
            throws IllegalAccessException, InvocationTargetException {
        try {
            method.invoke(LOB,getNullValues(method.getParameterTypes()));
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            if (cause instanceof SQLException ) {
                return ((SQLException)cause).getSQLState().equals("XJ215");
            }
            throw ite;
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
    
    /**
     * Tests the implementation of getCharacterStream(long pos, long length).
     * 
     * @throws Exception
     */
    public void testGetCharacterStreamLong()
    throws Exception {
        String str1 = "This is a test String. This is a test String";

        Reader r1 = new java.io.StringReader(str1);

        PreparedStatement ps = prepareStatement(
            "insert into BLOBCLOB(ID, CLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1,id);
        ps.setCharacterStream(2,r1);
        ps.execute();
        ps.close();

        Statement st = createStatement();

        ResultSet rs = st.executeQuery("select CLOBDATA from " +
            "BLOBCLOB where ID="+id);
        rs.next();
        Clob clob = rs.getClob(1);

        Reader r_1 = clob.getCharacterStream(2L,5L);
        String str2 = str1.substring(1,6);
        Reader r_2 = new java.io.StringReader(str2);

        assertEquals(r_2,r_1);

        rs.close();
        st.close();
    }

    /**
     * Tests the exceptions thrown by the getCharacterStream
     * (long pos, long length) for the following conditions
     * a) pos <= 0
     * b) pos > (length of LOB)
     * c) length < 0
     * d) pos + length > (length of LOB).
     *
     * @throws SQLException.
     */
    public void testGetCharacterStreamLongExceptionConditions()
    throws SQLException {
        String str1 = "This is a test String. This is a test String";

        Reader r1 = new java.io.StringReader(str1);

        PreparedStatement ps = prepareStatement(
            "insert into BLOBCLOB(ID, CLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1,id);
        ps.setCharacterStream(2,r1);
        ps.execute();
        ps.close();

        Statement st = createStatement();

        ResultSet rs = st.executeQuery("select CLOBDATA from " +
            "BLOBCLOB where ID="+id);
        rs.next();
        Clob clob = rs.getClob(1);
        // check the case where pos <= 0
        try {
            // set pos as negative
            clob.getCharacterStream(-2L,5L);
            //Should not come here. The exception has to be thrown.
            fail("FAIL: Expected SQLException for pos being negative " +
                    "not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when pos <= 0 is XJ070
            assertSQLState("XJ070", sqle);
        }

        // check for the case pos > length of clob
        try {
            // set the pos to any value greater than the Clob length
            clob.getCharacterStream(clob.length()+1, 5L);
            //Should not come here. The exception has to be thrown.
            fail("FAIL: Expected SQLException for position being greater than " +
                    "length of LOB not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when pos > length of Clob
            // is XJ076
            assertSQLState("XJ087", sqle);
        }

        //check for the case when length < 0
        try {
            // set length as negative
            clob.getCharacterStream(2L, -5L);
            // Should not come here. The exception has to be thrown.
            fail("Fail: expected exception for the length being negative " +
                    "not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when length < 0 of Clob
            // is XJ071
            assertSQLState("XJ071", sqle);
        }

        //check for the case when pos + length > length of Clob
        try {
            // set pos + length > length of Clob
            clob.getCharacterStream((clob.length() - 4), 10L);
            // Should not come here. The exception has to be thrown.
            fail("Fail: expected exception for the sum of position and length" +
                    " being greater than the LOB size not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when length < 0 of Clob
            // is XJ087
            assertSQLState("XJ087", sqle);
        }
    }
    
    /**
     * Tests that the InputStream got from
     * a empty Clob reflects new data in the
     * underlying Clob.
     *
     * @throws Exception
     */
     public void testGetAsciiStreamCreateClob() throws Exception {
         //The String that will be used
         //to do the inserts into the
         //Clob.
         String str = "Hi I am the insert String";
         
         //Create the InputStream that will
         //be used for comparing the Stream
         //that is obtained from the Blob after
         //the update.
         ByteArrayInputStream str_is = new ByteArrayInputStream
                 (str.getBytes());
         
         //create the empty Clob.
         Clob clob = getConnection().createClob();
         
         //Get the InputStream from this
         //Clob
         InputStream is = clob.getAsciiStream();
         
         //set the String into the clob.
         clob.setString(1, str);
         
         //Ensure that the Stream obtained from
         //the clob contains the expected bytes
         assertEquals(str_is, is);
     }
     
     /**
     * Tests that the Reader got from
     * a empty Clob reflects new data in the
     * underlying Clob.
     *
     * @throws Exception
     */
     public void testGetCharacterStreamCreateClob() throws Exception {
         //The String that will be used
         //to do the inserts into the
         //Clob.
         String str = "Hi I am the insert String";

         //The string reader corresponding to this
         //string that will be used in the comparison.
         StringReader r_string = new StringReader(str);
         
         //create the empty Clob.
         Clob clob = getConnection().createClob();
         
         //Get the Reader from this
         //Clob
         Reader r_clob = clob.getCharacterStream();
         
         //set the String into the clob.
         clob.setString(1, str);
         
         //Now compare the reader corresponding
         //to the string and the reader obtained
         //form the clob to see if they match.
         assertEquals(r_string, r_clob);
     }
     
    /**
     * Tests that the data updated in a Clob
     * is always reflected in the InputStream
     * got. Here the updates into the Clob are
     * done using both an OutputStream obtained
     * from this Clob as well as using Clob.setString.
     *
     * @throws Exception
     */
     public void testGetAsciiStreamClobUpdates() throws Exception {
         //The String that will be used
         //to do the inserts into the
         //Clob.
         String str1 = "Hi I am the insert string";
         
         //Stores the byte array representation of 
         //the insert string.
         byte[] str1_bytes = str1.getBytes();
         
         //The String that will be used in the
         //second series of updates
         String str2 = "Hi I am the update string";
         
         //create the empty Clob.
         Clob clob = getConnection().createClob();
         
         //Get the InputStream from this
         //Clob before any writes happen.
         InputStream is_BeforeWrite = clob.getAsciiStream();
         
         //Get an OutputStream from this Clob
         //into which the data can be written
         OutputStream os = clob.setAsciiStream(1);
         os.write(str1_bytes);
         
         //Doing a setString now on the Clob
         //should reflect the same extension
         //in the InputStream also.
         clob.setString((str1_bytes.length)+1, str2);
         
         //Get the input stream from the
         //Clob after the update
         InputStream is_AfterWrite = clob.getAsciiStream();
         
         //Now check if the two InputStreams
         //match
         assertEquals(is_BeforeWrite, is_AfterWrite);
     }
     
    /**
     * Tests that the data updated in a Clob
     * is always reflected in the Reader
     * got. Here the updates are done using
     * both a Writer obtained from this Clob
     * and using Clob.setString.
     *
     * @throws Exception
     */
     public void testGetCharacterStreamClobUpdates() throws Exception {
         //The String that will be used
         //to do the inserts into the
         //Clob.
         String str1 = "Hi I am the insert string";
         
         //The String that will be used in the
         //second series of updates
         String str2 = "Hi I am the update string";
         
         //create the empty Clob.
         Clob clob = getConnection().createClob();
         
         //Get the Reader from this
         //Clob
         Reader r_BeforeWrite = clob.getCharacterStream();
         
         //Get a writer from this Clob
         //into which the data can be written
         Writer w = clob.setCharacterStream(1);
         char [] chars_str1 = new char[str1.length()];
         str2.getChars(0, str1.length(), chars_str1, 0);
         w.write(chars_str1);
         
         //Doing a setString now on the Clob
         //should reflect the same extension
         //in the InputStream also.
         clob.setString((str1.getBytes().length)+1, str2);
         
         //Now get the reader from the Clob after
         //the update has been done.
         Reader r_AfterWrite = clob.getCharacterStream();
         
         //Now compare the two readers to see that they
         //contain the same data.
         assertEquals(r_BeforeWrite, r_AfterWrite);
     }


    /**
     * Create test suite for this test.
     */
    public static Test suite() {
        return  new BlobClobTestSetup(
                TestConfiguration.defaultSuite(
                ClobTest.class,
                false));
    }

} // End class ClobTest
