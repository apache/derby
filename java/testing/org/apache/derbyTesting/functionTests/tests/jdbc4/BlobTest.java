/*
 
   Derby - Class BlobTest
 
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
 * of java.sql.Blob.
 *
 * It store the following information about the methods
 *
 * a) Name
 * b) Method Parameters
 * c) Whether the method is exempted in the Embedded Sever
 * d) Whether the method is exempted in the NetworkClient
 *
 */
class ExemptBlobMD {
    // The Name of the method
    private String methodName_;
    
    // The parameters of the method
    private Class [] params_;
    
    //Whether it is exempted in the 
    //Client or the Embedded framework
    private boolean isClientFramework_;
    private boolean isEmbeddedFramework_;
    
    /**
     * The Constructor for the ExemptBlobMD class that
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
    public ExemptBlobMD(String methodName,Class [] params,
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
 * Tests of the JDBC 4.0 specific <code>Blob</code> methods.
 */
public class BlobTest
    extends BaseJDBCTestCase {

    /** Default Blob object used by the tests. */
    private Blob blob = null;
    
    // Initialize with the details of the method that are exempted from 
    //throwing a SQLException when they are called after calling free()
    //on a LOB.
    
    private static final ExemptBlobMD [] emd = new ExemptBlobMD [] {
        new ExemptBlobMD( "getBinaryStream", new Class[] { long.class,long.class }
                                                                   ,true,true ),
        new ExemptBlobMD( "setBinaryStream", new Class[] { long.class },false,true ),
        new ExemptBlobMD( "setBytes", new Class[] { long.class, byte[].class }
                                                                   ,false,true ),
        new ExemptBlobMD( "setBytes", new Class[] { long.class, byte[].class
                                           , int.class, int.class },false,true ),
        new ExemptBlobMD( "truncate", new Class[] { long.class },false,true),
        new ExemptBlobMD( "free",null,true,true)
    };
    
    // An HashMap that is indexed by the Method which facilitated easy
    //search for whether the given method has been exempted from the
    //LOB interface.
    
    private HashMap<Method,ExemptBlobMD> excludedMethodSet = 
                            new HashMap<Method,ExemptBlobMD>();
       
    /**
     * Create the test with the given name.
     *
     * @param name name of the test.
     */
    public BlobTest(String name) {
        super(name);
    }
    
    public void setUp() 
        throws SQLException {

        // Life span of Blob objects are limited by the transaction.  Need
        // autocommit off so Blob objects survive closing of result set.
        getConnection().setAutoCommit(false);

        blob = BlobClobTestSetup.getSampleBlob(getConnection());
        
        //call the buildHashSetMethod to initialize the 
        //HashSet with the method signatures that are exempted 
        //from throwing a SQLException after free has been called
        //on the Clob object.
        buildHashSet();
    }

    protected void tearDown() throws Exception {
        blob.free();
        blob = null;
        excludedMethodSet = null;
        super.tearDown();
    }
    
    /**
     * Builds the HashSet which will be used to test whether the given methods
     * can be exempted or not
     */
    void buildHashSet() {
        Class iface = Blob.class;
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
     * Blob interface.
     * 
     * @throws SQLException if an error occurs during releasing
     *         the Blob resources
     *
     */
    public void testFreeandMethodsAfterCallingFree()
        throws SQLException {
        blob.free();
        //testing the idempotence of the free() method
        //the method can be called multiple times on
        //the same instance. subsequent calls after 
        //the first are treated as no-ops
        blob.free();

        //blob becomes invalid after the first call 
        //to the free method so testing calling
        //a method on this invalid object should throw
        //an SQLException
        buildMethodList(blob);
    }
    
    /*
     * 
     * Enumerate the methods of the Blob interface and 
     * get the list of methods present in the interface
     * @param LOB an instance of the Blob interface implementation
     */
    void buildMethodList(Object LOB) {
        //If the given method throws the correct exception
        //set this to true and add it to the 
        boolean valid = true;
        
        //create a list of the methods that fail the test
        Vector<Method> methodList = new Vector<Method>();
        
        //The class whose methods are to be verified
        Class clazz = Blob.class;
        
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
        ExemptBlobMD md = excludedMethodSet.get(m);
        
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
    
    /**
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

    /**
     * Tests the implementation of the method
     * getBinaryStream(long pos, long length).
     *
     * @throws Exception
     */
    public void testGetBinaryStreamLong()
    throws Exception {
        byte[] BYTES1 = {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        PreparedStatement ps = prepareStatement(
            "insert into BLOBCLOB(ID, BLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1,id);
        ps.setBinaryStream(2,is);
        ps.execute();
        ps.close();

        Statement st = createStatement();

        ResultSet rs = st.executeQuery("select BLOBDATA from " +
            "BLOBCLOB where ID="+id);
        rs.next();
        Blob blob = rs.getBlob(1);

        InputStream is_1 = blob.getBinaryStream(2L,5L);
        InputStream is_2 = new java.io.ByteArrayInputStream(BYTES1,1,5);

        assertEquals(is_2,is_1);

        rs.close();
        st.close();
    }
    
    /**
     * Tests the exceptions thrown by the getBinaryStream
     * (long pos, long length) for the following conditions
     * a) pos <= 0
     * b) pos > (length of LOB)
     * c) length < 0
     * d) pos + length > (length of LOB).
     *
     * @throws SQLException.
     */
    public void testGetBinaryStreamLongExceptionConditions()
    throws SQLException {
        byte[] BYTES1 = {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        PreparedStatement ps = prepareStatement(
            "insert into BLOBCLOB(ID, BLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1,id);
        ps.setBinaryStream(2,is);
        ps.execute();
        ps.close();

        Statement st = createStatement();

        ResultSet rs = st.executeQuery("select BLOBDATA from " +
            "BLOBCLOB where ID="+id);
        rs.next();
        Blob blob = rs.getBlob(1);
        // check the case where pos <= 0
        try {
            // set pos as negative
            blob.getBinaryStream(-2L,5L);
            //Should not come here. The exception has to be thrown.
            fail("FAIL: Expected SQLException for pos being negative " +
                    "not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when pos <= 0 is XJ070
            assertSQLState("XJ070", sqle);
        }

        // check for the case pos > length of Blob
        try {
            // set the pos to any value greater than the Blob length
            blob.getBinaryStream(blob.length()+1, 5L);
            //Should not come here. The exception has to be thrown.
            fail("FAIL: Expected SQLException for position being greater than " +
                    "length of LOB not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when pos > length of Blob
            // is XJ076
            assertSQLState("XJ087", sqle);
        }

        //check for the case when length < 0
        try {
            // set length as negative
            blob.getBinaryStream(2L, -5L);
            // Should not come here. The exception has to be thrown.
            fail("Fail: expected exception for the length being negative " +
                    "not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when length < 0 of Blob
            // is XJ071
            assertSQLState("XJ071", sqle);
        }

        //check for the case when pos + length > length of Blob
        try {
            // set pos + length > length of Blob
            blob.getBinaryStream((blob.length() - 4), 10L);
            // Should not come here. The exception has to be thrown.
            fail("Fail: expected exception for the sum of position and length" +
                    " being greater than the LOB size not thrown");
        }
        catch(SQLException sqle) {
            // The SQLState for the exception thrown when length < 0 of Blob
            // is XJ087
            assertSQLState("XJ087", sqle);
        }
    }

    
    /**
     * Tests that the InputStream got from
     * a empty Blob reflects new data in the
     * underlying Blob.
     *
     * @throws Exception
     */
     public void testGetBinaryStreamCreateBlob() throws Exception {
         //The bytes that will be used
         //to do the inserts into the
         //Blob.
         byte[] bytes1 = {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
         };
         
         //The InputStream corresponding to the
         //Byte array
         ByteArrayInputStream is_bytes1 = new ByteArrayInputStream(bytes1);
         
         //create the empty Blob.
         Blob blob = getConnection().createBlob();
         
         //Get the InputStream from this
         //Blob
         InputStream is = blob.getBinaryStream();
         
         //set the bytes into the blob.
         blob.setBytes(1, bytes1);
         
         //Now compare the ByteArrayInputStream
         //and the stream from the Blob to
         //ensure that they are equal
         assertEquals(is_bytes1, is);
     }
     
    /**
     * Tests that the data updated in a Blob
     * is always reflected in the InputStream
     * got. Here we do updates in the Blob
     * both using Blob.setBytes and
     * using the OutputStream obtained from
     * the Blob.
     *
     * @throws Exception
     */
     public void testGetBinaryStreamBlobUpdates() throws Exception {
         //The bytes that will be used
         //to do the inserts into the
         //Blob using Blob.setBytes.
         byte[] bytes1 = {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
         };
         
         //The Byte array that will be used to do the
         //updates into the Blob using the OutputStream
         //obtained from the Blob
         byte[] bytes2 = {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x65, 0x66, 0x67, 0x68, 0x69
         };
         
         //create the empty Blob.
         Blob blob = getConnection().createBlob();
         
         //Get the InputStream from this
         //Blob
         InputStream is_BeforeWrite = blob.getBinaryStream();
         
         //Get an OutputStream from this Blob
         //into which the data can be written
         OutputStream os = blob.setBinaryStream(1);
         os.write(bytes1);
         
         //Doing a setBytes now on the Blob
         //should reflect the same extension
         //in the InputStream also.
         blob.setBytes(bytes1.length+1, bytes2);
         
         //Get the InputStream from this Blob
         //after the update has happened.
         InputStream is_AfterWrite = blob.getBinaryStream();
         
         //Compare the two streams to check that they
         //match
         assertEquals(is_BeforeWrite, is_AfterWrite);
     }
    

    /**
     * Create test suite for this test.
     */
    public static Test suite() {
        return  new BlobClobTestSetup(
                TestConfiguration.defaultSuite(
                BlobTest.class,
                false));
    }

} // End class BlobTest
