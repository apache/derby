/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ClobTest

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
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
//IC see: https://issues.apache.org/jira/browse/DERBY-1328
class ExemptClobMD {
    /** The Name of the method. */
    private String methodName_;

    /** The parameters of the method. */
    private Class [] params_;

    /** Tells if exempted in the client framework. */
    private boolean isClientFramework_;
    /** Tells if exempted in the embedded framework. */
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
        new ExemptClobMD( "getCharacterStream",
                new Class[] { long.class, long.class } ,true,true),
        new ExemptClobMD( "setString",
                new Class[] { long.class, String.class } ,false,true),
        new ExemptClobMD( "truncate",
                new Class[] { long.class },false,true),
        new ExemptClobMD( "free",
                null,true,true)
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2702
        getConnection().setAutoCommit(false);
    }

    protected void tearDown() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-3098
        if (clob != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2707
            clob.free();
            clob = null;
        }
        excludedMethodSet = null;
        super.tearDown();
    }

    /**
     * Builds the HashSet which will be used to test whether the given methods
     * can be exempted or not
     */
    void buildHashSet() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4877
        Class<Clob> iface = Clob.class;
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
     * Tests free() after implicit free
     *
     * @throws SQLException if an error occurs during free
     *        
     */
    public void testFreeAfterImplicitFree()  throws SQLException
    {
        Connection conn = getConnection();
        clob = BlobClobTestSetup.getSampleClob(conn);
        conn.commit();
        // DERBY-5605
        // free should not throw an exception even though it was 
        // implicitly freed with the commit.
        clob.free();
        
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3098
          throws IllegalAccessException, InvocationTargetException, SQLException
    {
        clob = BlobClobTestSetup.getSampleClob(getConnection());
//IC see: https://issues.apache.org/jira/browse/DERBY-1555

        //call the buildHashSetMethod to initialize the
        //HashSet with the method signatures that are exempted
        //from throwing a SQLException after free has been called
        //on the Clob object.
        buildHashSet();
//IC see: https://issues.apache.org/jira/browse/DERBY-1328

        InputStream asciiStream = clob.getAsciiStream();
        Reader charStream = clob.getCharacterStream();
        clob.free();
        //testing the idempotence of the free() method
        //the method can be called multiple times on

        //the first are treated as no-ops
        clob.free();


        //to the free method so testing calling
        //a method on this invalid object should throw
        //an SQLException
        buildMethodList(clob);
    }

    /**
     * Enumerate the methods of the Clob interface and
     * get the list of methods present in the interface
     * @param LOB an instance of the Clob interface implementation
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2646
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2646
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

    /**
     * Checks if the invocation of the method throws a SQLExceptio
     * as expected.
     * @param LOB    the Object that implements the Blob interface
     * @param method the method that needs to be tested to ensure
     *               that it throws the correct exception
     * @return true  If the method throws the SQLException required
     *               after the free method has been called on the
     *               LOB object
     */
    boolean checkIfMethodThrowsSQLException(Object LOB,Method method)
//IC see: https://issues.apache.org/jira/browse/DERBY-2646
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

    /**
     * Return a array of objects containing the default values for
     * the objects passed in as parameters
     *
     * @param params an array containing the types of the parames to the method
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

    /**
     * Returns the null value for the specific type
     *
     * @param type the type of the parameter for which the null
     *             value is required
     * @return the null value for the specific type
     */
     Object getNullValueForType(Class type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == Boolean.TYPE) {
            return Boolean.FALSE;
        }
        if (type == Character.TYPE) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            return (char) 0;
        }
        if (type == Byte.TYPE) {
            return (byte) 0;
        }
        if (type == Short.TYPE) {
            return (short) 0;
        }
        if (type == Integer.TYPE) {
            return 0;
        }
        if (type == Long.TYPE) {
            return 0L;
        }
        if (type == Float.TYPE) {
            return 0f;
        }
        if (type == Double.TYPE) {
            return 0d;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2444
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2702

        rs.close();
        st.close();
    }

    /**
     * Obtains streams from the Clob reading portions of the content, always
     * including the last character in the Clob.
     * <p>
     * This case fills the Clob with latin lowercase characters.
     */
    public void testGetCharacterStreamLongLastCharLatin()
//IC see: https://issues.apache.org/jira/browse/DERBY-4060
            throws IOException, SQLException {
        CharAlphabet alphabet = CharAlphabet.modernLatinLowercase();
        // Insert a Clob
        int length = 5000;
        PreparedStatement ps = prepareStatement(
            "insert into BLOBCLOB(ID, CLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1, id);
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(length, alphabet), length);
        ps.execute();
        ps.close();
        // Perform the actual test.
        getCharacterStreamLongLastChar(id, length, alphabet);
    }

    /**
     * Obtains streams from the Clob reading portions of the content, always
     * including the last character in the Clob.
     * <p>
     * This case fills the Clob with Chinese/Japanese/Korean characters.
     */
    public void testGetCharacterStreamLongLastCharCJK()
            throws IOException, SQLException {
        CharAlphabet alphabet = CharAlphabet.cjkSubset();
        // Insert a Clob
        int length = 9001;
        PreparedStatement ps = prepareStatement(
            "insert into BLOBCLOB(ID, CLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1, id);
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(length, alphabet), length);
        ps.execute();
        ps.close();
        // Perform the actual test.
        getCharacterStreamLongLastChar(id, length, alphabet);
    }

    /**
     * Obtains streams from the Clob and makes sure we can always read the
     * last char in the Clob.
     * <p>
     * See DERBY-4060.
     *
     * @param id id of the Clob to use
     * @param length the length of the Clob
     * @param alphabet the alphabet used to create the content
     * @throws IOException if reading from a stream fails
     * @throws SQLException if something goes wrong
     */
    private void getCharacterStreamLongLastChar(int id, int length,
                                                CharAlphabet alphabet)
            throws IOException, SQLException {
        // Get last char from the source stream.
        Reader cmpReader = new LoopingAlphabetReader(length, alphabet);
        cmpReader.skip(length -1);
        char srcLastChar = (char)cmpReader.read();
        assertTrue(cmpReader.read() == -1);

        PreparedStatement ps = prepareStatement(
                "select CLOBDATA from BLOBCLOB where ID=?");
        ps.setInt(1, id);
        // Read everything first.
        int charsToRead = length;
        ResultSet rs = ps.executeQuery();
        rs.next();
        Reader reader = rs.getClob(1).getCharacterStream(
                                        length - charsToRead +1, charsToRead);
        // Drain the stream, and make sure we are able to read the last char.
        char lastCharRead = getLastCharInStream(reader, charsToRead);
        assertEquals(srcLastChar, lastCharRead);
        reader.close();
        rs.close();

        // Read a portion of the stream.
        charsToRead = length / 4;
        rs = ps.executeQuery();
        rs.next();
        reader = rs.getClob(1).getCharacterStream(
                                        length - charsToRead +1, charsToRead);
        lastCharRead = getLastCharInStream(reader, charsToRead);
        assertEquals(srcLastChar, lastCharRead);
        reader.close();
        rs.close();

        // Read a very small portion of the stream.
        charsToRead = 1;
        rs = ps.executeQuery();
        rs.next();
        reader = rs.getClob(1).getCharacterStream(
                                        length - charsToRead +1, charsToRead);
        lastCharRead = getLastCharInStream(reader, charsToRead);
        assertEquals(srcLastChar, lastCharRead);
        reader.close();
        rs.close();
    }

    /**
     * Test that <code>Clob.getCharacterStream(long,long)</code> works on CLOBs
     * that are streamed from store. (DERBY-2891)
     */
    public void testGetCharacterStreamLongOnLargeClob() throws Exception {
        getConnection().setAutoCommit(false);

        // create large (>32k) clob that can be read from store
        final int size = 33000;
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i += 10) {
            sb.append("1234567890");
        }

        final int id = BlobClobTestSetup.getID();
        PreparedStatement ps = prepareStatement(
            "insert into blobclob(id, clobdata) values (?,cast(? as clob))");
        ps.setInt(1, id);
        ps.setString(2, sb.toString());
        ps.executeUpdate();
        ps.close();

        Statement s = createStatement();
        ResultSet rs = s.executeQuery(
            "select clobdata from blobclob where id = " + id);
        assertTrue(rs.next());
        Clob c = rs.getClob(1);

        // request a small region of the clob
        BufferedReader r = new BufferedReader(c.getCharacterStream(4L, 3L));
        assertEquals("456", r.readLine());

        r.close();
        c.free();
        rs.close();
        s.close();
        rollback();
    }

    /**
     * Tests the exceptions thrown by the getCharacterStream
     * (long pos, long length) for the following conditions
     * a) pos &lt;= 0
     * b) pos &gt; (length of LOB)
     * c) length &lt; 0
     * d) pos + length &gt; (length of LOB).
     *
     * @throws SQLException
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2444
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2763

         //Create the InputStream that will
         //be used for comparing the Stream
         //that is obtained from the Blob after
         //the update.
         ByteArrayInputStream str_is = new ByteArrayInputStream
                 (str.getBytes("US-ASCII"));
//IC see: https://issues.apache.org/jira/browse/DERBY-3774

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
         clob.setString((str1.length())+1, str2);
//IC see: https://issues.apache.org/jira/browse/DERBY-3780

         //Now get the reader from the Clob after
         //the update has been done.
         Reader r_AfterWrite = clob.getCharacterStream();

         //Now compare the two readers to see that they
         //contain the same data.
         assertEquals(r_BeforeWrite, r_AfterWrite);
     }


    /**
     * Test that a lock held on the corresponding row is released when free() is
     * called on the Clob object.
     * @throws java.sql.SQLException
     */
    public void testLockingAfterFree() throws SQLException
    {
        int id = initializeLongClob();  // Opens clob object
        executeParallelUpdate(id, true); // Test that timeout occurs
//IC see: https://issues.apache.org/jira/browse/DERBY-3098

        // Test that update goes through after the clob is closed
        clob.free();
        executeParallelUpdate(id, false);

        commit();
    }

    /**
     * Test that a lock held on the corresponding row is NOT released when
     * free() is called on the Clob object if the isolation level is
     * Repeatable Read
     * @throws java.sql.SQLException
     */
    public void testLockingAfterFreeWithRR() throws SQLException
    {
        getConnection().
                setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        int id = initializeLongClob(); // Opens clob object
        executeParallelUpdate(id, true); // Test that timeout occurs

        // Test that update still times out after the clob is closed
        clob.free();
        executeParallelUpdate(id, true);

        // Test that the update goes through after the transaction has committed
        commit();
        executeParallelUpdate(id, false);
    }


     /**
     * Test that a lock held on the corresponding row is released when
     * free() is called on the Clob object if the isolation level is
     * Read Uncommitted
     * @throws java.sql.SQLException
     */
    public void testLockingAfterFreeWithDirtyReads() throws SQLException
    {
        getConnection().
                setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        int id = initializeLongClob(); // Opens clob object
        executeParallelUpdate(id, true); // Test that timeout occurs

       // Test that update goes through after the clob is closed
        clob.free();
        executeParallelUpdate(id, false);

        commit();
    }

    /** Inserts, fetches and checks the length of a Clob using a stream. */
    public void testInsertAndFetchZeroLength()
//IC see: https://issues.apache.org/jira/browse/DERBY-3907
            throws IOException, SQLException {
        insertAndFetchTest(0);
    }

    /** Inserts, fetches and checks the length of a Clob using a stream. */
    public void testInsertAndFetchVerySmall()
            throws IOException, SQLException {
        insertAndFetchTest(7);
    }

    /** Inserts, fetches and checks the length of a Clob using a stream. */
    public void testInsertAndFetchSmall()
            throws IOException, SQLException {
        insertAndFetchTest(1587);
    }

    /** Inserts, fetches and checks the length of a Clob using a stream. */
    public void testInsertAndFetchMedium()
            throws IOException, SQLException {
        insertAndFetchTest(32000);
    }

    /** Inserts, fetches and checks the length of a Clob using a stream. */
    public void testInsertAndFetchMediumPlus()
            throws IOException, SQLException {
        insertAndFetchTest(64000);
    }

    /** Inserts, fetches and checks the length of a Clob using a stream. */
    public void testInsertAndFetchLarge()
            throws IOException, SQLException {
        insertAndFetchTest(128022);
    }

    /** Inserts, fetches and checks the length of a Clob using a stream. */
    public void testInsertAndFetchLarger()
            throws IOException, SQLException {
        insertAndFetchTest(3*1024*1024);
    }

    /**
     * Inserts a Clob with the specified length, using a stream source, then
     * fetches it from the database and checks the length.
     *
     * @param length number of characters in the Clob
     * @throws IOException if reading from the source fails
     * @throws SQLException if something goes wrong
     */
    private void insertAndFetchTest(long length)
            throws IOException, SQLException {
        PreparedStatement ps = prepareStatement(
                "insert into BLOBCLOB(ID, CLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1, id);
        ps.setCharacterStream(2, new LoopingAlphabetReader(length), length);
        long tsStart = System.currentTimeMillis();
        ps.execute();
        println("Inserted " + length + " chars (length specified) in " +
                (System.currentTimeMillis() - tsStart) + " ms");
        Statement stmt = createStatement();
        tsStart = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery(
                "select CLOBDATA from BLOBCLOB where id = " + id);
        assertTrue("Clob not inserted", rs.next());
        Clob aClob = rs.getClob(1);
        assertEquals("Invalid length", length, aClob.length());
        println("Fetched length (" + length + ") in " +
                (System.currentTimeMillis() - tsStart) + " ms");
        rs.close();

        // Insert same Clob again, using the lengthless override.
        id = BlobClobTestSetup.getID();
        ps.setInt(1, id);
        ps.setCharacterStream(2, new LoopingAlphabetReader(length));
        tsStart = System.currentTimeMillis();
        ps.executeUpdate();
        println("Inserted " + length + " chars (length unspecified) in " +
                (System.currentTimeMillis() - tsStart) + " ms");
        rs = stmt.executeQuery(
                "select CLOBDATA from BLOBCLOB where id = " + id);
        assertTrue("Clob not inserted", rs.next());
        aClob = rs.getClob(1);
        assertEquals("Invalid length", length, aClob.length());
        println("Fetched length (" + length + ") in " +
                (System.currentTimeMillis() - tsStart) + " ms");
        rs.close();

        rollback();
    }

    /**
     * Insert a row with a large clob into the test table.  Read the row from 
     * the database and assign the clob value to <code>clob</code>.
     * @return The id of the row that was inserted
     * @throws java.sql.SQLException
     */
    private int initializeLongClob() throws SQLException
    {
        // Clob needs to be larger than one page for locking to occur
        final int lobLength = 40000;

        // Insert a long Clob
        PreparedStatement ps = prepareStatement(
                "insert into BLOBCLOB(ID, CLOBDATA) values(?,?)");
        int id = BlobClobTestSetup.getID();
        ps.setInt(1,id);
        ps.setCharacterStream(2, new LoopingAlphabetReader(lobLength), lobLength);
        ps.execute();
        ps.close();
        commit();

        // Fetch the Clob object from the database
        Statement st = createStatement();
        ResultSet rs =
                st.executeQuery("select CLOBDATA from BLOBCLOB where ID=" + id);
        rs.next();
        clob = rs.getClob(1);
        rs.close();
        st.close();

        return id;
    }


    /**
     * Try to update the row with the given error.  Flag a failure if a
     * timeout occurs when not expected, and vice versa.
     * @param id The id of the row to be updated
     * @param timeoutExpected true if it is expected that the update times out
     * @throws java.sql.SQLException
     */
    private void executeParallelUpdate(int id, boolean timeoutExpected)
            throws SQLException
    {
        Connection conn2 = openDefaultConnection();
        Statement stmt2 = conn2.createStatement();

        try {
            stmt2.executeUpdate("update BLOBCLOB set BLOBDATA = " +
                                "cast(X'FFFFFF' as blob) where ID=" + id);
            stmt2.close();
            conn2.commit();
            conn2.close();
            if (timeoutExpected) {
                fail("FAIL - should have gotten lock timeout");
            }
         } catch (SQLException se) {
            stmt2.close();
            conn2.rollback();
            conn2.close();
            if (timeoutExpected) {
                assertSQLState(LOCK_TIMEOUT, se);
            } else {
                throw se;
            }
        }
    }

    /**
     * Drains the stream and returns the last char read from the stream.
     *
     * @param reader stream to drain
     * @param expectedCount expected number of chars (remaining) in the stream
     * @return The last char read.
     * @throws AssertionError if there are too many/few chars in the stream
     * @throws IOException if reading from the stream fails
     */
    public static char getLastCharInStream(Reader reader, int expectedCount)
//IC see: https://issues.apache.org/jira/browse/DERBY-4060
            throws IOException {
        int read = 0;
        final char[] buf = new char[256];
        assertTrue(buf.length > 0); // Do not allow an infinite loop here.
        while (true) {
            int readThisTime = reader.read(buf, 0, buf.length);
            // -1 is expected, but catch all cases with a negative return value.
            if (readThisTime < 0) {
                assertEquals("Invalid return value from stream",
                        -1, readThisTime);
                fail("Reached EOF prematurely, expected " + expectedCount +
                        ", got " + read);
            } else if (readThisTime == 0) {
                // Another special case that should not happen.
                fail("Stream breaks contract, read zero chars: " + reader);
            }
            read += readThisTime;
            if (read == expectedCount) {
                return buf[readThisTime -1];
            } else if (read > expectedCount) {
                fail("Too many chars in stream, expected " + expectedCount +
                        "have " + read + "(EOF not reached/confirmed)");
            }
        }
    }

    /**
     * Create test suite for this test.
     */
    public static Test suite()
    {
        return new BlobClobTestSetup(
                // Reduce lock timeouts so lock test case does not take too long
                DatabasePropertyTestSetup.setLockTimeouts(
                        TestConfiguration.defaultSuite(ClobTest.class, false),
                        2,
                        4));
    }

    private static final String LOCK_TIMEOUT = "40XL1";
} // End class ClobTest
