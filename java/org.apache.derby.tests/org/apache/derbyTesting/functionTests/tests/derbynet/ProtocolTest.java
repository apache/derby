/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.ProtocolTest

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import junit.framework.Test;
import org.apache.derby.impl.drda.ProtocolTestAdapter;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.ProtocolTestGrammar;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This class is used to test error conditions in the protocol.
 * <p>
 * The protocol to send to the Net server is contained in a file encoded
 * as calls to routines in {@code DDMReader} and {@code DDMWriter}.
 * Additional commands have been added for testing purposes.
 * To add tests, modify the file protocol.tests.
 * Tests can also be done as separate files and given as an argument to
 * this class.
 * <p>
 * This test was copied / converted from testProto.
 */
public class ProtocolTest
    extends BaseTestCase {

    private static final String PREFIX =
        "org/apache/derbyTesting/functionTests/tests/derbynet/";
    /** Newline character(s). */
    private static final String NL =
            BaseTestCase.getSystemProperty("line.separator");

    // Constants used to encode / represent multiple values for a command.
    private static final String MULTIVAL_START = "MULTIVALSTART";
    private static final String MULTIVAL_SEP = "SEP";
    private static final String MULTIVAL_END = "MULTIVALEND";

    // Replaces %UTF8TestString% in protocol.tests
    private static final String UTF8_TEST_MATCH = "%UTF8TestString%";
    private static final String UTF8_TEST_STRING =
            "\u4f60\u597d\u4e16\u754cABCDEFGHIJKLMNOPQ";


    /** Name of the file from which the test is obtained. */
    private final String filename;
    /** The start line of the test from the input file. */
    private final int startLine;
    /** Number of lines this test is composed of. */
    private final int lineCount;
    /**
     * The sequence of commands this test is composed of.
     * <p>
     * The sequence is expected to be line oriented, that is a one command and
     * optionally its arguments per line. The lines must be separated by a
     * newline token.
     */
    private String commandSequence;
    /**
     * The adapter that lives on the DRDA package, allowing us to call the
     * otherwise unavailable methods we need to call in package private
     * classes.
     */
    private ProtocolTestAdapter adapter;

    /**
     * Creates a new test case.
     *
     * @param file name of file containing the protocol tests (for verbosity)
     * @param cmds the sequence of commands (see {@link #commandSequence})
     * @param startLine starting line in the source file
     * @param lines number of lines from the source file
     */
    private ProtocolTest(String file, String cmds, int startLine, int lines) {
        super("testProtocolSequence");
        this.filename = file;
        this.commandSequence = cmds;
        this.startLine = startLine;
        this.lineCount = lines;
    }

    /**
     * Executes the test sequence.
     *
     * @throws IOException if accessing a file or the socket fails, or if
     *      Derby detects an unexpected protocol error
     * @throws UnknownHostException if the server host cannot be resolved
     */
    public void testProtocolSequence()
            throws IOException, UnknownHostException {
        // Update the name of the test.
        super.setName(
                filename + "_" + startLine + "_" + (startLine + lineCount));
        println(getName() + " :: STARTED");
        // Validate the line count. Expects a newline also at end of last line.
        assertEquals("Actual line count does not match the specified count",
                this.lineCount, this.commandSequence.length() -
                this.commandSequence.replaceAll("\n", "").length() -1);

        adapter = new ProtocolTestAdapter(createSocket());
        Reader cmdStream = new StringReader(this.commandSequence);
        try {
            processCommands(cmdStream);
        } finally {
            try {
                adapter.close();
            } catch (IOException ioe) {
                // Don't act on the exception, but print its message.
                alarm("adapter.close() failed: " + ioe.getMessage());
            }
        }
    }

    /** Cleans up test resources. */
    public void tearDown()
            throws Exception {
        commandSequence = null;
        adapter.close();
        adapter = null;
        super.tearDown();
    }

    /**
     * Initializes a socket to the server.
     *
     * @return A socket connected to the server.
     * @throws IOException if reading/writing to the socket fails
     * @throws UnknownHostException if the server host cannot be resolved
     */
    private static Socket createSocket()
            throws IOException, UnknownHostException {
        Socket socket = null;
        final TestConfiguration cfg = TestConfiguration.getCurrent();
        try {
            socket = AccessController.doPrivileged (
                new java.security.PrivilegedExceptionAction<Socket>() {
                    public Socket run()
                            throws IOException, UnknownHostException {
                        return new Socket(cfg.getHostName(), cfg.getPort());
                    }
                }
            );
        } catch (PrivilegedActionException pae) {
            if (pae.getCause() instanceof IOException) {
                throw (IOException)pae.getCause();
            } else if (pae.getCause() instanceof UnknownHostException) {
                throw (UnknownHostException)pae.getCause();
            }
            fail("Unhandled exception", pae);
        }
        return socket;
    }

    /**
     * Executes the test commands in the specified file.
     *
     * @param fileName file containing the test
     * @throws IOException if accessing a file or the socket fails, or if
     *      Derby detects an unexpected protocol error
     */
    private void processFile(String fileName)
            throws IOException {
        File incFile = SupportFilesSetup.getReadOnly(fileName);
        assertTrue("Missing file: " + fileName,
               PrivilegedFileOpsForTests.exists(incFile));
        BufferedReader bIn = new BufferedReader(
               PrivilegedFileOpsForTests.getFileReader(incFile));
        try {
            processCommands(bIn);
        } finally {
            bIn.close();
        }
    }

    /**
     * Processes the test commands in the stream.
     *
     * @param cmdStream command stream (see {@link #commandSequence})
     * @throws IOException if accessing a file or the socket fails, or if
     *      Derby detects an unexpected protocol error
     */
    private void processCommands(Reader cmdStream)
            throws  IOException {
        StreamTokenizer tkn = new StreamTokenizer(cmdStream);
        boolean endSignalled = false;
        int val;
        while ( (val = tkn.nextToken()) != StreamTokenizer.TT_EOF) {
            assertFalse("End signalled, data to process left: " + tkn.sval,
                    endSignalled);
            switch(val) {
                case StreamTokenizer.TT_NUMBER:
                    break;
                case StreamTokenizer.TT_WORD:
                    endSignalled = processCommand(tkn);
                    break;
                case StreamTokenizer.TT_EOL:
                    break;
            }
        }
    }

    /**
     * Process a command.
     */
    private boolean processCommand(StreamTokenizer tkn)
        throws IOException
    {
        ProtocolTestGrammar cmd = ProtocolTestGrammar.cmdFromString(
                                        tkn.sval.toLowerCase(Locale.ENGLISH));
        if (cmd == null) { // To avoid generating string for each command.
            fail("Unknown command '" + tkn.sval + "' in line " + ln(tkn));
        }
        int val;
        String str;

        switch (cmd)
        {
            case INCLUDE:
                processFile(getString(tkn));
                break;
            case CREATE_DSS_REQUEST:
                adapter.wCreateDssRequest();
                break;
            case CREATE_DSS_OBJECT:
                adapter.wCreateDssObject();
                break;
            case CREATE_DSS_REPLY:
                adapter.wCreateDssReply();
                break;
            case END_DSS:
                tkn.nextToken();
                tkn.pushBack();
                if ((tkn.sval != null) && tkn.sval.startsWith("0x"))
                // use specified chaining.
                    adapter.wEndDss((getBytes(tkn))[0]);
                else
                // use default chaining
                    adapter.wEndDss();
                break;
            case END_DDM:
                adapter.wEndDdm();
                break;
            case END_DDM_AND_DSS:
                adapter.wEndDdmAndDss();
                break;
            case START_DDM:
                adapter.wStartDdm(getCP(tkn));
                break;
            case WRITE_SCALAR_STRING:
                adapter.wWriteScalarString(getCP(tkn), getString(tkn));
                break;
            case WRITE_SCALAR_2BYTES:
                adapter.wWriteScalar2Bytes(getCP(tkn),getIntOrCP(tkn));
                break;
            case WRITE_SCALAR_1BYTE:
                adapter.wWriteScalar1Byte(getCP(tkn),getInt(tkn));
                break;
            case WRITE_SCALAR_BYTES:
                adapter.wWriteScalarBytes(getCP(tkn),getBytes(tkn));
                break;
            case WRITE_SCALAR_PADDED_BYTES:
                adapter.wWriteScalarPaddedBytes(getCP(tkn), getBytes(tkn),
                        getInt(tkn), ProtocolTestAdapter.SPACE);
                break;
            case WRITE_BYTE:
                adapter.wWriteByte(getInt(tkn));
                break;
            case WRITE_BYTES:
                adapter.wWriteBytes(getBytes(tkn));
                break;
            case WRITE_SHORT:
                adapter.wWriteShort(getInt(tkn));
                break;
            case WRITE_INT:
                adapter.wWriteInt(getInt(tkn));
                break;
            case WRITE_CODEPOINT_4BYTES:
                adapter.wWriteCodePoint4Bytes(getCP(tkn), getInt(tkn));
                break;
            case WRITE_STRING:
                str = getString(tkn);
                adapter.wWriteBytes(getEBCDIC(str));
                break;
            case WRITE_ENCODED_STRING:
                writeEncodedString(getString(tkn), getString(tkn));
                break;
            case WRITE_ENCODED_LDSTRING:
                writeEncodedLDString(getString(tkn), getString(tkn), getInt(tkn));
                break;
            case WRITE_PADDED_STRING:
                str = getString(tkn);
                adapter.wWriteBytes(getEBCDIC(str));
                int reqLen = getInt(tkn);
                int strLen = str.length();
                if (strLen < reqLen)
                    adapter.wPadBytes(ProtocolTestAdapter.SPACE, reqLen-strLen);
                break;
            case READ_REPLY_DSS:
                adapter.rReadReplyDss();
                break;
            case SKIP_DSS:
                adapter.rSkipDss();
                break;
            case SKIP_DDM:
                adapter.rSkipDdm();
                break;
            case MORE_DATA:
                str = getString(tkn);
                boolean expbool = Boolean.parseBoolean(str);
                assertEquals("Too much/little data",
                        expbool, adapter.rMoreData());
                break;
            case READ_LENGTH_AND_CODEPOINT:
                readLengthAndCodePoint(tkn);
                break;
            case READ_SCALAR_2BYTES:
                readLengthAndCodePoint(tkn);
                val = adapter.rReadNetworkShort();
                checkIntOrCP(tkn, val);
                break;
            case READ_SCALAR_1BYTE:
                readLengthAndCodePoint(tkn);
                val = adapter.rReadByte();
                checkIntOrCP(tkn, val);
                break;
            case READ_SECMEC_AND_SECCHKCD:
                readSecMecAndSECCHKCD();
                break;
            case READ_BYTES:
                assertTrue("Mismatch between the byte arrays",
                        Arrays.equals(getBytes(tkn), adapter.rReadBytes()));
                break;
            case READ_NETWORK_SHORT:
                val = adapter.rReadNetworkShort();
                checkIntOrCP(tkn, val);
                break;
            case FLUSH:
                adapter.wFlush();
                break;
            case DISPLAY:
                println(getString(tkn));
                break;
            case CHECKERROR:
                checkError(tkn);
                break;
            case CHECK_SQLCARD:
                checkSQLCARD(getInt(tkn), getString(tkn));
                break;
            case END_TEST:
                // Nothing to do for ending the test, as resources are closed
                // elsewhere. Note that each test case will get it's own
                //  connection and set of resources.
                println(getName() + " :: FINISHED");
                return true;
            case SKIP_BYTES:
                adapter.rSkipBytes();
                break;
            case SWITCH_TO_UTF8_CCSID_MANAGER:
                adapter.setUtf8Ccsid();
                break;
            case DELETE_DATABASE:
                deleteDatabase(getString(tkn));
                break;
            default:
                fail("Command in line " + ln(tkn) + " not implemented: " +
                        cmd.toString());
        }
        return false;
    }

    /**
     * Read an int from the command file
     * Negative numbers are preceded by "-"
     */
    private int getInt(StreamTokenizer tkn) throws IOException
    {
        int mult = 1;
        int val = tkn.nextToken();
        if (tkn.sval != null && tkn.sval.equals("-"))
        {
            mult = -1;
            val = tkn.nextToken();
        }

        if (val != StreamTokenizer.TT_NUMBER)
        {
            assertNotNull("Invalid string on line " + ln(tkn), tkn.sval);
            String str = tkn.sval.toLowerCase(Locale.ENGLISH);
            if (!str.startsWith("0x")) {
                fail("Expecting number, got " + tkn.sval + " on line " +
                        ln(tkn));
            } else {
                return convertHex(str, ln(tkn));
            }
        }
        return Double.valueOf(tkn.nval).intValue() * mult;
    }

    /**
     * Convert a token in hex format to int from the command file
     */
    private int convertHex(String str, int lineNumber) throws IOException
    {
        int retval = 0;
        int len = str.length();
        if ((len % 2) == 1 || len > 10)
        {
            fail("Invalid length for byte string, " + len +
                    " on line " + lineNumber);
        }
        for (int i = 2; i < len; i++)
        {
            retval = retval << 4;
            retval += Byte.valueOf(str.substring(i, i+1), 16).byteValue();
        }
        return retval;
    }

    /**
     * checks if value matches next int or cp.
     * Handles multiple legal values in protocol test file
     * FORMAT for Multiple Values
     * MULTIVALSTART 10 SEP 32 SEP 40 MULTIVALEND
     **/
    private boolean checkIntOrCP(StreamTokenizer tkn, int val)
            throws IOException {
        boolean rval = false;
        int tknType = tkn.nextToken();
        String reqVal = " ";

        if (tknType == StreamTokenizer.TT_WORD &&
                tkn.sval.trim().equals(MULTIVAL_START)) {
            do {
                int nextVal = getIntOrCP(tkn);
                reqVal = reqVal + nextVal + " ";
                rval = rval || (val == nextVal);
                tkn.nextToken();
            }
            while(tkn.sval.trim().equals(MULTIVAL_SEP));

            if (! (tkn.sval.trim().equals(MULTIVAL_END)))
                fail("Invalid test file format, requires " + MULTIVAL_END +
                     ", got: " + tkn.sval);
        }
        else
        {
            tkn.pushBack();
            int nextVal = getIntOrCP(tkn);
            reqVal = " " + nextVal;
            rval = (val == nextVal);
        }
        assertTrue("Expected '" + reqVal + "', got '" + val + "'", rval);

        return rval;
    }


    /**
     * Read an int or codepoint - codepoint is given as a string
     */
    private int getIntOrCP(StreamTokenizer tkn) throws IOException
    {
        int val = tkn.nextToken();
        if (val == StreamTokenizer.TT_NUMBER) {
            return Double.valueOf(tkn.nval).intValue();
        } else if (val == StreamTokenizer.TT_WORD) {
            return decodeCP(tkn.sval, ln(tkn));
        } else {
            fail("Expecting number, got '" + tkn.sval + "' on line " + ln(tkn));
        }
        return 0;
    }

    /**
     * Read an array of bytes from the command file
     * A byte string can start with 0x in which case the bytes are interpreted
     * in hex format or it can just be a string, in which case each char is
     * interpreted as  2 byte UNICODE
     *
     * @return byte array
     */
    private byte []  getBytes(StreamTokenizer tkn) throws IOException
    {
        byte[] retval;
        tkn.nextToken();
        assertNotNull("Missing input on line " + ln(tkn), tkn.sval);
        String str = tkn.sval.toLowerCase(Locale.ENGLISH);
        if (!str.startsWith("0x"))
        {
            //just convert the string to ebcdic byte array
            return getEBCDIC(str);
        }
        else
        {
            int len = str.length();
            if ((len % 2) == 1) {
                fail("Invalid length for byte string, " + len +
                        " on line " + ln(tkn));
            }
            retval = new byte[(len-2)/2];
            int j = 0;
            for (int i = 2; i < len; i+=2, j++)
            {
                retval[j] = (byte)(Byte.valueOf(str.substring(i, i+1), 16).byteValue() << 4);
                retval[j] += Byte.valueOf(str.substring(i+1, i+2), 16).byteValue();
            }
        }
        return retval;
    }

    /**
     * Read a string from the command file
     *
     * @return string found in file
     * @exception     IOException     error reading file
     */
    private String getString(StreamTokenizer tkn) throws IOException
    {
        int val = tkn.nextToken();
        assertTrue("Expected string, got number '" + tkn.nval + "' on line " +
                ln(tkn), val != StreamTokenizer.TT_NUMBER);

        /* Check whether '%UTF8TestString%' is in the string and replace
         * it with Chinese characters for the UTF8 protocol tests */
        if (tkn.sval.contains(UTF8_TEST_MATCH))
            return tkn.sval.replace(UTF8_TEST_MATCH, UTF8_TEST_STRING);

        return tkn.sval;
    }

    /**
     * Read the string version of a CodePoint
     *
     * @exception     IOException     error reading file
     */
    private int getCP(StreamTokenizer tkn)
            throws IOException {
        String strval = getString(tkn);
        return decodeCP(strval, ln(tkn));
    }

    /**
     * Translate a string codepoint such as ACCSEC to the equivalent int value
     *
     * @param strval    string codepoint
     * @return         integer value of codepoint
     */
    private int decodeCP(String strval, int lineNumber) {
        Integer cp = adapter.decodeCodePoint(strval);
        assertNotNull("Unknown codepoint '" + strval + "' in line " +
                lineNumber, cp);
        return cp.intValue();
    }

    /**
     * Check error sent back to application requester
      *
     * @exception     IOException error reading file or protocol
     */
    private void checkError(StreamTokenizer tkn)
            throws IOException {
        int svrcod = 0;
        int invalidCodePoint = 0;
        int prccnvcd = 0;
        int synerrcd = 0;
        int codepoint;
        int reqVal;
        ArrayList<Integer> manager = new ArrayList<Integer>();
        ArrayList<Integer> managerLevel = new ArrayList<Integer>() ;
        adapter.rReadReplyDss();
        int error = adapter.rReadLengthAndCodePoint( false );
        int reqCP = getCP(tkn);
        assertCP(reqCP, error);
        while (adapter.rMoreDssData())
        {
            codepoint = adapter.rReadLengthAndCodePoint( false );
            switch (codepoint)
            {
                case ProtocolTestAdapter.CP_SVRCOD:
                    svrcod = adapter.rReadNetworkShort();
                    break;
                case ProtocolTestAdapter.CP_CODPNT:
                    invalidCodePoint = adapter.rReadNetworkShort();
                    break;
                case ProtocolTestAdapter.CP_PRCCNVCD:
                    prccnvcd = adapter.rReadByte();
                    break;
                case ProtocolTestAdapter.CP_SYNERRCD:
                    synerrcd = adapter.rReadByte();
                    break;
                case ProtocolTestAdapter.CP_MGRLVLLS:
                    while (adapter.rMoreDdmData())
                    {
                        manager.add(Integer.valueOf(adapter.rReadNetworkShort()));
                        managerLevel.add(Integer.valueOf(adapter.rReadNetworkShort()));
                    }
                    break;
                default:
                    //ignore codepoints we don't understand
                    adapter.rSkipBytes();
                    println("Skipped bytes for codepoint " + codepoint + " (" +
                            adapter.lookupCodePoint(codepoint) + ")");
            }
        }
        reqVal = getInt(tkn);
        assertEquals("Wrong svrcod (0x" + Integer.toHexString(reqVal) +
                " != 0x" + Integer.toHexString(svrcod) + ")", reqVal, svrcod);
        if (error == ProtocolTestAdapter.CP_PRCCNVRM) {
            reqVal = getInt(tkn);
            assertEquals("Wrong prccnvcd (0x" + Integer.toHexString(reqVal) +
                " != 0x" + Integer.toHexString(prccnvcd) + ")",
                reqVal, prccnvcd);
        }
        if (error == ProtocolTestAdapter.CP_SYNTAXRM) {
            reqVal = getInt(tkn);
            assertEquals("Wrong synerrcd (0x" + Integer.toHexString(reqVal) +
                " != 0x" + Integer.toHexString(synerrcd) + ")",
                reqVal, synerrcd);
            reqVal = getIntOrCP(tkn);
            assertCP(reqVal, invalidCodePoint);
        }
        if (error == ProtocolTestAdapter.CP_MGRLVLRM)
        {
            int mgr, mgrLevel;
            for (int i = 0; i < manager.size(); i++)
            {
                reqVal = getCP(tkn);
                mgr = manager.get(i);
                assertCP(reqVal, mgr);
                mgrLevel = managerLevel.get(i);
                reqVal = getInt(tkn);
                assertEquals("Wrong manager level (0x" +
                        Integer.toHexString(reqVal) + " != 0x" +
                        Integer.toHexString(mgrLevel) + ")", reqVal, mgrLevel);
            }
        }
    }

    /**
     * Read length and codepoint and check against required values
      *
     * @exception     IOException error reading file or protocol
     */
    private void readLengthAndCodePoint(StreamTokenizer tkn)
            throws IOException {
        int codepoint = adapter.rReadLengthAndCodePoint( false );
        int reqCP = getCP(tkn);
        assertCP(reqCP, codepoint);
    }

    /**
     * Handle the case of testing the reading of SECMEC and SECCHKCD,
     * where on an invalid SECMEC value for ACCSEC, the server can send
     * valid supported SECMEC values. One of the valid supported value can be
     * EUSRIDPWD (secmec value of 9) depending on if the server JVM
     * can actually support it or not.
     * @exception   IOException error reading file or protocol
     */
    private void readSecMecAndSECCHKCD() throws IOException
    {
        int codepoint;
        boolean notDone = true;
        int val;
        do
        {
            codepoint = adapter.rReadLengthAndCodePoint( false );
            switch(codepoint)
            {
              case ProtocolTestAdapter.CP_SECMEC:
              {
                  val = adapter.rReadNetworkShort();
                  println("SECMEC=" + val);
              }
              break;
              case ProtocolTestAdapter.CP_SECCHKCD:
              {
                  val = adapter.rReadByte();
                  println("SECCHKCD=" + val);
                  notDone = false;
              }
              break;
              default:
                  notDone=false;
            }
        }while(notDone);
    }

    /**
     * Codepoint error
      *
     * @exception IOException error reading command file
     */
    private void assertCP(int reqCP, int cp) {
        String cpName = adapter.lookupCodePoint(cp);
        String reqCPName = adapter.lookupCodePoint(reqCP);
        assertEquals("Wrong codepoint (0x" + Integer.toHexString(reqCP) +
             "/" + reqCPName + " != 0x" + Integer.toHexString(cp) +
             "/" + cpName + ")", reqCP, cp);
    }

    /**
     * Translate a string to EBCDIC for use in the protocol
     *
     * @param str    string to transform
     * @return EBCDIC string
     */
    private byte[] getEBCDIC(String str)
    {
        return adapter.convertFromJavaString(str);
    }

    /**
     * Write an encoded string
     *
     * @param str    string to write
     * @param encoding    Java encoding to use
     * @exception IOException
     */
    private void writeEncodedString(String str, String encoding) {
        try {
            byte [] buf = str.getBytes(encoding);
            adapter.wWriteBytes(buf);
        } catch (UnsupportedEncodingException e) {
            fail("Unsupported encoding " + encoding, e);
        }
    }

    /**
     * Write length and encoded string
     *
     * @param str string to write
     * @param encoding    Java encoding to use
     * @param len            Size of length value (2 or 4 bytes)
     * @exception IOException
     */
    private void writeEncodedLDString(String str, String encoding, int len) {
        try {
            byte [] buf = str.getBytes(encoding);
            if (len == 2)
                adapter.wWriteShort(buf.length);
            else
                adapter.wWriteInt(buf.length);
            adapter.wWriteBytes(buf);
        } catch (UnsupportedEncodingException e) {
            fail("Unsupported encoding " + encoding, e);
        }
    }

    /**
     * Check the value of SQLCARD
     *
     * @param sqlCode    SQLCODE value
     * @param sqlState    SQLSTATE value
     * @exception IOException, DRDAProtocolException
     */
    private void checkSQLCARD(int sqlCode, String sqlState)
            throws IOException {
        adapter.rReadReplyDss();
        int codepoint = adapter.rReadLengthAndCodePoint( false );
        assertEquals("Expected SQLCARD (0x2408), got " +
                Integer.toHexString(codepoint), ProtocolTestAdapter.CP_SQLCARD,
                codepoint);
        adapter.rReadByte(); // Skip null indicator.
        //cheating here and using readNetworkInt since the byteorder is the same
        int code = adapter.rReadNetworkInt();
        assertEquals("Expected " + Integer.toHexString(sqlCode) + ", got " +
                Integer.toHexString(code), sqlCode, code);
        String state = adapter.rReadString(5, "UTF-8");
        assertEquals("Wrong SQL state", sqlState, state);
        // skip the rest of the SQLCARD
        adapter.rSkipBytes();
    }

    /**
     * Calculates the current line number from the source file.
     * <p>
     * The intention is to be able to print which line number the test failed
     * on, so that the source of the error can be easily located.
     *
     * @param st processing stream tokenizer
     * @return The calculated current line number.
     */
    private int ln(StreamTokenizer st) {
        return (this.startLine + st.lineno() -1);
    }

    /**
     * Delete the database with the name 'name'
     * @param name Name of the database to delete
     */
    private void deleteDatabase(String name) {
        String shutdownUrl = "jdbc:derby:"+name+";shutdown=true";
        try {
            DriverManager.getConnection(shutdownUrl);
        } catch (SQLException sqle) {
            // ignore shutdown exception
            BaseJDBCTestCase.assertSQLState("08006", sqle);
        }
        removeDirectory(getSystemProperty("derby.system.home") +
                File.separator + name);
    }

    /**
     * Creates a suite of tests dynamically from a file describing protocol
     * tests.
     *
     * @return A suite of tests.
     * @throws Exception if creating the suite fails for some reason
     */
    public static Test suite()
            throws Exception {
        BaseTestSuite suite = new BaseTestSuite("Derby DRDA protocol tests");
        // Process the list of files and create test cases for the sub-tests.
        // NOTE: We cannot assume anything about the order in which the tests
        //      are executed.
        final String testFile = PREFIX + "protocol.tests";
        final URL testFileURL = BaseTestCase.getTestResource(testFile);
        BufferedReader bIn = new BufferedReader(
                new InputStreamReader(
                    openTestResource(testFileURL),
                    Charset.forName("UTF-8")));

        // Split the tests into individual tests.
        final String END_TEST = ProtocolTestGrammar.END_TEST.toCmdString();
        int currentLine = 1;
        int startLine = 1; // First line is line number one.
        ArrayList<String> cmdLines = new ArrayList<String>();
        StringBuilder str = new StringBuilder();
        String line;
        // Iterate until the end of test token is reached.
        while ((line = bIn.readLine()) != null) {
            cmdLines.add(line);
            str.append(line).append(NL);
            if (line.toLowerCase(Locale.ENGLISH).startsWith(END_TEST)) {
                // Create a new test case.
                suite.addTest(new ProtocolTest(
                        "protocol.tests",
                        str.toString(),
                        startLine,
                        currentLine - startLine));
                cmdLines.clear();
                str.setLength(0);
                startLine = currentLine +1;
            }
            currentLine++;
        }
        bIn.close();

        // Install a security policy and copy the required include files.
        final String resourcePath = "functionTests/tests/derbynet";
        return new SecurityManagerSetup(
                TestConfiguration.clientServerDecorator(
                    new SupportFilesSetup(suite, new String[] {
                        resourcePath + "/connect.inc",
                        resourcePath + "/excsat_accsecrd1.inc",
                        resourcePath + "/excsat_accsecrd2.inc",
                        resourcePath + "/excsat_accsecrd_nordb.inc",
                        resourcePath + "/excsat_secchk_nordbonaccsec.inc",
                        resourcePath + "/excsat_secchk.inc",
                        resourcePath + "/values1.inc",
                        resourcePath + "/values64kblksz.inc",
                    })),
                PREFIX + "ProtocolTest.policy",
                true);
    }
}
