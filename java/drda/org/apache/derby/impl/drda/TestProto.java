/*

   Derby - Class org.apache.derby.impl.drda.TestProto

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.drda;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.StreamTokenizer;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Vector;
import java.util.Locale;
import java.io.UnsupportedEncodingException;

/**
	This class is used to test error conditions in the protocol.
	The protocol to send to the Net server is contained in a file encoded
	as calls to routines in ddmreader and ddmwriter.
	Additional commands have been added for testing purposes.
	To add tests, modify the file protocol.tests.
	Tests can also be done as separate files and given as an argument to
	this class.
*/

public class TestProto {

	private static final CodePointNameTable codePointNameTable = new CodePointNameTable();
	private static final Hashtable codePointValueTable = new Hashtable();
	private static final Hashtable commandTable = new Hashtable();
	private	static final CcsidManager ccsidManager = new EbcdicCcsidManager();
	//commands
	private static final int CREATE_DSS_REQUEST = 1;
	private static final int CREATE_DSS_OBJECT = 2;
	private static final int END_DSS = 3;
	private static final int END_DDM_AND_DSS = 4;
	private static final int START_DDM = 5;
	private static final int END_DDM = 6;
	private static final int WRITE_BYTE = 7;
	private static final int WRITE_NETWORK_SHORT = 8;
	private static final int WRITE_NETWORK_INT = 9;
	private static final int WRITE_BYTES = 10;
	private static final int WRITE_CODEPOINT_4BYTES = 11;
	private static final int WRITE_SCALAR_1BYTE = 12;
	private static final int WRITE_SCALAR_2BYTES = 13;
	private static final int WRITE_SCALAR_BYTES = 14;
	private static final int WRITE_SCALAR_HEADER = 15;
	private static final int WRITE_SCALAR_STRING = 16;
	private static final int WRITE_SCALAR_PADDED_STRING = 17;
	private static final int WRITE_SCALAR_PADDED_BYTES = 18;
	private static final int WRITE_SHORT = 19;
	private static final int WRITE_INT = 20;
	private static final int WRITE_LONG = 21;
	private static final int WRITE_FLOAT = 22;
	private static final int WRITE_DOUBLE = 23;
	private static final int READ_REPLY_DSS = 24;
	private static final int READ_LENGTH_AND_CODEPOINT = 25;
	private static final int READ_CODEPOINT = 26;
	private static final int MARK_COLLECTION = 27;
	private static final int GET_CODEPOINT = 28;
	private static final int READ_BYTE = 29;
	private static final int READ_NETWORK_SHORT = 30;
	private static final int READ_SHORT = 31;
	private static final int READ_NETWORK_INT = 32;
	private static final int READ_INT = 33;
	private static final int READ_LONG = 34;
	private static final int READ_BOOLEAN = 35;
	private static final int READ_STRING = 36;
	private static final int READ_BYTES = 37;
	private static final int FLUSH = 38;
	private static final int DISPLAY = 39;
	private static final int CHECKERROR = 40;
	private static final int RESET = 41;
	private static final int CREATE_DSS_REPLY = 42;
	private static final int SKIP_DSS = 43;
	private static final int READ_SCALAR_2BYTES = 44;
	private static final int READ_SCALAR_1BYTE = 45;
	private static final int END_TEST = 46;
	private static final int SKIP_DDM = 47;
	private static final int INCLUDE = 48;
	private static final int SKIP_BYTES = 49;
	private static final int WRITE_PADDED_STRING = 50;
	private static final int WRITE_STRING = 51;
	private static final int WRITE_ENCODED_STRING = 52;
	private static final int WRITE_ENCODED_LDSTRING = 53;
	private static final int CHECK_SQLCARD = 54;
	private static final int MORE_DATA = 55;

	private static final String MULTIVAL_START = "MULTIVALSTART";
	private static final String MULTIVAL_SEP = "SEP";
	private static final String MULTIVAL_END = "MULTIVALEND";
	// initialize hash tables
	static {
			init();
			}


	private Socket monitorSocket = null;
	private InputStream monitorIs = null;
	private OutputStream monitorOs = null;
	private DDMWriter writer = new DDMWriter(ccsidManager, null, null);
	private DDMReader reader;
	private boolean failed = false;
	private StreamTokenizer tkn;
	private String current_filename;

	// constructor
	public TestProto(String filename) 
	{
		current_filename = filename;
		getConnection();

		try 
		{
			reader = new DDMReader(ccsidManager, monitorIs);
			processFile(filename);
		}
		catch (Exception e)
		{
			int line = 0;
			if (tkn != null)
				line = tkn.lineno();
			System.err.println("Unexpected exception in line " + line + " file: " + current_filename);
			e.printStackTrace();
		}
		finally
		{
			closeConnection();
		}

	}
	/**
	 * Process include file
	 *
	 * @exception 	IOException, DRDAProtocolException 	error reading file or protocol
	 */
	private void processIncludeFile()
		throws IOException, DRDAProtocolException
	{
		String fileName = getString();
		StreamTokenizer saveTkn = tkn;
		processFile(fileName);
		tkn = saveTkn;
	}
	/**
	 * Process a command file
	 *
	 * @param  filename
	 * @exception 	IOException, DRDAProtocolException 	error reading file or protocol
	 */
	private void processFile(String filename)
		throws IOException, DRDAProtocolException
	{
		String prev_filename = current_filename;
		current_filename = filename;
		FileReader fr = new FileReader(filename);
		tkn = new StreamTokenizer(fr);
		int val;
		while ( (val = tkn.nextToken()) != StreamTokenizer.TT_EOF)
		{
			switch(val)
			{
				case StreamTokenizer.TT_NUMBER:
					break;
				case StreamTokenizer.TT_WORD:
					processCommand();
					break;
				case StreamTokenizer.TT_EOL:
					break;
			}
		}
		current_filename = prev_filename;
	}
	/**
	 * Set up a connection to the Network server
	 */
	private void getConnection() 
	{
		try {
            monitorSocket = new Socket("localhost",1527);
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host: localhost");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to: localhost");
            System.exit(1);
        }
        try
        {
	       monitorIs = monitorSocket.getInputStream();
	       monitorOs = monitorSocket.getOutputStream();
		}
		catch (IOException e)
        {
            System.err.println("Couldn't get I/O for the connection to: localhost");
            System.exit(1);
        }
	}
	/**
	 * Close connection to the network server
	 */
	private void closeConnection()
	{
		try {
			monitorIs.close();
			monitorOs.close();
			monitorSocket.close();
		}
		catch (Exception e) {} //Ignore exceptions when closing the connection
	}
	/**
	 * Reset connection for another test
	 */
	private void reset()
	{
		closeConnection();
		getConnection();
		reader.initialize(monitorIs);
		writer.reset(null);
	}
	/**
	 * Initialize hashtable for commands and set up a table to translate from
	 * the codepoint name to the codepoint value
	 */
	private static void init()
	{
		commandTable.put("createdssrequest", new Integer(CREATE_DSS_REQUEST));
		commandTable.put("createdssobject", new Integer(CREATE_DSS_OBJECT));
		commandTable.put("createdssreply", new Integer(CREATE_DSS_REPLY));
		commandTable.put("enddss", new Integer(END_DSS));
		commandTable.put("enddss", new Integer(END_DSS));
		commandTable.put("endddmanddss", new Integer(END_DDM_AND_DSS));
		commandTable.put("startddm", new Integer(START_DDM));
		commandTable.put("endddm", new Integer(END_DDM));
		commandTable.put("writebyte", new Integer(WRITE_BYTE));
		commandTable.put("writenetworkshort", new Integer(WRITE_NETWORK_SHORT));
		commandTable.put("writenetworkint", new Integer(WRITE_NETWORK_INT));
		commandTable.put("writebytes", new Integer(WRITE_BYTES));
		commandTable.put("writecodepoint4bytes", new Integer(WRITE_CODEPOINT_4BYTES));
		commandTable.put("writescalar1byte", new Integer(WRITE_SCALAR_1BYTE));
		commandTable.put("writescalar2bytes", new Integer(WRITE_SCALAR_2BYTES));
		commandTable.put("writescalarbytes", new Integer(WRITE_SCALAR_BYTES));
		commandTable.put("writescalarheader", new Integer(WRITE_SCALAR_HEADER));
		commandTable.put("writescalarstring", new Integer(WRITE_SCALAR_STRING));
		commandTable.put("writescalarpaddedstring", new Integer(WRITE_SCALAR_PADDED_STRING));
		commandTable.put("writescalarpaddedbytes", new Integer(WRITE_SCALAR_PADDED_BYTES));
		commandTable.put("writeshort", new Integer(WRITE_SHORT));
		commandTable.put("writeint", new Integer(WRITE_INT));
		commandTable.put("writelong", new Integer(WRITE_LONG));
		commandTable.put("writefloat", new Integer(WRITE_FLOAT));
		commandTable.put("writedouble", new Integer(WRITE_DOUBLE));
		commandTable.put("readreplydss", new Integer(READ_REPLY_DSS));
		commandTable.put("readlengthandcodepoint", new Integer(READ_LENGTH_AND_CODEPOINT));
		commandTable.put("readcodepoint", new Integer(READ_CODEPOINT));
		commandTable.put("markcollection", new Integer(MARK_COLLECTION));
		commandTable.put("getcodepoint", new Integer(GET_CODEPOINT));
		commandTable.put("readbyte", new Integer(READ_BYTE));
		commandTable.put("readnetworkshort", new Integer(READ_NETWORK_SHORT));
		commandTable.put("readshort", new Integer(READ_SHORT));
		commandTable.put("readint", new Integer(READ_INT));
		commandTable.put("readlong", new Integer(READ_LONG));
		commandTable.put("readboolean", new Integer(READ_BOOLEAN));
		commandTable.put("readstring", new Integer(READ_STRING));
		commandTable.put("readbytes", new Integer(READ_BYTES));
		commandTable.put("flush", new Integer(FLUSH));
		commandTable.put("display", new Integer(DISPLAY));
		commandTable.put("checkerror", new Integer(CHECKERROR));
		commandTable.put("reset", new Integer(RESET));
		commandTable.put("skipdss", new Integer(SKIP_DSS));
		commandTable.put("skipddm", new Integer(SKIP_DDM));
		commandTable.put("readscalar2bytes", new Integer(READ_SCALAR_2BYTES));
		commandTable.put("readscalar1byte", new Integer(READ_SCALAR_1BYTE));
		commandTable.put("endtest", new Integer(END_TEST));
		commandTable.put("include", new Integer(INCLUDE));
		commandTable.put("skipbytes", new Integer(SKIP_BYTES));
		commandTable.put("writepaddedstring", new Integer(WRITE_PADDED_STRING));
		commandTable.put("writestring", new Integer(WRITE_STRING));
		commandTable.put("writeencodedstring", new Integer(WRITE_ENCODED_STRING));
		commandTable.put("writeencodedldstring", new Integer(WRITE_ENCODED_LDSTRING));
		commandTable.put("checksqlcard", new Integer(CHECK_SQLCARD));
		commandTable.put("moredata", new Integer(MORE_DATA));
		
		Integer key;
		for (Enumeration e = codePointNameTable.keys(); e.hasMoreElements(); )
		{
			key = (Integer)e.nextElement();
			codePointValueTable.put(codePointNameTable.get(key), key);
		}
		
	}
	/**
	 * Process a command
  	 */
	private void processCommand()
		throws IOException, DRDAProtocolException
	{
		Integer icmd  = (Integer)commandTable.get(tkn.sval.toLowerCase(Locale.ENGLISH));
		if (icmd == null)
		{
			System.err.println("Unknown command, " + tkn.sval + " in line " +
				tkn.lineno());
			System.exit(1);
		}
		int cmd  = icmd.intValue();
		int codepoint;
		int val;
		int reqVal;
		String str;

		switch (cmd)
		{
			case INCLUDE:
				processIncludeFile();
				break;
			case CREATE_DSS_REQUEST:
				writer.createDssRequest();
				break;
			case CREATE_DSS_OBJECT:
				writer.createDssObject();
				break;
			case CREATE_DSS_REPLY:
				writer.createDssReply();
				break;
			case END_DSS:
				tkn.nextToken();
				tkn.pushBack();
				if ((tkn.sval != null) && tkn.sval.startsWith("0x"))
				// use specified chaining.
					writer.endDss((getBytes())[0]);
				else
				// use default chaining
					writer.endDss();
				break;
			case END_DDM:
				writer.endDdm();
				break;
			case END_DDM_AND_DSS:
				writer.endDdmAndDss();
				break;
			case START_DDM:
				writer.startDdm(getCP());
				break;
			case WRITE_SCALAR_STRING:
				writer.writeScalarString(getCP(), getString());
				break;
			case WRITE_SCALAR_2BYTES:
				writer.writeScalar2Bytes(getCP(),getIntOrCP());
				break;
			case WRITE_SCALAR_1BYTE:
				writer.writeScalar1Byte(getCP(),getInt());
				break;
			case WRITE_SCALAR_BYTES:
				writer.writeScalarBytes(getCP(),getBytes());
				break;
			case WRITE_SCALAR_PADDED_BYTES:
				writer.writeScalarPaddedBytes(getCP(), getBytes(), getInt(),
					ccsidManager.space);
				break;
			case WRITE_BYTE:
				writer.writeByte(getInt());
				break;
			case WRITE_BYTES:
				writer.writeBytes(getBytes());
				break;
			case WRITE_SHORT:
				writer.writeShort(getInt());
				break;
			case WRITE_INT:
				writer.writeInt(getInt());
				break;
			case WRITE_CODEPOINT_4BYTES:
				writer.writeCodePoint4Bytes(getCP(), getInt());
				break;
			case WRITE_STRING:
				str = getString();
				writer.writeBytes(getEBCDIC(str));
				break;
			case WRITE_ENCODED_STRING:
				writeEncodedString(getString(), getString());
				break;
			case WRITE_ENCODED_LDSTRING:
				writeEncodedLDString(getString(), getString(), getInt());
				break;
			case WRITE_PADDED_STRING:
				str = getString();
				writer.writeBytes(getEBCDIC(str));
				int reqLen = getInt();
				int strLen = str.length();
				if (strLen < reqLen)
					writer.padBytes(ccsidManager.space, reqLen-strLen);
				break;
			case READ_REPLY_DSS:
				reader.readReplyDss();
				break;
			case SKIP_DSS:
				skipDss();
				break;
			case SKIP_DDM:
				skipDdm();
				break;
			case MORE_DATA:
				boolean expbool;
				str = getString();
				if (str.equalsIgnoreCase("true"))
					expbool = true;
				else
					expbool = false;
				if (reader.moreData() && expbool == false )
					fail("Failed - more data left");
				if (!reader.moreData() && expbool == true )
					fail("Failed - no data left");
				break;
			case READ_LENGTH_AND_CODEPOINT:
				readLengthAndCodePoint();
				break;
			case READ_SCALAR_2BYTES:
				readLengthAndCodePoint();
				val = reader.readNetworkShort();
				checkIntOrCP(val);
				break;
			case READ_SCALAR_1BYTE:
				readLengthAndCodePoint();
				val = reader.readByte();
				checkIntOrCP(val);
				break;
			case READ_BYTES:
				byte[] byteArray = reader.readBytes();
				byte[] reqArray = getBytes();
				if (byteArray.length != reqArray.length)
						fail("Failed - byte array didn't match");
				for (int i = 0; i < byteArray.length; i++)
					if (byteArray[i] != reqArray[i])
						fail("Failed - byte array didn't match");
				break;
			case READ_NETWORK_SHORT:
				val = reader.readNetworkShort();
				checkIntOrCP(val);
				break;
			case FLUSH:
				writer.finalizeChain(reader.getCurrChainState(), monitorOs);
				writer.reset(null);
				break;
			case DISPLAY:
				System.out.println(getString());
				break;
			case CHECKERROR:
				checkError();
				break;
			case CHECK_SQLCARD:
				checkSQLCARD(getInt(), getString());
				break;
			case END_TEST:
				// print that we passed the test if we haven't failed
				if (failed == false)
					System.out.println("PASSED");
				failed = false;
				reset();
				break;
			case RESET:
				reset();
				break;
			case SKIP_BYTES:
				reader.skipBytes();
				break;
			default:
				System.out.println("unknown command in line " + tkn.lineno());
				// skip remainder of line
				while (tkn.nextToken() !=  StreamTokenizer.TT_EOL)
						;
	
		}
	}
	/**
	 * Skip a DSS communication 
	 */
	private void skipDss() throws DRDAProtocolException
	{
		reader.readReplyDss();
		reader.skipDss();
	}
	/**
	 * Skip the a Ddm communication
	 */
	private void skipDdm() throws DRDAProtocolException
	{
		reader.readLengthAndCodePoint();
		reader.skipBytes();
	}
	/**
	 * Read an int from the command file
	 * Negative numbers are preceded by "-"
	 */
	private int getInt() throws IOException
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
			if (tkn.sval == null)
			{
				System.err.println("Invalid string on line " + tkn.lineno());
				System.exit(1);
			}
			String str = tkn.sval.toLowerCase(Locale.ENGLISH);
			if (!str.startsWith("0x"))
			{
				System.err.println("Expecting number, got " + tkn.sval + " on line " + tkn.lineno());
				System.exit(1);
			}
			else
				return convertHex(str);
		}
		return (new Double(tkn.nval).intValue() * mult);
	}
	/**
	 * Convert a token in hex format to int from the command file
	 */
	private int convertHex(String str) throws IOException
	{
		int retval = 0;
		int len = str.length(); 
		if ((len % 2) == 1 || len > 10)
		{
			System.err.println("Invalid length for byte string, " + len + 
			" on line " + tkn.lineno());
			System.exit(1);
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
	private boolean checkIntOrCP(int val)  throws IOException
	{
		boolean rval = false;
		int tknType  = tkn.nextToken();
		String reqVal = " ";

		
		if (tknType == StreamTokenizer.TT_WORD && tkn.sval.trim().equals(MULTIVAL_START))
		{
			do {
				int nextVal = getIntOrCP();
				reqVal = reqVal + nextVal + " ";
				// System.out.println("Checking MULTIVAL (" + val + "==" + nextVal + ")");
				rval = rval || (val == nextVal);
				tkn.nextToken();
			}
			while(tkn.sval.trim().equals(MULTIVAL_SEP));
			
			if (! (tkn.sval.trim().equals(MULTIVAL_END)))
				fail("Invalid test file format requires " + MULTIVAL_END + 
					 " got: " + tkn.sval);
			
		}
		else
		{
			tkn.pushBack();
			int nextVal = getIntOrCP();
			reqVal = " " + nextVal;
			// System.out.println("Checking Single Value (" + val + "==" + nextVal + ")");
			rval = (val == nextVal);
		}
		if (rval == false)
			fail("Failed - wrong val = " + val + " Required Value: " + reqVal);

		return rval;
	}


	/**
	 * Read an int or codepoint - codepoint is given as a string
	 */
	private int getIntOrCP() throws IOException
	{
		int val = tkn.nextToken();
		if (val == StreamTokenizer.TT_NUMBER)
		{
			return new Double(tkn.nval).intValue();
		}
		else if (val == StreamTokenizer.TT_WORD)
		{
			return decodeCP(tkn.sval);
		}
		else
		{
			fail("Expecting number, got " + tkn.sval + " on line "
							   + tkn.lineno());
			System.exit(1);
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
	private byte []  getBytes() throws IOException
	{
		byte[] retval = null;
		int val = tkn.nextToken();
		if (tkn.sval == null)
		{
			System.err.println("Invalid string on line " + tkn.lineno());
			System.exit(1);
		}
		String str = tkn.sval.toLowerCase(Locale.ENGLISH);
		if (!str.startsWith("0x"))
		{
			//just convert the string to ebcdic byte array
			return ccsidManager.convertFromUCS2(str);
		}
		else
		{
			int len = str.length(); 
			if ((len % 2) == 1)
			{
				System.err.println("Invalid length for byte string, " + len + 
				" on line " + tkn.lineno());
				System.exit(1);
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
	 * @exception 	IOException 	error reading file
	 */
	private String getString() throws IOException
	{
		int val = tkn.nextToken();
		if (val == StreamTokenizer.TT_NUMBER)
		{
			System.err.println("Expecting word, got " + tkn.nval + " on line " + tkn.lineno());
			System.exit(1);
		}
		return tkn.sval;
	}
	/**
	 * Read the string version of a CodePoint
	 *
	 * @exception 	IOException 	error reading file
	 */
	private int getCP() throws IOException
	{
		String strval = getString();
		return decodeCP(strval);
	}
	/**
	 * Translate a string codepoint such as ACCSEC to the equivalent int value
	 *
	 * @param strval	string codepoint
	 * @return 		integer value of codepoint
	 */
	private int decodeCP(String strval) 
	{
		Integer cp = (Integer)codePointValueTable.get(strval);
		if (cp == null)
		{
		   System.err.println("Unknown codepoint, "+ strval + " in line " 
				+ tkn.lineno());
		   Exception e = new Exception();
		   e.printStackTrace();
			System.exit(1);
		}
		return cp.intValue();
	}
	/**
	 * Print failure message and skip to the next test
 	 *
	 * @exception 	IOException 	error reading file
	 */
	private void fail(String msg) throws IOException
	{
		System.out.println("FAILED - " + msg + " in line " + tkn.lineno());
		// skip remainder of the test look for endtest or end of file
		int val = tkn.nextToken();
		while (val != StreamTokenizer.TT_EOF)
		{
			if (val == StreamTokenizer.TT_WORD && tkn.sval.toLowerCase(Locale.ENGLISH).equals("endtest"))
				break;

			val = tkn.nextToken();
		}
		failed = true;
		// get ready for next test
		reset();
		// print out stack trace so we know where the failure occurred
		Exception e = new Exception();
		e.printStackTrace();
	}
	/**
	 * Check error sent back to application requester
 	 *
	 * @exception 	IOException, DRDAProtocolException 	error reading file or protocol
	 */
	private void checkError() throws IOException, DRDAProtocolException
	{
		int svrcod = 0;
		int invalidCodePoint = 0;
		int prccnvcd = 0;
		int synerrcd = 0;
		int codepoint;
		int reqVal;
		Vector manager = new Vector(), managerLevel = new Vector() ;
		reader.readReplyDss();
		int error = reader.readLengthAndCodePoint();
		int reqCP = getCP();
		if (error != reqCP)
		{
			cpError(error, reqCP);
			return;
		}
		while (reader.moreDssData())
		{
			codepoint = reader.readLengthAndCodePoint();
			switch (codepoint)
			{
				case CodePoint.SVRCOD:
					svrcod = reader.readNetworkShort();
					break;
				case CodePoint.CODPNT:
					invalidCodePoint = reader.readNetworkShort();
					break;
				case CodePoint.PRCCNVCD:
					prccnvcd = reader.readByte();
					break;
				case CodePoint.SYNERRCD:
					synerrcd = reader.readByte();
					break;
				case CodePoint.MGRLVLLS:
					while (reader.moreDdmData())
					{
						manager.addElement(new Integer(reader.readNetworkShort()));
						managerLevel.addElement(new Integer(reader.readNetworkShort()));
					}
					break;
				default:
					//ignore codepoints we don't understand
					reader.skipBytes();
	
			}
		}
		reqVal = getInt();
		if (svrcod != reqVal)
		{
			fail("wrong svrcod val = " + Integer.toHexString(svrcod)
					+ ", required val = " + Integer.toHexString(reqVal));
			return;
		}
		if (error == CodePoint.PRCCNVRM)
		{
			reqVal = getInt();
			if (prccnvcd != reqVal)
			{
				fail("wrong prccnvd, val = " + Integer.toHexString(prccnvcd)
					+ ", required val = " + Integer.toHexString(reqVal));
				return;
			}
		}
		if (error == CodePoint.SYNTAXRM)
		{
			reqVal = getInt();
			if (synerrcd != reqVal)
			{
				fail("wrong synerrcd, val = " + Integer.toHexString(synerrcd)
					+ ", required val = " + Integer.toHexString(reqVal));
				return;
			}
			reqVal = getIntOrCP();
			if (invalidCodePoint != reqVal)
			{
				cpError(invalidCodePoint, reqVal);
				return;
			}
		}
		if (error == CodePoint.MGRLVLRM)
		{
			int mgr, mgrLevel;
			for (int i = 0; i < manager.size(); i++)
			{
				reqVal = getCP();
				mgr = ((Integer)(manager.elementAt(i))).intValue();
				if (mgr != reqVal)
				{
					cpError(mgr, reqVal);
					return;
				}
				mgrLevel = ((Integer)(managerLevel.elementAt(i))).intValue();
				reqVal = getInt();
				if (mgrLevel != reqVal)
				{
					fail("wrong manager level, level = " + Integer.toHexString(mgrLevel)
					+ ", required val = " + Integer.toHexString(reqVal));
					return;
				}
			}
		}
	}
	/**
	 * Read length and codepoint and check against required values
 	 *
	 * @exception 	IOException, DRDAProtocolException 	error reading file or protocol
	 */
	private void readLengthAndCodePoint() throws IOException, DRDAProtocolException
	{
		int codepoint = reader.readLengthAndCodePoint();
		int reqCP = getCP();
		if (codepoint != reqCP)
			cpError(codepoint, reqCP);
	}
	/**
	 * Codepoint error
 	 *
	 * @exception IOException error reading command file
	 */
	private void cpError(int cp, int reqCP) throws IOException
	{
		String cpName = (String)codePointNameTable.get(new Integer(cp));
		String reqCPName = (String)codePointNameTable.get(new Integer(reqCP));
		fail("wrong codepoint val = " + Integer.toHexString(cp) + 
			 "("+cpName+")" +
			 ", required codepoint = " + Integer.toHexString(reqCP) +
			 "("+reqCPName+")");
	}
	/**
	 * Translate a string to EBCDIC for use in the protocol
	 *
	 * @param str	string to transform
	 * @return EBCDIC string
	 */
	private byte[] getEBCDIC(String str)
	{
		byte [] buf = new byte[str.length()];
		ccsidManager.convertFromUCS2(str, buf, 0);
		return buf;
	}
	/**
	 * Write an encoded string
	 *
	 * @param str	string to write
	 * @param encoding	Java encoding to use
	 * @exception IOException
	 */
	private void writeEncodedString(String str, String encoding)
		throws IOException
	{
		try {
			byte [] buf = str.getBytes(encoding);
			writer.writeBytes(buf);
		} catch (UnsupportedEncodingException e) {
			fail("Unsupported encoding " + encoding);	
		}
	}
	/**
	 * Write length and encoded string
	 *
	 * @param str string to write
	 * @param encoding	Java encoding to use
	 * @param len			Size of length value (2 or 4 bytes)
	 * @exception IOException
	 */
	private void writeEncodedLDString(String str, String encoding, int len)
		throws IOException
	{
		try {
			byte [] buf = str.getBytes(encoding);
			if (len == 2)
				writer.writeShort(buf.length);
			else
				writer.writeInt(buf.length);
			writer.writeBytes(buf);
		} catch (UnsupportedEncodingException e) {
			fail("Unsupported encoding " + encoding);	
		}
	}
	/**
	 * Check the value of SQLCARD
	 *
	 * @param sqlCode	SQLCODE value
	 * @param sqlState	SQLSTATE value
	 * @exception IOException, DRDAProtocolException
	 */
	private void checkSQLCARD(int sqlCode, String sqlState)
		throws IOException, DRDAProtocolException
	{
		reader.readReplyDss();
		int codepoint = reader.readLengthAndCodePoint();
		if (codepoint != CodePoint.SQLCARD)
		{
			fail("Expecting SQLCARD got "+ Integer.toHexString(codepoint));
			return;
		}
		int nullind = reader.readByte();
		//cheating here and using readNetworkInt since the byteorder is the same
		int code = reader.readNetworkInt();
		if (code != sqlCode)
		{
			fail("Expecting sqlCode " + sqlCode + " got "+ Integer.toHexString(code));
			return;
		}
		String state = reader.readString(5, "UTF-8");
		if (!state.equals(sqlState))
		{
			fail("Expecting sqlState " + sqlState + " got "+ state);
			return;
		}
		// skip the rest of the SQLCARD
		reader.skipBytes();
	}
}
