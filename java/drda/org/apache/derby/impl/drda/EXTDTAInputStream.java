/*
 
 Derby - Class org.apache.derby.impl.drda.DRDAStatement

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.impl.jdbc.Util;

/**
 * @author marsden
 * 
 * EXTDTAObjectHolder provides Externalized Large Object representation that
 * does not hold locks until the end of the transaction (DERBY-255)
 * 
 * It serves as a holder for lob data and is only valid as long as the original
 * result set from which it came is on the same row.  
 * 
 *  
 */
public class EXTDTAInputStream extends InputStream {

	long dataLength = 0; // length of the stream;

	InputStream binaryInputStream = null;

	int columnNumber;

	ResultSet dataResultSet = null;
	
	
	/**
	 * @param dataLength
	 * @param binaryInputStream
	 */
	private EXTDTAInputStream( int dataLength, InputStream binaryInputStream) {
		
		this.dataLength = dataLength;
		this.binaryInputStream = binaryInputStream;
	}

	/**
	 * Retrieve stream from the ResultSet and column specified.  Create an
	 * input stream and length for the large object being retrieved. Do not hold
	 * locks until end of transaction. DERBY-255.
	 * 
	 * 
	 * @see DDMWriter.writeScalarStream
	 * 
	 * @param rs
	 *            result set from which to retrieve the lob
	 * @param column
	 *            column number
	 * @param drdaType
	 *            FD:OCA type of object one of
	 * 			   FdocaConstants.DRDA_TYPE_NLOBBYTES
	 * 			   FdocaConstants.DRDA_TYPE_LOBBYTES
	 * 			   FdocaConstants.DRDA_TYPE_NLOBCMIXED
	 *  		   FdocaConstants.DRDA_TYPE_LOBCMIXED
	 * 
	 * @returns null if the value is null or a new EXTDTAInputStream corresponding to 
	 *  		rs.getBinaryStream(column) value and associated length
	 * 
	 * @throws SQLException
	 */
	public static EXTDTAInputStream getEXTDTAStream(ResultSet rs, int column, int drdaType) 
			throws SQLException {
		
		EXTDTAInputStream extdtaStream = null;
		int length = 0;
		byte[] bytes = null;
		
		int ndrdaType = drdaType | 1; //nullable drdaType
		// BLOBS
		if (ndrdaType == FdocaConstants.DRDA_TYPE_NLOBBYTES) 
		{
			//TODO: Change to just use rs.getBinaryStream() by 
			// eliminating the need for a length parameter in
			//DDMWriter.writeScalarStream and therefore eliminating the need for dataLength in this class
			bytes = rs.getBytes(column);
			
		}
		// CLOBS
		else if (ndrdaType ==  FdocaConstants.DRDA_TYPE_NLOBCMIXED)
		{	
			//TODO: Change to use getCharacterStream and change the read method
			// to stream the data after length is no longer needed in DDMWRiter.writeScalarStream
			String s  = rs.getString(column);
			try {
				if (s != null)
					bytes = s.getBytes(NetworkServerControlImpl.DEFAULT_ENCODING);
			}
			catch (java.io.UnsupportedEncodingException e) {
				throw new SQLException (e.getMessage());
			}
		}
		else
		{
			SanityManager.THROWASSERT("DRDAType: " + drdaType +
						" not valid EXTDTA object type");
		}
		
		if (bytes != null)
		{
			length = bytes.length;
			InputStream is = new ByteArrayInputStream(bytes);
			extdtaStream =  new EXTDTAInputStream(length, is);
		}
		
		return extdtaStream;
	}

	
	/**
	 * Get the length of the InputStream 
	 * This method is currently not used because there seems to be no way to 
	 * reset the she stream.
	 *   
	 * @param binaryInputStream
	 *            an InputStream whose length needs to be calclulated
	 * @return length of stream
	 */
	private static long getInputStreamLength(InputStream binaryInputStream)
			throws SQLException {
		long length = 0;
		if (binaryInputStream == null)
			return length;
		
		try {
			for (;;) {
				int avail = binaryInputStream.available();
				binaryInputStream.skip(avail);
				if (avail == 0)
					break;
				length += avail;
				
			}
			//binaryInputStream.close();
		} catch (IOException ioe) {
			throw Util.javaException(ioe);
		}

		return length;

	}
	
	
	
	/**
	 * Return the length of the binary stream which was calculated when
	 * EXTDTAObject was created.
	 * 
	 * @return the length of the stream once converted to an InputStream
	 */
	public long length() throws SQLException {
		return dataLength;
		
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#read()
	 */
	public int read() throws IOException {
		return binaryInputStream.read();
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#available()
	 */
	public int available() throws IOException {
		return binaryInputStream.available();
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#close()
	 */
	public void close() throws IOException {
		if (binaryInputStream != null)
			binaryInputStream.close();	
	}

	/**
	 * 
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object arg0) {
		return binaryInputStream.equals(arg0);
	}

	/**
	 * 
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return binaryInputStream.hashCode();
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#mark(int)
	 */
	public void mark(int arg0) {
		binaryInputStream.mark(arg0);
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#markSupported()
	 */
	public boolean markSupported() {
		return binaryInputStream.markSupported();
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#read(byte[])
	 */
	public int read(byte[] arg0) throws IOException {
		return binaryInputStream.read(arg0);
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#read(byte[], int, int)
	 */
	public int read(byte[] arg0, int arg1, int arg2) throws IOException {
		return binaryInputStream.read(arg0, arg1, arg2);
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#reset()
	 */
	public void reset() throws IOException {
		binaryInputStream.reset();
	}

	/**
	 * 
	 * 
	 * @see java.io.InputStream#skip(long)
	 */
	public long skip(long arg0) throws IOException {
		return binaryInputStream.skip(arg0);
	}


}
