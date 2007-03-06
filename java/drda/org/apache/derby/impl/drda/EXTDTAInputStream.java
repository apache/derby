/*
 
 Derby - Class org.apache.derby.impl.drda.DRDAStatement

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
package org.apache.derby.impl.drda;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.sql.ResultSet;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

import java.io.UnsupportedEncodingException;

import org.apache.derby.iapi.reference.DRDAConstants;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.impl.jdbc.Util;

/**
 * 
 * EXTDTAObjectHolder provides Externalized Large Object representation that
 * does not hold locks until the end of the transaction (DERBY-255)
 * 
 * It serves as a holder for lob data and is only valid as long as the original
 * result set from which it came is on the same row.  
 * 
 *  
 */
class EXTDTAInputStream extends InputStream {

    private InputStream binaryInputStream = null;

    private boolean isEmptyStream;
	
    private ResultSet dataResultSet = null;
    private Blob blob = null;
    private Clob clob = null;
	
	private EXTDTAInputStream(ResultSet rs,
				  int columnNumber,
				  int ndrdaType) 
	    throws SQLException, IOException
    {
	
	    this.dataResultSet = rs;
	    this.isEmptyStream = ! initInputStream(rs,
						   columnNumber,
						   ndrdaType);
		
	}

    
    
	/**
	 * Retrieve stream from the ResultSet and column specified.  Create an
	 * input stream for the large object being retrieved. Do not hold
	 * locks until end of transaction. DERBY-255.
	 * 
	 * 
	 * See DDMWriter.writeScalarStream
	 * 
	 * @param rs
	 *            result set from which to retrieve the lob
	 * @param column
	 *            column number
	 * @param drdaType
	 *            FD:OCA type of object one of
	 * 			   DRDAConstants.DRDA_TYPE_NLOBBYTES
	 * 			   DRDAConstants.DRDA_TYPE_LOBBYTES
	 * 			   DRDAConstants.DRDA_TYPE_NLOBCMIXED
	 *  		   DRDAConstants.DRDA_TYPE_LOBCMIXED
	 * 
	 * @return null if the value is null or a new EXTDTAInputStream corresponding to 
	 *  		rs.getBinaryStream(column) value and associated length
	 * 
	 * @throws SQLException
	 */
	public static EXTDTAInputStream getEXTDTAStream(ResultSet rs, int column, int drdaType) 
			throws SQLException {
 	    try{
		int ndrdaType = drdaType | 1; //nullable drdaType
			
		return new EXTDTAInputStream(rs,
					     column,
					     ndrdaType);
		
 	    }catch(IOException e){
 		throw new SQLException(e.getMessage());
		}
		
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
	    
	    try{
		if (binaryInputStream != null)
			binaryInputStream.close();	
		binaryInputStream = null;

	    }finally{
		
		blob = null;
		clob = null;
		dataResultSet = null;
	    }
	    
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


    protected boolean isEmptyStream(){
	return isEmptyStream;
    }
    
    
    /**
     * This method takes information of ResultSet and 
     * initialize binaryInputStream variable of this object with not empty stream and return true.
     * If the stream was empty, this method remain binaryInputStream null and return false.
     *
     * @param rs        ResultSet object to get stream from.
     * @param column    index number of column in ResultSet to get stream.
     * @param ndrdaType describe type column to get stream.
     *
     * @return          true if the stream was not empty, false if the stream was empty.
     *
     */
    private boolean initInputStream(ResultSet rs,
				    int column,
				    int ndrdaType)
	throws SQLException,
	       IOException
    {

	InputStream is = null;
	try{
	    // BLOBS
	    if (ndrdaType == DRDAConstants.DRDA_TYPE_NLOBBYTES) 
		{
		    blob = rs.getBlob(column);
		    if(blob == null){
			return false;
		    }
		    
		    is = blob.getBinaryStream();
		    
		}
	    // CLOBS
	    else if (ndrdaType ==  DRDAConstants.DRDA_TYPE_NLOBCMIXED)
		{	
		    try {
			clob = rs.getClob(column);
			
			if(clob == null){
			    return false;
			}

			is = new ReEncodedInputStream(clob.getCharacterStream());
			
		    }catch (java.io.UnsupportedEncodingException e) {
			throw new SQLException (e.getMessage());
			
		    }catch (IOException e){
			throw new SQLException (e.getMessage());
			
		    }
		    
		}
	    else
		{
		    if (SanityManager.DEBUG)
			{
			    SanityManager.THROWASSERT("NDRDAType: " + ndrdaType +
						      " not valid EXTDTA object type");
			}
		}
	    
	    boolean exist = is.read() > -1;
	    
	    is.close();
	    is = null;
	    
	    if(exist){
		openInputStreamAgain();
	    }

	    return exist;
	    
	}catch(IllegalStateException e){
	    throw Util.javaException(e);

	}finally{
	    if(is != null)
		is.close();
	    
	}
	
    }
    
    
    /**
     *
     * This method is called from initInputStream and 
     * opens inputstream again to stream actually.
     *
     */
    private void openInputStreamAgain() throws IllegalStateException,SQLException {
	
	if(this.binaryInputStream != null){
	    return;
	}
		
	InputStream is = null;
	try{
	    
	    if(SanityManager.DEBUG){
		SanityManager.ASSERT( ( blob != null && clob == null ) ||
				      ( clob != null && blob == null ),
				      "One of blob or clob must be non-null.");
	    }

	    if(blob != null){
		is = blob.getBinaryStream();
		
	    }else if(clob != null){
		is = new ReEncodedInputStream(clob.getCharacterStream());
	    }
	    
	}catch(IOException e){
	    throw new IllegalStateException(e.getMessage());
	}
	
	if(! is.markSupported() ){
	    is = new BufferedInputStream(is);
	}

	this.binaryInputStream = is;

    }
    
    
    protected void finalize() throws Throwable{
	close();
	}


}
