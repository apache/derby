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

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

import org.apache.derby.iapi.jdbc.EngineResultSet;
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
 
    
    /** ResultSet that contains the stream*/
    EngineResultSet rs;
    /** Column index starting with 1 */
    int columnNumber;
    /** DRDA Type of column */
    int ndrdaType;
      
	
	private EXTDTAInputStream(ResultSet rs,
				  int columnNumber,
				  int ndrdaType) 
    {
	
        this.rs = (EngineResultSet) rs;
        this.columnNumber = columnNumber;
        this.ndrdaType = ndrdaType;
    }

    
	/**
	 * Create a new EXTDTAInputStream.  Before read the stream must be 
     * initialized by the user with {@link #initInputStream()} 
	 * 
	 * @see DDMWriter#writeScalarStream
     * @see #initInputStream()
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
		{
 	    
		int ndrdaType = drdaType | 1; //nullable drdaType
			
		return new EXTDTAInputStream(rs,
					     column,
					     ndrdaType);
		
	}

	
	
	/**
	 * Requires {@link #initInputStream()} be called before we can read from the stream
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
		binaryInputStream = null;
	    
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
		if (arg0 < 0L) {
			return 0L;
		}
		return binaryInputStream.skip(arg0);
	}


    protected boolean isEmptyStream() throws SQLException{
            return (rs.getLength(columnNumber) == 0);
        
        }
    
    
    /**
     * This method takes information of ResultSet and 
     * initializes the binaryInputStream variable of this object with not empty stream 
     * by calling getBinaryStream or getCharacterStream() as appropriate.
     * The Reader returned from getCharacterStream() will be encoded in binarystream.
     *
     *
     */
    public  void initInputStream()
	throws SQLException
    {

	InputStream is = null;
	Reader r = null;
	// BLOBS
	if (ndrdaType == DRDAConstants.DRDA_TYPE_NLOBBYTES) 
	{ 	    	
	    is = this.rs.getBinaryStream(this.columnNumber);
	    if (is == null) 
              return;
	}
	    // CLOBS
	else if (ndrdaType ==  DRDAConstants.DRDA_TYPE_NLOBCMIXED)
	{	
	    try {
	        
	        r = this.rs.getCharacterStream(this.columnNumber);
		    	
	        if(r == null){	            
                    return;
	        }

			is = new ReEncodedInputStream(r);
			
		    }catch (java.io.UnsupportedEncodingException e) {
			throw Util.javaException(e);
			
		    }catch (IOException e){
			throw Util.javaException(e);
			
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
	if (! is.markSupported()) {
	    is = new BufferedInputStream(is);
	    }
	    
 	this.binaryInputStream=is;
    }
    
        
    protected void finalize() throws Throwable{
	close();
	}

    /**
     * Is the value null?  Null status is obtained from the underlying 
     * EngineResultSet, so that it can be determined before the stream
     * is retrieved.
     * 
     * @return true if this value is null
     * 
     */
    public boolean isNull() throws SQLException
    {
        return this.rs.isNull(columnNumber);
     
    }
}
