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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.derby.iapi.jdbc.EngineResultSet;
import org.apache.derby.shared.common.reference.DRDAConstants;
import org.apache.derby.shared.common.sanity.SanityManager;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-467

    private InputStream binaryInputStream = null;
 
    /** DRDA Type of column/parameter */
    int ndrdaType;

    //
    // Used when this class wraps a ResultSet
    //
    
    /** ResultSet that contains the stream*/
//IC see: https://issues.apache.org/jira/browse/DERBY-2941
    EngineResultSet rs;
    /** Column index starting with 1 */
    int columnNumber;

    //
    // Used when this class wraps a CallableStatement
    //
    private Clob _clob;
    private Blob _blob;
    
      
    
    private EXTDTAInputStream(ResultSet rs,
                  int columnNumber,
                  int ndrdaType) 
    {
    
        this.rs = (EngineResultSet) rs;
        this.columnNumber = columnNumber;
        this.ndrdaType = ndrdaType;
    }

    private EXTDTAInputStream(Clob clob, int ndrdaType ) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
        _clob = clob;
        this.ndrdaType = ndrdaType;
    }

    private EXTDTAInputStream(Blob blob, int ndrdaType ) 
    {
        _blob = blob;
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
     *             DRDAConstants.DRDA_TYPE_NLOBBYTES
     *             DRDAConstants.DRDA_TYPE_LOBBYTES
     *             DRDAConstants.DRDA_TYPE_NLOBCMIXED
     *             DRDAConstants.DRDA_TYPE_LOBCMIXED
     * 
     * @return null if the value is null or a new EXTDTAInputStream corresponding to 
     *          rs.getBinaryStream(column) value and associated length
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
     * Create a new EXTDTAInputStream from a CallableStatement.
     * 
     * 
     * @param cs
     *            CallableStatement from which to retrieve the lob
     * @param column
     *            column number
     * @param drdaType
     *            FD:OCA type of object one of
     *             DRDAConstants.DRDA_TYPE_NLOBBYTES
     *             DRDAConstants.DRDA_TYPE_LOBBYTES
     *             DRDAConstants.DRDA_TYPE_NLOBCMIXED
     *             DRDAConstants.DRDA_TYPE_LOBCMIXED
     */
    public static EXTDTAInputStream getEXTDTAStream(CallableStatement cs, int column, int drdaType)
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
        throws SQLException
    {

        int ndrdaType = drdaType | 1; //nullable drdaType

        switch ( ndrdaType )
        {
        case DRDAConstants.DRDA_TYPE_NLOBBYTES:
            Blob    blob = cs.getBlob( column );
            if ( blob ==  null ) { return null; }
            return new EXTDTAInputStream( blob, ndrdaType );
        case DRDAConstants.DRDA_TYPE_NLOBCMIXED:
            Clob    clob = cs.getClob( column );
            if ( clob == null ) { return null; }
            return new EXTDTAInputStream( clob, ndrdaType );
        default:
            badDRDAType( ndrdaType );
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            return null;
        }
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        return binaryInputStream.available();
    }

    /**
     * 
     * 
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException {
        
        if (binaryInputStream != null)
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            binaryInputStream.close();
        binaryInputStream = null;
        
    }

    /**
     * 
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object arg0) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        return binaryInputStream.equals(arg0);
    }

    /**
     * 
     * 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        return binaryInputStream.hashCode();
    }

    /**
     * 
     * 
     * @see java.io.InputStream#mark(int)
     */
    public void mark(int arg0) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        binaryInputStream.mark(arg0);
    }

    /**
     * 
     * 
     * @see java.io.InputStream#markSupported()
     */
    public boolean markSupported() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        return binaryInputStream.markSupported();
    }

    /**
     * 
     * 
     * @see java.io.InputStream#read(byte[])
     */
    public int read(byte[] arg0) throws IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        return binaryInputStream.read(arg0);
    }

    /**
     * 
     * 
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] arg0, int arg1, int arg2) throws IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        return binaryInputStream.read(arg0, arg1, arg2);
    }

    /**
     * 
     * 
     * @see java.io.InputStream#reset()
     */
    public void reset() throws IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        binaryInputStream.reset();
    }

    /**
     * 
     * 
     * @see java.io.InputStream#skip(long)
     */
    public long skip(long arg0) throws IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2686
        if (arg0 < 0L) {
            return 0L;
        }
        return binaryInputStream.skip(arg0);
    }


    protected boolean isEmptyStream() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
        return (length() == 0);
    }
    private long length() throws SQLException
    {
        if ( rs != null ) { return rs.getLength(columnNumber); }
        else if ( _clob != null ) { return _clob.length(); }
        else { return _blob.length(); }
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    throws SQLException
    {

    InputStream is = null;
    Reader r = null;
    // BLOBS
//IC see: https://issues.apache.org/jira/browse/DERBY-499
    if (ndrdaType == DRDAConstants.DRDA_TYPE_NLOBBYTES) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
        is = getBinaryStream();
        if (is == null) { return; }
    }
        // CLOBS
//IC see: https://issues.apache.org/jira/browse/DERBY-499
    else if (ndrdaType ==  DRDAConstants.DRDA_TYPE_NLOBCMIXED)
    {
        try {
            
            r = getCharacterStream();
                
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
        else { badDRDAType( ndrdaType ); }
//IC see: https://issues.apache.org/jira/browse/DERBY-2941
    if (! is.markSupported()) {
        is = new BufferedInputStream(is);
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    this.binaryInputStream=is;
    }
    private InputStream getBinaryStream() throws SQLException
    {
        if ( rs != null ) { return rs.getBinaryStream(this.columnNumber); }
        else { return _blob.getBinaryStream(); }
    }
    private Reader getCharacterStream() throws SQLException
    {
        if ( rs != null ) { return rs.getCharacterStream(this.columnNumber); }
        else { return _clob.getCharacterStream(); }
    }
    private static void badDRDAType( int drdaType )
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.THROWASSERT("NDRDAType: " + drdaType +
                                      " not valid EXTDTA object type");
        }
    }
    
        
    //
    // This method in java.lang.Object was deprecated as of build 167
    // of JDK 9. See DERBY-6932.
    //
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable{
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    close();
    }

    /**
     * Is the value null?  Null status is obtained from the underlying 
     * EngineResultSet or LOB, so that it can be determined before the stream
     * is retrieved.
     * 
     * @return true if this value is null
     * 
     */
    public boolean isNull() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
        if ( rs != null ) { return rs.isNull(columnNumber); }
        else { return (_clob == null) && (_blob == null); }
    }
}
