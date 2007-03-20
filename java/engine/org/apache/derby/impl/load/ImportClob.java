/*

   Derby - Class org.apache.derby.impl.load.ImportClob

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

package org.apache.derby.impl.load;
import org.apache.derby.iapi.services.io.LimitReader;
import java.sql.Clob;
import java.sql.SQLException;
import java.io.Reader;
import java.io.IOException;

/**
 * This class implements  <code > java.sql.CLOB interface </code>.  
 * Objects created using the <code> ImportClob </code> class  are 
 * intended to be be used to create a clob object of the data  stored 
 * in an import file.  Only the routines that are needed  to read the 
 * clob data for the clob columns by the  inserts done through the VTI  
 * have real implementations,  Other routines are dummy ones to satisfy
 * <code> java.sql.Clob </code>  interface.
 */

class ImportClob implements java.sql.Clob {

    private ImportLobFile lobFile;
	private long position; // postion in the import file in bytes.
    private long length;  // length in bytes
	private long clobLength; // length of clob in chars. 
    private String clobData = null;


    /**
     * Create a import Clob object, that reads <code> length </code> amount of 
     * data  from an external file, starting at <code> position </code>. 
     * @param lobFile  lob file resource object, using which data is read.
     * @param position  byte offset in the file, of this clob column data. 
     * @param length   length of this clob object data in bytes. 
     */
    public ImportClob(ImportLobFile lobFile, long position, long length) 
        throws IOException 
	{
		this.lobFile = lobFile;
		this.position = position;
        this.length = length;
		this.clobLength = lobFile.getClobDataLength(position, length);
	}


    /**
     * Create a Clob object, whose value is given as string.  
     * @param data  String that contains the clob data. 
     */
    public ImportClob(String data) 
    {
       clobData = data;
       clobLength = data.length();
    }


  /**
   * Returns the number of characters in this <code>CLOB</code>  object.
   * @return length of the <code>CLOB</code> in characters
   * @exception SQLException on any error.
   */
	public long length() throws SQLException {
		return clobLength;
	}


    /**
     * Returns  <code>CLOB</code> value designated by this
     * <code>Clob</code> object as a <code> Reader </code>.
     *
     * @return a Reader containing the <code>CLOB</code> data.
     * @exception SQLException if any error occurs while setting up 
     *                         this clob data in the import file as Reader. 
     * @see java.sql.Clob 
     */
    public java.io.Reader getCharacterStream() throws SQLException {
        try {
            Reader ir;
            if(clobData != null) {
                // the data is string is already in the user spefied code set.
                ir = new java.io.StringReader(clobData);
                // wrap the Reader with a LimitReader class,
                // so that only the data of the clob length is read.
                LimitReader lr = new  LimitReader(ir);
                lr.setLimit((int) clobLength);
                return lr;
            } else {
                return lobFile.getCharacterStream(position, length);
            }
		} catch (Exception e) {
			throw LoadError.unexpectedError(e);
		}
    }


        
    /** following rotines does not have implmentation because they
     * are not used by the VTI that is used to import the data. 
     */

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     *
     * @see java.sql.Clob 
     */
    public String getSubString(long pos, int length) throws SQLException {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented"));
    }


    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public java.io.InputStream getAsciiStream() throws SQLException {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented")); 
    }


    /** 
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public long position(String searchstr, long start) throws SQLException {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented")); 
    }

    /** 
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public long position(Clob searchstr, long start) throws SQLException {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented")); 
    }

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public int setString(long pos, String str) throws SQLException {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented"));  
    }

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public int setString(long pos, String str, int offset, int len) 
        throws SQLException
    {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented")); 
    }

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public java.io.OutputStream setAsciiStream(long pos) throws SQLException
    {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented")); 
    }

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public java.io.Writer setCharacterStream(long pos) throws SQLException
    {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented")); 
     }

    /**
     * This routine is not used by the VTI to read the data, so no 
     * implementation is provided, an exception is thrown if it is
     * called.  
     * @see java.sql.Clob 
     */
    public void truncate(long len) throws SQLException 
    {
        throw LoadError.unexpectedError(
                         new Exception("Method not implemented"));  
    }
}

