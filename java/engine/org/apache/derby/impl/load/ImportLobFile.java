/*

   Derby - Class org.apache.derby.impl.load.ImportLobFile

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
import java.io.File;
import java.io.RandomAccessFile;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.FileNotFoundException;
import org.apache.derby.iapi.services.io.LimitInputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;


/**
 * Helper class to read large object data at random locations 
 * from a file that contains large object data. 
 */

class ImportLobFile 
{
    private ImportFileInputStream lobInputStream = null;
    private LimitInputStream  lobLimitIn;
    private Reader lobReader = null;
    private String dataCodeset; 
    

    /**
     * Create a ImportLobFile object.
     * @param lobFile the file which has the LOB Data.
     * @param dataCodeset the code set to use char data in the file.
     */
    ImportLobFile(File lobFile, String dataCodeset) throws Exception {
        this.dataCodeset = dataCodeset;
        openLobFile(lobFile);
    }


    /**
     * Open the lob file and setup the stream required to read the data.
     * @param lobFile the file that contains lob data.
     * @exception Exception if an error occurs.     
     */
    private void openLobFile(final File lobFile) 
        throws Exception 
    {
        RandomAccessFile lobRaf;
        try {
            // open the lob file under a privelged block.
            try {
                lobRaf = AccessController.doPrivileged
                (new java.security.PrivilegedExceptionAction<RandomAccessFile>(){
                        public RandomAccessFile run() throws IOException{
                            return new RandomAccessFile(lobFile, "r");
                        }   
                    }
                 );    	
            } catch (PrivilegedActionException pae) {
                throw pae.getException();
            }
        } catch (FileNotFoundException ex) {
            throw PublicAPI.wrapStandardException(
                      StandardException.newException(
                      SQLState.LOB_DATA_FILE_NOT_FOUND, 
                      lobFile.getPath()));
        } 
        
        // Set up stream to read from input file, starting from 
        // any offset in the file. Users can specify columns in 
        // any order or skip some during import. So it is 
        // required for this stream to have the ability to read
        // from any offset in the file. 

        lobInputStream = new ImportFileInputStream(lobRaf);

        // wrap the lobInputStream with a LimitInputStream class,
        // This will help in making sure only the specific amout 
        // of data is read from the file, for example to read one 
        // column data from the file. 
        lobLimitIn = new  LimitInputStream(lobInputStream);
   
    }


    /**
     * Returns a stream that points to the lob data from file at the 
     * given <code>offset</code>.
     *
     * @param offset  byte offset of the column data in the file. 
     * @param length  length of the the data.
     * @exception  IOException  if any I/O error occurs.     
     */
    public InputStream getBinaryStream(long offset, long length) 
        throws IOException {
        lobInputStream.seek(offset);
        lobLimitIn.clearLimit();
        lobLimitIn.setLimit((int) length);
        return lobLimitIn;
    }


    /**
     * Returns the clob data at the given location as <code>String</code>. 
     * @param offset  byte offset of the column data in the file. 
     * @param length  length of the the data.
     * @exception  IOException  on any I/O error.     
     */
    public String getString(long offset, int length) throws IOException {
        lobInputStream.seek(offset);
        lobLimitIn.clearLimit();
        lobLimitIn.setLimit(length);
        
        // wrap a reader on top of the stream, so that calls 
        // to read the clob data from the file can read the 
        // with approapriate  data code set. 
        lobReader = dataCodeset == null ?
    		new InputStreamReader(lobLimitIn) : 
            new InputStreamReader(lobLimitIn, dataCodeset);    

        // read data from the file, and return it as string. 
        StringBuffer sb = new StringBuffer();
        char[] buf= new char[1024];
        int noChars = lobReader.read(buf , 0 , 1024);
        while (noChars != -1) {
            sb.append(buf , 0 , noChars);
            noChars = lobReader.read(buf , 0 , 1024);
        }
		return sb.toString();
    }


    /** 
     * Returns a stream that points to the clob data from file at the 
     * given <code>offset</code>.
     * @param offset  byte offset of the column data in the file. 
     * @param length  length of the the data in bytes.
     * @exception  IOException  on any I/O error.     
     */
    public java.io.Reader getCharacterStream(long offset, long length) 
        throws IOException 
    {
        lobInputStream.seek(offset);
        lobLimitIn.clearLimit();
        lobLimitIn.setLimit((int) length);

        // wrap a reader on top of the stream, so that calls 
        // to read the clob data from the file can read the 
        // with approapriate  data code set. 
        lobReader = dataCodeset == null ?
    		new InputStreamReader(lobLimitIn) : 
            new InputStreamReader(lobLimitIn, dataCodeset);    

        return lobReader;
    }

    /**
     * Returns the clob data length in characters at the give location. 
     * @param offset  byte offset of the column data in the file. 
     * @param length  length of the the data in bytes.
     * @exception  IOException  on any I/O error.     
     */
    public long getClobDataLength(long offset, long length) throws IOException {
        lobInputStream.seek(offset);
        lobLimitIn.clearLimit();
        lobLimitIn.setLimit((int) length);
        
        // wrap a reader on top of the stream, so that calls 
        // to read the clob data from the file can read the 
        // with approapriate  data code set. 
        lobReader = dataCodeset == null ?
            new InputStreamReader(lobLimitIn) : 
            new InputStreamReader(lobLimitIn, dataCodeset);   

        // find the length in characters 
        char[] buf= new char[1024];
        long lengthInChars = 0;
        int noChars = lobReader.read(buf , 0 , 1024);
        while (noChars != -1) {
            lengthInChars += noChars;
            noChars = lobReader.read(buf , 0 , 1024);
        }
        
        return lengthInChars;
    }

    /**
     * Close all the resources realated to the lob file.
     */
    public void close() throws IOException {

        if (lobReader != null) {
            lobReader.close();
            // above call also will close the 
            // stream under it. 
        } else {

            if (lobLimitIn != null) {
                lobLimitIn.close();
                // above close call , will also 
                // close the lobInputStream
            } else {
                if (lobInputStream != null)
                    lobInputStream.close();
            }
        }
    }
}


/**
 * An InputStream, which can stream data from a file, starting from 
 * any offset in the file. This stream operates on top of a 
 * RandomAccessFile object. This class overrides InputStream methods to 
 * read from the given RandomAccessFile and provides an addtional method
 * <code>seek(..)</code> to position the stream at offset in the file. 
 */
class ImportFileInputStream extends InputStream 
{

    private RandomAccessFile raf = null;
    private long currentPosition = 0 ;
    private long fileLength = 0;

    /**
     * Create a <code>ImportFileInputStream</code> object for 
     * the given file.  
     * @param raf  file the stream reads from. 
     * @exception  IOException  if any I/O error occurs.
     */
    ImportFileInputStream(RandomAccessFile raf)  
        throws IOException
    {
        this.raf = raf;
        this.fileLength = raf.length();
    }

    /**
     * Sets the file offset at which the next read will occur. 
     * @param offset byte offset in the file.
     * @exception  IOException  if an I/O error occurs.     
     */
    void seek(long offset) throws IOException {
        raf.seek(offset);
        currentPosition = offset;
    }


    /* Override the following InputStream-methods to
     * read data from the current postion of the file.
     */
    

    /**
     * Reads a byte of data from this input stream. 
     * @exception  IOException  if an I/O error occurs.
     */
    public int read() throws IOException {
        return raf.read();
    }

    /**
     * Reads up to <code>length</code> bytes of data from this input stream
     * into given array. This method blocks until some input is
     * available.
     *
     * @param      buf     the buffer into which the data is read.
     * @param      offset   the start offset of the data.
     * @param      length   the maximum number of bytes read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception  IOException  if an I/O error occurs.
     */
    public int read(byte buf[], int offset, int length) throws IOException {
        return raf.read(buf, offset, length);
    }


    /**
     * Returns the number of bytes that can be read from this stream.
     * @return     the number of bytes that can be read from this stream.
     * @exception  IOException  if an I/O error occurs.
     */
    public int available() throws IOException
    {
        return (int) (fileLength - currentPosition);
    }


    /**
     * Closes this input stream and releases any associated resources
     * @exception  IOException  if an I/O error occurs.
     */
    public void close() throws IOException {
        if (raf != null)
            raf.close();
    }   
}

