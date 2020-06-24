/*
 
   Derby - Class org.apache.derby.client.am.UpdateSensitiveLOBLocatorInputStream
 
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

package org.apache.derby.client.am;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * Super-class of the Update sensitive locator streams.
 * Before a read operation if performed on the stream
 * this stream verifies that the underlying LOB has not
 * changed and if it has it recreates the specific streams.
 * Since Locator streams are specific to Blob and Clob the
 * sub-classes would take care of creating the appropriate
 * streams.
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
abstract class UpdateSensitiveLOBLocatorInputStream extends InputStream {
    //The ClobLocatorInputStream or
    //BlobLocatorInputStream instance
    //wrapped inside a BufferedInputStream
    //this class will hold
    //Will be used while re-creating the stream
    //in the sub-class hence protected.
    private BufferedInputStream is = null;
    
    //The connection object from which
    //this LOB is obtained.
    //Will be used while re-creating the stream
    //in the sub-class hence protected.
    protected ClientConnection con = null;
    
    //Stores the Blob instance
    //this class refers to.
    private Lob lob = null;
    
    //stores the current value of
    //the updateCount in the Clob
    //or Blob.
    private long updateCount;
    
    
    //Current position in the underlying lob.
    //lobs are indexed from 1
    //Will be used while re-creating the stream
    //in the sub-class hence protected.
    protected long currentPos;
    
    //Stores the length of the partial value
    //contained in this stream.
    //Will be used while re-creating the stream
    //in the sub-class hence protected.
    protected long length;
    
    /**
     * Initializes the InputStream, updateCount, currentPos
     * and the connection to the appropriate values.
     *
     * @param con connection to be used to read the
     *        {@code Lob} value from the server
     * @param lob {@code Lob} object which could be a
     *            {@code Blob} or a {@code Clob}.
     * @param is an {@code InputStream} that contains the
     *           appropriate locator stream instance.
     */
    protected UpdateSensitiveLOBLocatorInputStream(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection con,
            Lob lob,
            InputStream is) throws SqlException {

        //check if the locator associated with the
        //underlying Lob is valid.
        lob.checkForLocatorValidity();
        
        this.is = new BufferedInputStream(is);
        //Since the position has
        //not been mentioned it starts
        //with 1.
        this.currentPos = 1;
        this.con = con;
        this.lob = lob;
        //store the current update
        //count.
        updateCount = lob.getUpdateCount();
        //The length has not been mentioned
        //hence initialize it to -1.
        this.length = -1;
    }
    
    /**
     * Initializes the InputStream, updateCount, currentPos,
     * length and the connection to the appropriate values.
     *
     * @param con connection to be used to read the
     *        {@code Lob} value from the server
     * @param lob {@code Lob} object which could be a
     *            {@code Blob} or a {@code Clob}.
     * @param is an {@code InputStream} that contains the
     *           appropriate locator stream instance.
     * @param pos the position from which the first read begins.
     * @param len the length in bytes of the partial value to be
     *            retrieved.
     *
     */
    protected UpdateSensitiveLOBLocatorInputStream(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientConnection con,
            Lob lob,
            InputStream is,
            long pos,
            long len) throws SqlException {

        this(con, lob, is);
        //Initialize with the mentioned
        //position and length values.
        this.currentPos = pos;
        this.length = len;
    }
    
    /**
     * @see java.io.InputStream#read()
     */
    public int read() throws IOException {
        //verify if the underlying LOB has
        //been modified and if yes recreate
        //the streams.
        identifyAndReplaceObseleteStream();
        int ret = is.read();
        if (ret == -1) {
            return ret;
        }
        currentPos++;
        return ret;
    }
    
    /**
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        //verify if the underlying LOB has
        //been modified and if yes recreate
        //the streams.
        identifyAndReplaceObseleteStream();
        int ret = is.read(b, off, len);
        if (ret == -1) {
            return ret;
        }
        currentPos += ret;
        return ret;
    }

    public void close() throws IOException 
    {
       if (is != null) {
            is.close();
        }
    }
    
    /**
     * Verifies whether the current updateCount matches
     * the updateCount of the LOB object and if it does
     * not it recreates the stream.
     *
     * @throws IOException If any exception occurs upon
     *                     Locator stream creation.
     */
    private void identifyAndReplaceObseleteStream() throws IOException {
        //Stores the current update count
        //value obtained from the LOB.
        long newUpdateCount;
        //Get the current update count.
        newUpdateCount = lob.getUpdateCount();
        //verify if the updateCount of the stream
        //and the current value present in the LOB
        //matches.
        if(updateCount != newUpdateCount) {
            //The values do not match.
            //This means that the data
            //in the LOB has changed and
            //hence the streams need
            //to be re-created.
            
            //re-create the stream
            try {
                is = new BufferedInputStream(reCreateStream());
            }
            catch(SqlException sqle) {
                IOException ioe = new IOException();
                ioe.initCause(sqle);
                throw ioe;
            }
            
            //update the current update count
            //with the new update count value.
            updateCount = newUpdateCount;
        } else {
            //The underlying LOB value is
            //the same. Hence do nothing.
        }
    }
    
    /**
     * Abstract method that will be implemented by
     * the underlying streams specific to Clob and
     * Blob.
     */
    protected abstract InputStream reCreateStream() throws SqlException;
}
