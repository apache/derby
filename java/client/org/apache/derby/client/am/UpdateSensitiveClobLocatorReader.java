/*
 
   Derby - Class org.apache.derby.client.am.UpdateSensitiveClobLocatorReader
 
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Wraps a Buffered Clob locator reader and watches out
 * for updates on the Clob associated with it.
 * Before a read operation is performed on the Reader
 * this stream verifies that the underlying Clob has not
 * changed and if it has it recreates the specific streams.
 */
public class UpdateSensitiveClobLocatorReader extends Reader {
    //The ClobLocatorReader instance
    //wrapped inside a BufferedReader
    private BufferedReader r = null;
    
    //The connection object from which
    //this Clob is obtained.
    private Connection con = null;
    
    //Stores the Clob instance
    //this class refers to.
    private Clob clob = null;
    
    //stores the current value of
    //the updateCount in the Clob.
    private long updateCount;
    
    
    //Current position in the underlying Clob.
    //lobs are indexed from 1
    private long currentPos;
    
    //Stores the length of the partial value
    //contained in this stream.
    private long length;
    
    /**
     * Initializes the Reader, updateCount, currentPos
     * and the connection to the appropriate values.
     *
     * @param con connection to be used to read the
     *        <code>Clob</code> value from the server
     * @param clob the <code>Clob</code> object associated with
     *            this stream.
     */
    protected UpdateSensitiveClobLocatorReader(Connection con, Clob clob) 
    throws SqlException {
        //check if the locator associated with the
        //underlying Clob is valid.
        clob.checkForLocatorValidity();
        
        //Wrap the ClobLocator instance inside a
        //Buffered reader.
        this.r = new BufferedReader(new ClobLocatorReader(con, clob));
        //Since the position has
        //not been mentioned it starts
        //with 1.
        this.currentPos = 1;
        this.con = con;
        this.clob = clob;
        //store the current update
        //count.
        updateCount = clob.getUpdateCount();
        //The length has not been mentioned
        //hence initialize it to -1.
        this.length = -1;
    }
    
    /**
     * Initializes the Reader, updateCount, currentPos,
     * length and the connection to the appropriate values.
     *
     * @param con connection to be used to read the
     *        <code>Clob</code> value from the server
     * @param clob the <code>Clob</code> object associated with
     *             this reader.
     * @param pos the position from which the first read begins.
     * @param len the length in bytes of the partial value to be
     *            retrieved.
     *
     */
    protected UpdateSensitiveClobLocatorReader(Connection con, Clob clob, 
            long pos, long len) throws SqlException {
        //check if the locator associated with the
        //underlying Clob is valid.
        clob.checkForLocatorValidity();
        
        this.r = new BufferedReader(new ClobLocatorReader(con, clob, pos, len));
        this.con = con;
        this.clob = clob;
        this.currentPos = pos;
        this.length = len;
        //store the current update
        //count.
        updateCount = clob.getUpdateCount();
    }
    
    /**
     * @see java.io.Reader#read()
     */
    public int read() throws IOException {
        //verify if the underlying Clob has
        //been modified and if yes recreate
        //the streams.
        identifyAndReplaceObseleteStream();
        int ret = r.read();
        if (ret == -1) {
            return ret;
        }
        currentPos++;
        return ret;
    }
    
    /**
     * @see java.io.Reader#read(char[], int, int)
     */
    public int read(char[] c, int off, int len) throws IOException {
        //verify if the underlying Clob has
        //been modified and if yes recreate
        //the streams.
        identifyAndReplaceObseleteStream();
        int ret = r.read(c, off, len);
        if (ret == -1) {
            return ret;
        }
        currentPos += ret;
        return ret;
    }
    
    /**
     * @see java.io.Reader#close()
     */
    public void close() throws IOException {
        r.close();
    }
    
    /**
     * Verifies whether the current updateCount matches
     * the updateCount of the Clob object and if it does
     * not it recreates the stream.
     *
     * @throws IOException If any exception occurs upon
     *                     Locator stream creation.
     */
    private void identifyAndReplaceObseleteStream() throws IOException {
        //Stores the current update count
        //value obtained from the Clob.
        long newUpdateCount;
        //Get the current update count.
        newUpdateCount = clob.getUpdateCount();
        //verify if the updateCount of the stream
        //and the current value present in the Clob
        //matches.
        if (updateCount != newUpdateCount) {
            //The values do not match.
            //This means that the data
            //in the Clob has changed and
            //hence the streams need
            //to be re-created.
            
            //re-create the stream
            try {
                //Wrap the re-created stream in a
                //BufferedReader before returning
                //it to he user.
                r = new BufferedReader(reCreateStream());
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
            //The underlying Clob value is
            //the same. Hence do nothing.
            return;
        }
    }
    
    /**
     * Re-creates the underlying Locator stream
     * with the current position and the length
     * values if specified.
     */
    protected Reader reCreateStream() throws SqlException {
        Reader r_ = null;
        //Verify if a subset of the underlying
        //Clob data was requested.
        if (length != -1) {
            //The length information is available.
            //It should be used while re-creating
            //the stream.
            r_ = new ClobLocatorReader(con, clob, currentPos, 
                    length - currentPos +1);
        }
        else {
            //The length information is not
            //available.
            r_ = new ClobLocatorReader(con, clob, currentPos, -1);
        }
        return r_;
    }
}
