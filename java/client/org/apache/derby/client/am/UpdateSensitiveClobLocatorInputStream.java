/*
 
   Derby - Class org.apache.derby.client.am.UpdateSensitiveClobLocatorInputStream
 
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
import java.io.InputStream;

/**
 * This class extends from the UpdateSensitiveLOBLocatorInputStream
 * and creates and returns an implementation of the Clob specific
 * locator InputStream. It also over-rides the reCreateStream method
 * which re-creates the underlying Clob locator stream whenever a
 * update happens on the Clob object associated with this stream.
 */
public class UpdateSensitiveClobLocatorInputStream 
        extends UpdateSensitiveLOBLocatorInputStream {
    //Stores the Clob instance associated with
    //this InputStream.
    private Clob clob = null;
    
    /**
     * Creates an instance of the ClobLocatorInputStream
     * wrapped in a BufferedInputStream and and calls the 
     * super class constructor with appropriate initializers.
     *
     * @param con connection to be used to read the
     *        <code>Clob</code> value from the server
     * @param clob <code>Clob</code> object that contains locator for
     *        the <code>Clob</code> value on the server.
     *
     * @throws SqlException If any exception occurs during stream
     *                      creation.
     */
    public UpdateSensitiveClobLocatorInputStream(Connection con, Clob clob)
    throws SqlException {
        super(con, clob, new BufferedInputStream
                (new ClobLocatorInputStream(con, clob)));
        this.clob = clob;
    }
    
    /**
     * Re-creates the underlying Locator stream
     * with the current position and the length
     * values if specified.
     */
    protected InputStream reCreateStream() throws SqlException {
        return new ClobLocatorInputStream(con, clob, currentPos);
    }
}
