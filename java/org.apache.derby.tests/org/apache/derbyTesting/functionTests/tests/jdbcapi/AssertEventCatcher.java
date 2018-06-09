/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.AssertEventCatcher

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import javax.sql.*;

public class AssertEventCatcher implements ConnectionEventListener
{
    private final int catcher;
    //The following flags will indicate what kind of event was
    //received by this listener
    private boolean gotConnectionClosed = false;
    private boolean gotConnectionErrorOccured = false;

    public AssertEventCatcher(int which) {
        catcher=which;
    }

    // ConnectionEventListener methods
    public void connectionClosed(ConnectionEvent event)
    {
        gotConnectionClosed = true;
    }

    public void connectionErrorOccurred(ConnectionEvent event)
    {
        gotConnectionErrorOccured = true;
    }

    /**
     * Tell the caller if we received Connection closed event
     * @return true if received Connection closed event
     */
    public boolean didConnectionClosedEventHappen() 
    {
    	return gotConnectionClosed;
    }
    
    /**
     * Tell the caller if we received Connection error event
     * @return true if received Connection error event
     */
    public boolean didConnectionErrorEventHappen() 
    {
    	return gotConnectionErrorOccured;
    }
    
    /**
     * Clear the event received flags for this listener.
     */
    public void resetState() 
    {
    	gotConnectionClosed = false;
    	gotConnectionErrorOccured = false;
    }
}
