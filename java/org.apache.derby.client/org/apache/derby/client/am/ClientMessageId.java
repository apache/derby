/*
   Derby - Class org.apache.derby.client.am.MessageId
 
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

/**
 * A very simple wrapper around a message id.  This is needed so that
 * the new constructors for SqlException using message ids don't conflict
 * with the old constructors.  
 *
 * Once all messages have been internationalized, we could conceivably
 * get rid of this class.
 */
public class ClientMessageId
{
    public String msgid;
    
    /**
     * Creates a new instance of MessageId
     *
     * @param msgid The message id name
     */
    public ClientMessageId(String msgid)
    {
        this.msgid = msgid;
    }
    
}
