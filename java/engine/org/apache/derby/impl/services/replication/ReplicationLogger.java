/*
 
   Derby - Class
   org.apache.derby.impl.services.replication.ReplicationLogger
 
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

package org.apache.derby.impl.services.replication;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.services.context.ErrorStringBuilder;
import org.apache.derby.iapi.services.monitor.Monitor;

public class ReplicationLogger {

    /** Whether or not to print log messages to derby.log. Defaults to
     * true, but can be set to false with derby property
     * "derby.replication.logerrormessages=true"
     */
    // TODO: make this configurable through the aforementioned
    // property
    private static final boolean LOG_REPLICATION_MESSAGES = true;


    /**
     * Print error message and the stack trace of the throwable to the
     * log (usually derby.log) provided that LOG_REPLICATION_MESSAGES
     * is true. If LOG_REPLICATION_MESSAGES is false, nothing is
     * logged.
     *
     * @param msgId The error message id
     * @param t Error trace starts from this error
     * @param dbname The name of the replicated database
     */
    public static void logError(String msgId, Throwable t, String dbname) {

        if (LOG_REPLICATION_MESSAGES) {

            Monitor.logTextMessage(MessageId.REPLICATION_ERROR_BEGIN);

            if (msgId != null) {
                Monitor.logTextMessage(msgId, dbname);
            }

            if (t != null) {
                ErrorStringBuilder esb =
                    new ErrorStringBuilder(Monitor.getStream().getHeader());
                esb.stackTrace(t);
                Monitor.logMessage(esb.get().toString());
                esb.reset();
            }
            Monitor.logTextMessage(MessageId.REPLICATION_ERROR_END);
        }
    }

}
