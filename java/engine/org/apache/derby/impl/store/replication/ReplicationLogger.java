/*
 
   Derby - Class
   org.apache.derby.impl.store.replication.ReplicationLogger
 
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

package org.apache.derby.impl.store.replication;

import java.util.Date;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.error.ErrorStringBuilder;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;

public class ReplicationLogger {

    /** Whether or not to print log messages to derby.log. Defaults to
     * true, but can be set to false with derby property
     * "derby.replication.verbose=false".
     */
    private final boolean verbose;

    /** The name of the replicated database */
    private final String dbname;

    public ReplicationLogger(String dbname) {
        verbose = PropertyUtil.getSystemBoolean(Property.REPLICATION_VERBOSE,
                                                true);
        this.dbname = dbname;
    }

    /**
     * Print error message and the stack trace of the throwable to the
     * log (usually derby.log) provided that verbose
     * is true. If verbose is false, nothing is
     * logged.
     *
     * @param msgId The error message id
     * @param t Error trace starts from this error
     */
    public void logError(String msgId, Throwable t) {

        if (verbose) {

            Monitor.logTextMessage(MessageId.REPLICATION_ERROR_BEGIN,
                                   new Date());

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

    /**
     * Print a text to the log (usually derby.log), provided that
     * verbose is true.
     * @param text The text that will be logged
     * @param writeHeader if true, encapsulates message in "begin
     * error message" and "end error message" lines. If false,
     * timestamps the text and writes it to the log without the header
     * and footer.
     */
    public void logText(String text, boolean writeHeader) {

        if (verbose) {
            if (writeHeader) {
                Monitor.logTextMessage(MessageId.REPLICATION_ERROR_BEGIN,
                                       new Date());
                Monitor.logMessage(text);
                Monitor.logTextMessage(MessageId.REPLICATION_ERROR_END);
            } else {
                Monitor.
                    logTextMessage(MessageId.REPLICATION_ONELINE_MSG_HEADER,
                                   new Date(), text);
            }
        }
    }

}
