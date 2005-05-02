/*

   Derby - Class org.apache.derby.client.am.QueryTimerTask

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/
package org.apache.derby.client.am;

public class QueryTimerTask extends java.util.TimerTask {
    private Statement statement_;
    private java.util.Timer timer_;

    public QueryTimerTask(Statement statement, java.util.Timer timer) {
        statement_ = statement;
        timer_ = timer;
    }

    public void run() {
        timer_.cancel(); // method call on java.util.Timer to kill the timer thread that triggered this task thread
        try {
            statement_.cancel(); // jdbc cancel
        } catch (SqlException e) {
            SqlWarning warning = new SqlWarning(statement_.agent_.logWriter_,
                    "An exception occurred while trying to cancel a statement that has timed out." +
                    " See chained SQLException.");
            warning.setNextException(e);
            statement_.accumulateWarning(warning);
        }
        boolean notYetRun = this.cancel(); // method call on java.util.TimerTask to kill this task thread.
        if (notYetRun) {
            // The following is most likely just a bugcheck - but we'll see.
            // May be able to remove it later.
            SqlWarning warning = new SqlWarning(statement_.agent_.logWriter_,
                    "An unexpected error occurred while trying to cancel a statement that has timed out.");
            statement_.accumulateWarning(warning);
        }
    }
}
