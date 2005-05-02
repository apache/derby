/*

   Derby - Class org.apache.derby.client.am.BatchUpdateException

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

import org.apache.derby.client.resources.ResourceKeys;


public class BatchUpdateException extends java.sql.BatchUpdateException {

    //-----------------constructors-----------------------------------------------

    public BatchUpdateException(LogWriter logWriter, ErrorKey errorKey, int[] updateCounts) {
        super(ResourceUtilities.getResource(ResourceKeys.driverOriginationIndicator) +
                ResourceUtilities.getResource(errorKey.getResourceKey()),
                errorKey.getSQLState(),
                errorKey.getErrorCode(),
                updateCounts);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public BatchUpdateException(LogWriter logWriter, ErrorKey errorKey, Object[] args, int[] updateCounts) {
        super(ResourceUtilities.getResource(ResourceKeys.driverOriginationIndicator) +
                ResourceUtilities.getResource(errorKey.getResourceKey(), args),
                errorKey.getSQLState(),
                errorKey.getErrorCode(),
                updateCounts);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    public BatchUpdateException(LogWriter logWriter, ErrorKey errorKey, Object arg, int[] updateCounts) {
        this(logWriter, errorKey, new Object[]{arg}, updateCounts);
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter) {
        super(null, null, -99999, null);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, int[] updateCounts) {
        super(null, null, -99999, updateCounts);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, String reason, int[] updateCounts) {
        super(reason, null, -99999, updateCounts);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, String reason, String sqlState, int[] updateCounts) {
        super(reason, sqlState, -99999, updateCounts);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }

    // Temporary constructor until all error keys are defined.
    public BatchUpdateException(LogWriter logWriter, String reason, String sqlState, int errorCode, int[] updateCounts) {
        super(reason, sqlState, errorCode, updateCounts);
        if (logWriter != null) {
            logWriter.traceDiagnosable(this);
        }
    }
}

