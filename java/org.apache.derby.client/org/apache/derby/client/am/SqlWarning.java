/*

   Derby - Class org.apache.derby.client.am.SqlWarning

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

import java.sql.SQLWarning;

/**
 * This represents a warning versus a full exception.  As with
 * SqlException, this is an internal representation of java.sql.SQLWarning.
 *
 * Public JDBC methods need to convert an internal SqlWarning to a SQLWarning
 * using <code>getSQLWarning()</code>
 */
public class SqlWarning extends SqlException implements Diagnosable {

    private SqlWarning nextWarning_;
    
    SqlWarning(LogWriter logwriter, ClientMessageId msgid, Object... args)
    {
        super(logwriter, msgid, args);
    }
    
    public SqlWarning(LogWriter logWriter, Sqlca sqlca)
    {
        super(logWriter, sqlca);
    }
    
    void setNextWarning(SqlWarning warning)
    {
        // Add this warning to the end of the chain
        SqlWarning theEnd = this;
        while (theEnd.nextWarning_ != null) {
            theEnd = theEnd.nextWarning_;
        }
        theEnd.nextWarning_ = warning;
    }
    
    /**
     * Get the java.sql.SQLWarning for this SqlWarning
     */
    public SQLWarning getSQLWarning()
    {
        if (wrappedException_ != null) {
            return (SQLWarning) wrappedException_;
        }

        SQLWarning sqlw = new SQLWarning(getMessage(), getSQLState(), 
            getErrorCode());

        sqlw.initCause(this);

        // Set up the nextException chain
        if ( nextWarning_ != null )
        {
            // The warning chain gets constructed automatically through
            // the beautiful power of recursion
            sqlw.setNextWarning(nextWarning_.getSQLWarning());
        }
        
        return sqlw;
        
    }
}

