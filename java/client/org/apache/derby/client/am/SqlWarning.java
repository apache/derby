/*

   Derby - Class org.apache.derby.client.am.SqlWarning

   Copyright (c) 2006 The Apache Software Foundation or its licensors, 
   where applicable.

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

import java.sql.SQLWarning;
import org.apache.derby.iapi.services.info.JVMInfo;

/**
 * This represents a warning versus a full exception.  As with
 * SqlException, this is an internal representation of java.sql.SQLWarning.
 *
 * Public JDBC methods need to convert an internal SqlWarning to a SQLWarning
 * using <code>getSQLWarning()</code>
 */
public class SqlWarning extends SqlException implements Diagnosable {

    protected SqlWarning nextWarning_;
    
    public SqlWarning(LogWriter logwriter, 
        ClientMessageId msgid, Object[] args, Throwable cause)
    {
        super(logwriter, msgid, args, cause);
    }
    
    public SqlWarning(LogWriter logwriter, ClientMessageId msgid, Object[] args)
    {
        this(logwriter, msgid, args, null);
    }
    
    public SqlWarning (LogWriter logwriter, ClientMessageId msgid)
    {
        super(logwriter, msgid);
    }
    
    public SqlWarning(LogWriter logwriter, ClientMessageId msgid, Object arg1)
    {
        super(logwriter, msgid, arg1);
    }
    
    public SqlWarning(LogWriter logwriter,
        ClientMessageId msgid, Object arg1, Object arg2)
    {
        super(logwriter, msgid, arg1, arg2);
    }
    
    public SqlWarning(LogWriter logwriter,
        ClientMessageId msgid, Object arg1, Object arg2, Object arg3)
    {
        super(logwriter, msgid, arg1, arg2, arg3);
    }
    
    public SqlWarning(LogWriter logWriter, Sqlca sqlca)
    {
        super(logWriter, sqlca);
    }
    
    public void setNextWarning(SqlWarning warning)
    {
        // Add this warning to the end of the chain
        SqlWarning theEnd = this;
        while (theEnd.nextWarning_ != null) {
            theEnd = theEnd.nextWarning_;
        }
        theEnd.nextWarning_ = warning;
    }
    
    public SqlWarning getNextWarning()
    {
        return nextWarning_;
    }
    
    /**
     * Get the java.sql.SQLWarning for this SqlWarning
     */
    public SQLWarning getSQLWarning()
    {
        SQLWarning sqlw = new SQLWarning(getMessage(), getSQLState(), 
            getErrorCode());

        // If we're in a runtime that supports chained exceptions, set the cause 
        // of the SQLWarning to be this SqlWarning.
         if (JVMInfo.JDK_ID >= JVMInfo.J2SE_14 )
        {
            sqlw.initCause(this);
        }

        // Set up the nextException chain
        if ( nextWarning_ != null )
        {
            // The exception chain gets constructed automatically through 
            // the beautiful power of recursion
            //
            // We have to use the right method to convert the next exception
            // depending upon its type.  Luckily with all the other subclasses
            // of SQLException we don't have to make our own matching 
            // subclasses because 
            sqlw.setNextException(
                nextException_ instanceof SqlWarning ?
                    ((SqlWarning)nextException_).getSQLWarning() :
                    nextException_.getSQLException());
        }
        
        return sqlw;
        
    }
}

