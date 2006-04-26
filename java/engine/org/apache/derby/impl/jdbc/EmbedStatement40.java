/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedStatement40

   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.jdbc;

import java.sql.SQLException;
import org.apache.derby.iapi.reference.SQLState;

public class EmbedStatement40 extends EmbedStatement {
    
    /**
     * calls superclass contructor with the parameter passed
     *
     * @param connection           EmbedConnection object associated with this 
     *                             statement 
     * @param forMetaData          boolean
     * @param resultSetType        int
     * @param resultSetConcurrency int
     * @param resultSetHoldability int
     * 
     */
    public EmbedStatement40 (EmbedConnection connection, boolean forMetaData,
                             int resultSetType, int resultSetConcurrency, 
                             int resultSetHoldability) {
        super(connection,forMetaData,resultSetType,resultSetConcurrency,
                resultSetHoldability);
    }
    
    /**
     * Returns false unless <code>interfaces</code> is implemented 
     * 
     * @param  interfaces             a Class defining an interface.
     * @return true                   if this implements the interface or 
     *                                directly or indirectly wraps an object 
     *                                that does.
     * @throws java.sql.SQLException  if an error occurs while determining 
     *                                whether this is a wrapper for an object 
     *                                with the given interface.
     */
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        checkStatus();
        return interfaces.isInstance(this);
    }
    
    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLExption if no object if found that implements the 
     * interface
     */
    public <T> T unwrap(java.lang.Class<T> interfaces)
    throws SQLException{
        checkStatus();
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw newSQLException(SQLState.UNABLE_TO_UNWRAP,interfaces);
        }
    }
    
}
