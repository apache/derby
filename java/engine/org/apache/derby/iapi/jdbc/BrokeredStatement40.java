/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredStatement40

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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
package org.apache.derby.iapi.jdbc;

import java.sql.SQLException;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.reference.SQLState;


public class BrokeredStatement40 extends BrokeredStatement {
    
    /**
     * calls the superclass constructor to pass the parameters
     *
     * @param control   BrokeredStatementControl
     * @param jdbcLevel int
     * @throws java.sql.SQLException
     *
     */
    
    BrokeredStatement40(BrokeredStatementControl control, int jdbcLevel) 
                                                    throws SQLException {
        super(control, jdbcLevel);
    }
    
    /**
     * Checks if the statement is closed.
     *
     * @return <code>true</code> if the statement is closed,
     * <code>false</code> otherwise
     * @exception SQLException if an error occurs
     */
    public final boolean isClosed() throws SQLException {
        return getStatement().isClosed();
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
                            throws SQLException {
        checkIfClosed();
        //Derby does not implement non-standard methods on 
        //JDBC objects
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                    interfaces);
        }
    }

    /** 
     * Forwards to the real Statement.
     * @return true if the underlying Statement is poolable, false otherwise.
     * @throws SQLException if the forwarding call fails.
     */
    public boolean isPoolable() throws SQLException {
        return getStatement().isPoolable();
    }

    /** 
     * Forwards to the real Statement.
     * @param poolable the new value for the poolable hint.
     * @throws SQLException if the forwarding call fails.
     */
    public void setPoolable(boolean poolable) throws SQLException {
        getStatement().setPoolable(poolable);
    }
}
