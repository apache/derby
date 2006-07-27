/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedDatabaseMetaData40

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.reference.SQLState;


public class EmbedDatabaseMetaData40 extends EmbedDatabaseMetaData {
    
    private final String url;
    
    public EmbedDatabaseMetaData40(EmbedConnection connection, String url) throws SQLException {
        super(connection,url);
        this.url = url;
    }

    /**
     * Retrieves the major JDBC version number for this driver.
     *
     * @return JDBC version major number
     */
    public int getJDBCMajorVersion() {
        return 4;
    }

    /**
     * Retrieves the minor JDBC version number for this driver.
     *
     * @return JDBC version minor number
     */
    public int getJDBCMinorVersion() {
        return 0;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }
     
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        // TODO - find out what this really should be 
        return false;
    }
   
    public boolean providesQueryObjectGenerator() throws SQLException {
        return false;
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
        //Derby does not implement non-standard methods on 
        //JDBC objects
        //hence return this if this class implements the interface 
        //or throw an SQLException
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw newSQLException(SQLState.UNABLE_TO_UNWRAP,interfaces);
        }
    }

}
