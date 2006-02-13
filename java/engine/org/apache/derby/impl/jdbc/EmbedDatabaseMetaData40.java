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


public class EmbedDatabaseMetaData40 extends EmbedDatabaseMetaData {
    
    private final String url;
    
    public EmbedDatabaseMetaData40(EmbedConnection connection, String url) throws SQLException {
        super(connection,url);
        this.url = url;
    }
   
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		PreparedStatement s = getPreparedQuery("getSchemasWithParams");
		s.setString(1, swapNull(catalog));
		s.setString(2, swapNull(schemaPattern));
		return s.executeQuery();
    }
    
    
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }
     
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        // TODO - find out what this really should be 
        return false;
    }
   
    public ResultSet getClientInfoProperties() throws SQLException {
        return getSimpleQuery("getClientInfoProperties");
    }
   
    public boolean providesQueryObjectGenerator() throws SQLException {
        return false;
    }
    
    public ResultSet getFunctions(java.lang.String catalog,
                       java.lang.String schemaPattern,
                       java.lang.String functionNamePattern)
                       throws SQLException
    {
        return getSimpleQuery("getFunctions");
    }
    
    public ResultSet getFunctionParameters(java.lang.String catalog,
                                java.lang.String schemaPattern,
                                java.lang.String functionNamePattern,
                                java.lang.String parameterNamePattern)
                                throws SQLException
    {
        return getSimpleQuery("getFunctionParameters");
    }

}
