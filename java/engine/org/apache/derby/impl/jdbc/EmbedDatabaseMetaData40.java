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
import java.sql.SQLException;
import org.apache.derby.impl.jdbc.Util;


public class EmbedDatabaseMetaData40 extends EmbedDatabaseMetaData {
    
    private final String url;
    
    public EmbedDatabaseMetaData40(EmbedConnection connection, String url) throws SQLException {
        super(connection,url);
        this.url = url;
    }
   
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw Util.notImplemented();
        
    }

    
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw Util.notImplemented();
    }
    
    
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw Util.notImplemented();
    }
     
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw Util.notImplemented();
    }
   
    public ResultSet getClientInfoProperties()
		throws SQLException {
        throw Util.notImplemented();
    }
   
    public boolean providesQueryObjectGenerator() throws SQLException {
        throw Util.notImplemented();
    }
}
