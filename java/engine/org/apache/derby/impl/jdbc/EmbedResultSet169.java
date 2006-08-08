/*

Derby - Class org.apache.derby.impl.jdbc.EmbedResultSet169

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSet implementation for JSR169.
 * Adds no functionality to its (abstract) parent class.
 * If Derby could be compiled against JSR169 that the parent
 * class could be the concrete class for the environment.
 * Just like for the JDBC 2.0 specific classes.
 * Until that is possible (ie. easily downloadable J2ME/CDC/Foundation/JSR169
 * jar files, this class is required and is only compiled by an optional target.
 <P><B>Supports</B>
 <UL>
 <LI> JSR 169
 </UL>

 */

public final class EmbedResultSet169 extends EmbedResultSet
{
    public EmbedResultSet169(EmbedConnection conn, 
            ResultSet resultsToWrap,  
            boolean forMetaData,
            EmbedStatement stmt,
            boolean isAtomic)  
	throws SQLException
	{
    	super(conn, resultsToWrap, forMetaData, stmt, isAtomic);
	}
}