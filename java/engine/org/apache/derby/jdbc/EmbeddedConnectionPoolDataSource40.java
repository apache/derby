/*
 
   Derby - Class org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource40
 
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
package org.apache.derby.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import org.apache.derby.impl.jdbc.Util;

/** 
    This datasource is suitable for an application using embedded Derby,
    running on the following platforms:
    <p>
    <ul>
      <li>Java SE 7 (JDBC 4.1) and
      <li>full Java SE 8 (JDBC 4.2).
    </ul>
    <p>
    Use BasicEmbeddedConnectionPoolDataSource40 if your application
    runs on Java 8 Compact Profile 2.
    <p>
    Use EmbeddedConnectionPoolDataSource if your application
    runs on the following platforms:
    <p>
    <ul>
      <li> JDBC 3.0 - J2SE 5.0 </li>
      <li> JDBC 4.0 - Java SE 6 </li>
    </ul>
    <p>EmbeddedConnectionPoolDataSource40 is a ConnectionPoolDataSource
    implementation.

	<P>A ConnectionPoolDataSource is a factory for PooledConnection
	objects. An object that implements this interface will typically be
	registered with a JNDI service.

	<P>EmbeddedConnectionPoolDataSource40 is serializable and referenceable.

	<P>See EmbeddedDataSource40 for DataSource properties.

 */
public class EmbeddedConnectionPoolDataSource40 
    extends EmbeddedConnectionPoolDataSource
    implements javax.sql.ConnectionPoolDataSource // compile-time check for
                                                  // 4.1 extension
{
    // This explicit UID was missing for releases 10.10.1.1 and lower.
    // The default changed between 10.7 and 10.8,
    // so even with this explicit UID in place, serialized data sources
    // created with 10.7 or older would not be readable.
    // The default UID in 10.7 was -2155993232624542236.
    // This was not caught by the serialization tests at the time since
    // the "40"-suffixed data sources were not tested back then, cf DERBY-5955
    // http://svn.apache.org/viewvc?view=revision&revision=1438035 .
    private static final long serialVersionUID = -4368824293743156916L;
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  Logger getParentLogger()
        throws SQLFeatureNotSupportedException
    {
        throw (SQLFeatureNotSupportedException) Util.notImplemented( "getParentLogger()" );
    }

}
