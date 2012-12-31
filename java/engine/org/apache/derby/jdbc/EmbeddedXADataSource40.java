/*
 
   Derby - Class org.apache.derby.jdbc.EmbeddedXADataSource40
 
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

    EmbeddedXADataSource40 is Derby's XADataSource implementation for
    JDBC 4.1 (and higher).

	<P>An XADataSource is a factory for XAConnection objects.  It represents a
	RM in a DTP environment.  An object that implements the XADataSource
	interface is typically registered with a JNDI service provider.   	
	<P>
    Use EmbeddedXADataSource40 if your application runs at JDBC level 4.1
    (or higher).
	Use EmbeddedXADataSource
	if your application runs in the following environments:
	<UL>
    <LI> JDBC 3.0 - J2SE 5.0 </LI>
    <LI> JDBC 4.0 - Java SE 6 </LI>
	</UL>

	<P>EmbeddedXADataSource40 object only works on a local database.  There is no
	client/server support.  An EmbeddedXADataSource40 object must live in the same jvm as
	the database. 

	<P>EmbeddedXADataSource40 is serializable and referenceable.

	<P>See EmbeddedDataSource40 for DataSource properties.

 */
public class EmbeddedXADataSource40 extends EmbeddedXADataSource
    implements javax.sql.XADataSource /* compile-time check for 4.1 extension */
{
   private static final long serialVersionUID = 4048303427908481258L;

    /** Creates a new instance of EmbeddedXADataSource40 */
    public EmbeddedXADataSource40() {
        super();
    }
    
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
