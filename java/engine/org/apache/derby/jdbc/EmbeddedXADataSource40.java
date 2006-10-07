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

import org.apache.derby.iapi.jdbc.ResourceAdapter;

import java.sql.SQLException;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.reference.SQLState;

/**

	EmbeddedXADataSource40 is Derby's XADataSource implementation for JDBC4.0.
	

	<P>An XADataSource is a factory for XAConnection objects.  It represents a
	RM in a DTP environment.  An object that implements the XADataSource
	interface is typically registered with a JNDI service provider.   	
	<P>
	EmbeddedXADataSource40 supports the JDBC 4.0 specification
	for the J2SE 6.0 Java Virtual Machine environment. Use EmbeddedXADataSource
	if your application runs in the following environments:
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
	<LI> JDBC 2.0 - Java 2 - JDK 1.2,1.3
	</UL>

	<P>EmbeddedXADataSource40 object only works on a local database.  There is no
	client/server support.  An EmbeddedXADataSource40 object must live in the same jvm as
	the database. 

	<P>EmbeddedXADataSource40 is serializable and referenceable.

	<P>See EmbeddedDataSource40 for DataSource properties.

 */
public class EmbeddedXADataSource40 extends EmbeddedXADataSource {
    /** Creates a new instance of EmbeddedXADataSource40 */
    public EmbeddedXADataSource40() {
        super();
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
            throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                    interfaces);
        }
    }
	
    /**
     * Intantiate and returns EmbedXAConnection.
     * @param user 
     * @param password 
     * @return XAConnection
     */
        protected XAConnection createXAConnection (ResourceAdapter ra, 
                String user, String password,
                boolean requestPassword)  throws SQLException {
            return new EmbedXAConnection40 (this, ra, user, 
                    password, requestPassword);
        }
}
