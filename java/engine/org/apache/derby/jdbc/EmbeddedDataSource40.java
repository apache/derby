/*

   Derby - Class org.apache.derby.jdbc.EmbeddedDataSource40

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

import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.reference.SQLState;

/** 
	

	EmbeddedDataSource40 is Derby's DataSource implementation for JDBC4.0.
	

	<P>A DataSource  is a factory for Connection objects. An object that
	implements the DataSource interface will typically be registered with a
	JNDI service provider.
	<P>
	EmbeddedDataSource40 supports the JDBC 4.0 specification
	for the J2SE 6.0 Java Virtual Machine environment. Use EmbeddedDataSource
	if your application is running in one of the following older
	environments:
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
	</UL>	

	<P>The following is a list of properties that can be set on a Derby
	DataSource object:
	<P><B>Standard DataSource properties</B> (from JDBC 3.0 specification).

	<UL><LI><B><code>databaseName</code></B> (String): <I>Mandatory</I>
	<BR>This property must be set and it
	identifies which database to access.  If a database named wombat located at
	g:/db/wombat is to be accessed, then one should call
	<code>setDatabaseName("g:/db/wombat")</code> on the data source object.</LI>

	<LI><B><code>dataSourceName</code></B> (String): <I>Optional</I>
	<BR> Name for DataSource.  Not used by the data source object.  Used for
	informational purpose only.</LI>

	<LI><B><code>description</code></B> (String): <I>Optional</I>
	<BR>Description of the data source.  Not
	used by the data source object.  Used for informational purpose only.</LI> 

	<LI><B><code>password</code></B> (String): <I>Optional</I>
	<BR>Database password for the no argument <code>DataSource.getConnection()</code>,
	<code>ConnectionPoolDataSource.getPooledConnection()</code>
	and <code>XADataSource.getXAConnection()</code> methods.

	<LI><B><code>user</code></B> (String): <I>Optional</I>
	<BR>Database user for the no argument <code>DataSource.getConnection()</code>,
	<code>ConnectionPoolDataSource.getPooledConnection()</code>
	and <code>XADataSource.getXAConnection()</code> methods.
	</UL>

	<BR><B>Derby specific DataSource properties.</B>

  <UL>

  <LI><B><code>attributesAsPassword</code></B> (Boolean): <I>Optional</I>
	<BR>If true, treat the password value in a
	<code>DataSource.getConnection(String user, String password)</code>,
	<code>ConnectionPoolDataSource.getPooledConnection(String user, String password)</code>
	or <code>XADataSource.getXAConnection(String user, String password)</code> as a set
	of connection attributes. The format of the attributes is the same as the format
	of the attributes in the property connectionAttributes. If false the password value
	is treated normally as the password for the given user.
	Setting this property to true allows a connection request from an application to
	provide more authentication information that just a password, for example the request
	can include the user's password and an encrypted database's boot password.</LI>

  <LI><B><code>connectionAttributes</code></B> (String): <I>Optional</I>
  <BR>Defines a set of Derby connection attributes for use in all connection requests.
  The format of the String matches the format of the connection attributes in a Derby JDBC URL.
  That is a list of attributes in the form <code><I>attribute</I>=<I>value</I></code>, each separated by semi-colon (';').
  E.g. <code>setConnectionAttributes("bootPassword=erd3234dggd3kazkj3000");</code>.
  <BR>The database name must be set by the DataSource property <code>databaseName</code> and not by setting the <code>databaseName</code>
  connection attribute in the <code>connectionAttributes</code> property.
	<BR>
   Any attributes that can be set using a property of this DataSource implementation
   (e.g user, password) should not be set in connectionAttributes. Conflicting
   settings in connectionAttributes and properties of the DataSource will lead to
   unexpected behaviour. 
  <BR>Please see the Derby documentation for a complete list of connection attributes. </LI>

  <LI><B><code>createDatabase</code></B> (String): <I>Optional</I>
	<BR>If set to the string "create", this will
	cause a new database of <code>databaseName</code> if that database does not already
	exist.  The database is created when a connection object is obtained from
	the data source. </LI> 

	<LI><B><code>shutdownDatabase</code></B> (String): <I>Optional</I>
	<BR>If set to the string "shutdown",
	this will cause the database to shutdown when a java.sql.Connection object
	is obtained from the data source.  E.g., If the data source is an
	XADataSource, a getXAConnection().getConnection() is necessary to cause the
	database to shutdown.

	</UL>

	<P><B>Examples.</B>

	<P>This is an example of setting a property directly using Derby's
	EmbeddedDataSource40 object.  This code is typically written by a system integrator :
	<PRE> 
	*
	* import org.apache.derby.jdbc.*;
	*
	* // dbname is the database name
	* // if create is true, create the database if necessary
	* javax.sql.DataSource makeDataSource (String dbname, boolean create)
	*	throws Throwable 
	* { 
	*	EmbeddedDataSource40 ds = new EmbeddedDataSource40(); 
	*	ds.setDatabaseName(dbname);
	*
	*	if (create)
	*		ds.setCreateDatabase("create");
    *   
	*	return ds;
	* }
	</PRE>

	<P>Example of setting properties thru reflection.  This code is typically
	generated by tools or written by a system integrator: <PRE>
	*	
	* javax.sql.DataSource makeDataSource(String dbname) 
	*	throws Throwable 
	* {
	*	Class[] parameter = new Class[1];
	*	parameter[0] = dbname.getClass();
	*	DataSource ds =  new EmbeddedDataSource40();
	*	Class cl = ds.getClass();
	*
	*	Method setName = cl.getMethod("setDatabaseName", parameter);
	*	Object[] arg = new Object[1];
	*	arg[0] = dbname;
	*	setName.invoke(ds, arg);
	*
	*	return ds;
	* }
	</PRE>

	<P>Example on how to register a data source object with a JNDI naming
	service.
	<PRE>
	* DataSource ds = makeDataSource("mydb");
	* Context ctx = new InitialContext();
	* ctx.bind("jdbc/MyDB", ds);
	</PRE>

	<P>Example on how to retrieve a data source object from a JNDI naming
	service. 
	<PRE>
	* Context ctx = new InitialContext();
	* DataSource ds = (DataSource)ctx.lookup("jdbc/MyDB");
	</PRE>

*/
public class EmbeddedDataSource40 extends EmbeddedDataSource {
    
    public EmbeddedDataSource40() {
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
}
