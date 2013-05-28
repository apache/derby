/*

   Derby - Class org.apache.derby.jdbc.EmbeddedDataSource

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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

/**
   <P>
    This datasource is suitable for an application using embedded Derby,
    running on full Java SE 6 and higher, corresponding to 4.0 and higher.
    </P>

	<P>A DataSource  is a factory for Connection objects. An object that
	implements the DataSource interface will typically be registered with a
	JNDI service provider.</P>
    
	<P>
	EmbeddedDataSource automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.</P>
    
	<UL>
    <LI>JDBC 4.0 - Java SE 6</LI>
    <LI>JDBC 4.1 - Java SE 7</LI>
    <LI>JDBC 4.2 - full Java SE 8</LI>
	</UL>

    <P>
    Use BasicEmbeddedDataSource40 if your application runs on Java 8
    Compact Profile 2.
    </P>

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
	EmbeddedDataSource object.  This code is typically written by a system integrator :
	<PRE> 
	*
	* import org.apache.derby.jdbc.*;
	*
	* // dbname is the database name
	* // if create is true, create the database if necessary
	* javax.sql.DataSource makeDataSource (String dbname, boolean create)
	*	throws Throwable 
	* { 
	*	EmbeddedDataSource ds = new EmbeddedDataSource(); 
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
	*	DataSource ds =  new EmbeddedDataSource();
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
public class EmbeddedDataSource extends ReferenceableDataSource
                                implements Referenceable
{

	private static final long serialVersionUID = -4945135214995641181L;

	/**
		No-arg constructor.
	 */
	public EmbeddedDataSource() {
		// needed by Object Factory

		// don't put anything in here or in any of the set method because this
		// object may be materialized in a remote machine and then sent thru
		// the net to the machine where it will be used.
	}





    /**
     * {@code javax.naming.Referenceable} interface
     */

    /**
     * This method creates a new {@code Reference} object to represent this
     * data source.  The class name of the data source object is saved
     * in the {@code Reference}, so that an object factory will know that it
     * should create an instance of that class when a lookup operation
     * is performed. The class is also stored in the reference.  This
     * is not required by JNDI, but is recommend in practice.  JNDI
     * will always use the object factory class specified in the
     * reference when reconstructing an object, if a class name has
     * been specified.  See the JNDI SPI documentation for further
     * details on this topic, and for a complete description of the
     * {@code Reference} and {@code StringRefAddr} classes.
     * <p/>
     * Derby data source classes class provides several standard JDBC
     * properties.  The names and values of the data source properties
     * are also stored in the reference using the {@code StringRefAddr} class.
     * This is all the information needed to reconstruct an embedded
     * data source object.
     *
     * @return the created reference object for this data source
     * @exception NamingException cannot find named object
     */
    public final Reference getReference() throws NamingException
	{
        // These fields will be set by the JNDI server when it decides to
        // materialize a data source.
        Reference ref = new Reference(
            this.getClass().getName(),
            "org.apache.derby.jdbc.ReferenceableDataSource",
            null);

        addBeanProperties(this, ref);

        return ref;
	}

    /**
     * Add Java Bean properties to the reference using
     * StringRefAddr for each property. List of bean properties
     * is defined from the public getXXX() methods on this object
     * that take no arguments and return short, int, boolean or String.
     * The StringRefAddr has a key of the Java bean property name,
     * converted from the method name. E.g. traceDirectory for
     * traceDirectory.
     *
     */
    private static void addBeanProperties(Object ths, Reference ref)
    {
        // Look for all the getXXX methods in the class that take no arguments.
        Method[] methods = ths.getClass().getMethods();

        for (int i = 0; i < methods.length; i++) {

            Method m = methods[i];

            // only look for simple getter methods.
            if (m.getParameterTypes().length != 0)
                continue;

            // only non-static methods
            if (Modifier.isStatic(m.getModifiers()))
                continue;

            // Only getXXX methods
            String methodName = m.getName();
            if ((methodName.length() < 5) || !methodName.startsWith("get"))
                continue;

            Class returnType = m.getReturnType();

            if (Integer.TYPE.equals(returnType)
                    || Short.TYPE.equals(returnType)
                    || String.class.equals(returnType)
                    || Boolean.TYPE.equals(returnType)) {

                // setSomeProperty
                // 01234

                String propertyName = methodName.substring(3, 4).toLowerCase(
                        java.util.Locale.ENGLISH).concat(
                        methodName.substring(4));

                try {
                    Object ov = m.invoke(ths, null);
                    // Need to check if property value is null, otherwise
                    // "null" string gets stored.
                    if (ov != null) {
                        ref.add(new StringRefAddr(propertyName, ov.toString()));
                    }
                } catch (IllegalAccessException iae) {
                } catch (InvocationTargetException ite) {
                }

            }
        }
    }
}
