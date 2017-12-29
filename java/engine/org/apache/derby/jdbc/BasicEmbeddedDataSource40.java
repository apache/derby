/*

   Derby - Class org.apache.derby.jdbc.BasicEmbeddedDataSource40

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

import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.iapi.jdbc.ResourceAdapter;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.impl.jdbc.Util;

/**
 *
 * This data source is suitable for an application using embedded Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicEmbeddedDataSource40 is similar to EmbeddedDataSource, but does
 * not support JNDI naming, i.e. it does not implement
 * {@code javax.naming.Referenceable}.
 * <p/>
 * The standard attributes provided are, cf. e.g. table
 * 9.1 in the JDBC 4.2 specification.
 * <ul>
 *   <li>databaseName</li>
 *   <li>dataSourceName</li>
 *   <li>description</li>
 *   <li>password</li>
 *   <li>user</li>
 * </ul>
 * These standard attributes are not supported:
 * <ul>
 *   <li>networkProtocol</li>
 *   <li>portNumber</li>
 *   <li>roleName</li>
 *   <li>serverName</li>
 * </ul>
 * The embedded Derby driver also supports these attributes:
 * <ul>
 *   <li>loginTimeout</li> @see javax.sql.CommonDataSource set/get
 *   <li>logWriter</li> @see javax.sql.CommonDataSource set/get
 *   <li>createDatabase</li>
 *   <li>connectionAttributes</li>
 *   <li>shutdownDatabase</li>
 *   <li>attributesAsPassword</li>
 * </ul>
 * <br>
 * See the specific Derby DataSource implementation for details on their
 * meaning.
 * <p/>
 * See also the JDBC specifications for more details.
 *
 * @see EmbeddedDataSource
 */
public class BasicEmbeddedDataSource40 
    implements javax.sql.DataSource, java.io.Serializable, 
               EmbeddedDataSourceInterface
{

    private static final long serialVersionUID = -4945135214995641182L;

    /**
     * Set by {@link #setDescription(java.lang.String)}.
     * @serial
     */
    protected String description;

    /**
     * Set by {@link #setDataSourceName(java.lang.String)}.
     * @serial
     */
    protected String dataSourceName;

    /**
     * Set by {@link #setDatabaseName(java.lang.String)}.
     * @serial
     */
    protected String databaseName;

    /**
     * Derby specific connection attributes. Set by
     * {@link #setConnectionAttributes(java.lang.String)}.
     * @serial
     */
    protected String connectionAttributes;

    /**
     * Set to "create" if the database should be created.
     * See {@link #setCreateDatabase(java.lang.String)}.
     * @serial
     */
    protected String createDatabase;

    /**
     * Set to "shutdown" if the database should be shutdown. See {@link
     * #setShutdownDatabase(java.lang.String)}.
     * @serial
     */
    protected String shutdownDatabase;

    /**
     * Set password to be a set of connection attributes.
     * @serial
     */
    protected boolean attributesAsPassword;

    /**
     * {@code shortDatabaseName} has attributes of {@code databaseName}
     * stripped off. See {@link #databaseName}.
     * @serial
     */
    private String shortDatabaseName;

    /**
     * Set by {@link #setPassword(java.lang.String)}.
     * @serial
     */
    private String password;

    /**
     * Set by {@link #setUser(java.lang.String)}.
     * @serial
     */
    private String user;

    /**
     * Set by {@link #setLoginTimeout(int)}.
     * @serial
     */
    protected int loginTimeout;

    /**
     * Instance variable that will not be serialized.
     */
    transient private PrintWriter printer;

    /**
     * Instance variable that will not be serialized.
     */
    transient protected String jdbcurl;

    /**
     * Unlike a DataSource, the internal driver is shared by all
     * Derby databases in the same JVM.
     */
    transient protected InternalDriver driver;

    /**
     * Constructs a basic embedded data source. See the class Javadoc.
     */
    public BasicEmbeddedDataSource40() {
        update();
    }

   /*
     * Properties to be seen by Bean - access through reflection.
     */

    /**
     * Set the database name.  Setting this property is mandatory.  If a
     * database named wombat at g:/db needs to be accessed, database name
     * should be set to "g:/db/wombat".  The database will be booted if it
     * is not already running in the system.
     *
     * @param databaseName the name of the database
     */
    @Override
    public synchronized void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;

        if( databaseName!= null && databaseName.contains(";")) {
            String[] dbShort = databaseName.split(";");
            this.shortDatabaseName = dbShort[0];
        } else {
            this.shortDatabaseName = databaseName;
        }

        update();
    }

    /**
     * @return the database name set by {@link #setDatabaseName}.
     */
    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Return database name with ant attributes stripped off.
    */
    private String getShortDatabaseName() {
        return shortDatabaseName;
    }

    /**
     * Set the data source name.  The property is not mandatory.  It is used
     * for informational purposes only.
     *
     *  @param dsn the name of the data source
     */
    @Override
    public void setDataSourceName(String dsn) {
        dataSourceName = dsn;
    }

    /**
     * @return data source name as set in {@link #setDataSourceName}.
     */
    @Override
    public String getDataSourceName() {
        return dataSourceName;
    }

    /**
     * Set the data source descripton. This property is not mandatory.
     * It is used for informational purposes only.
     *
     * @param desc the description of the data source
     */
    @Override
    public void setDescription(String desc) {
        description = desc;
    }

    /**
     * @return the description as set in {@link #setDescription}.
     */
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Set the {@code user} property for the data source.
     * <p/>
     * This is user name for any data source {@code getConnection()} call
     * that takes no arguments.
     * @param user The user
    */
    @Override
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the user name as set by {@link #setUser}.
     */
    @Override
    public String getUser() {
        return user;
    }

    /**
     * Set the {@code password} property for the data source.
     * <p/>
     * This is user's password for any data source {@code getConnection()} call
     * that takes no arguments.
     * @param password The password in plain text
     */
    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the password as set in {@link #setPassword}.
     */
    @Override
    public String getPassword() {
        return password;
    }

    /*
     * DataSource methods
     */

    /**
     * Gets the maximum time in seconds that this data source can wait
     * while attempting to connect to a database.  A value of zero
     * means that the timeout is the default system timeout
     * if there is one; otherwise it means that there is no timeout.
     * When a data source object is created, the login timeout is
     * initially zero. See {@link #setLoginTimeout}.
     *
     * @return the data source login time limit
     * @exception SQLException if a database access error occurs.
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    /**
     * Sets the maximum time in seconds that this data source will wait
     * while attempting to connect to a database.  A value of zero
     * specifies that the timeout is the default system timeout
     * if there is one; otherwise it specifies that there is no timeout.
     * When a data source object is created, the login timeout is
     * initially zero.
     * <p/>
     * <b>Derby currently ignores this property.</b>
     *
     * @param seconds the data source login time limit
     * @exception SQLException if a database access error occurs.
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = seconds;
    }


    /**
     * Get the log writer for this data source.
     * <p/>
     * The log writer is a character output stream to which all logging
     * and tracing messages for this data source object instance will be
     * printed.  This includes messages printed by the methods of this
     * object, messages printed by methods of other objects manufactured
     * by this object, and so on.  Messages printed to a data source
     * specific log writer are not printed to the log writer associated
     * with the {@code java.sql.Drivermanager} class.
     * When a data source object is created the log writer is
     * initially null, in other words, logging is disabled.
     *
     * @return the log writer for this data source, null if disabled
     * @exception SQLException if a database-access error occurs.
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printer;
    }

    /**
     * Set the log writer for this data source.
     * <p/>
     * The log writer is a character output stream to which all logging
     * and tracing messages for this data source object instance will be
     * printed.  This includes messages printed by the methods of this
     * object, messages printed by methods of other objects manufactured
     * by this object, and so on.  Messages printed to a data source
     * specific log writer are not printed to the log writer associated
     * with the {@code java.sql.Drivermanager} class.
     * When a data source object is created the log writer is
     * initially null, in other words, logging is disabled.
     *
     * @param out the new log writer; to disable, set to null
     * @exception SQLException if a database-access error occurs.
     */
    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printer = out;
    }


    /**
     * Update {@link #jdbcurl} from attributes set.
     */
    protected void update() {
        StringBuilder sb = new StringBuilder(64);

        sb.append(Attribute.PROTOCOL);

        // Set the database name from the databaseName property
        String dbName = getDatabaseName();

        if (dbName != null) {
            dbName = dbName.trim();
        }

        if (dbName == null || dbName.length() == 0) {
            // need to put something in so that we do not allow the
            // database name to be set from the request or from the
            // connection attributes.

            // this space will selected as the database name (and
            // trimmed to an empty string) See the getDatabaseName()
            // code in InternalDriver. Since this is a non-null value,
            // it will be selected over any databaseName connection
            // attribute.
            dbName = " ";
        }

        sb.append(dbName);
        String connAttrs = getConnectionAttributes();

        if (connAttrs != null) {
            connAttrs = connAttrs.trim();

            if (connAttrs.length() != 0) {
                sb.append(';');
                sb.append(connectionAttributes);
            }
        }

        jdbcurl = sb.toString();
    }

    /*
     * Properties to be seen by Bean - access thru reflection.
     */

    /**
     * Set this property to create a new database.  If this property
     * is not set, the database (identified by {@code databaseName})
     * is assumed to be already existing.
     *
     * @param create if set to the string {@code "create"}, this data
     * source will try to create a new database of databaseName, or
     * boot the database if one by that name already exists.
     */
    @Override
    public void setCreateDatabase(String create) {
        if (create != null &&
            create.toLowerCase(java.util.Locale.ENGLISH).equals("create")) {
            createDatabase = create;
        } else {
            createDatabase = null;
        }
    }

    /**
     * @return The string {@code "create"} if create is set, or {@code
     * null} if not
     */
    @Override
    public String getCreateDatabase() {
        return createDatabase;
    }

    /**
     * Return a handle to the internal driver, possibly instantiating it first
     * if it hasn't been booted or if it has been shut down.
     *
     * @return The internal driver handle
     * @throws SQLException
     */
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    InternalDriver findDriver() throws SQLException {
        String url = jdbcurl;

        synchronized(this) {
            // The driver has either never been booted, or it has been
            // shutdown by a 'jdbc:derby:;shutdown=true'
            if (driver == null || !driver.acceptsURL(url)) {

                new org.apache.derby.jdbc.EmbeddedDriver();

                // If we know the driver, we loaded it.   Otherwise only
                // work if DriverManager has already loaded it.
                // DriverManager will throw an exception if driver is not found
                Driver registerDriver = DriverManager.getDriver(url);

                if (registerDriver instanceof AutoloadedDriver) {
                    driver =
                        (InternalDriver)AutoloadedDriver.getDriverModule();
                } else {
                    driver = (InternalDriver) registerDriver;
                }
            }
            // else driver != null and driver can accept url
        }

        return driver;
    }

    /**
     * Set this property to pass in more Derby specific connection URL
     * attributes.
     * <br>
     * Any attributes that can be set using a property of this DataSource
     * implementation (e.g user, password) should not be set in connection
     * attributes. Conflicting settings in connection attributes and
     * properties of the DataSource will lead to unexpected behaviour.
     *
     * @param prop set to the list of Derby connection attributes
     * separated by semi-colons.  E.g., to specify an encryption
     * bootPassword of "x8hhk2adf", and set upgrade to true, do the
     * following:
     *
     * <pre>
     *     ds.setConnectionAttributes("bootPassword=x8hhk2adf;upgrade=true");
     * </pre>
     *
     * See the Derby documentation for complete list.
     */
    @Override
    public void setConnectionAttributes(String prop) {
         connectionAttributes = prop;
         update();
    }


    /**
     * @return the Derby specific connection URL attributes, see
     * {@link #setConnectionAttributes}.
     */
    @Override
    public String getConnectionAttributes() {
        return connectionAttributes;
    }

    /**
     * Set this property if you wish to shutdown the database identified by
     * {@code databaseName}.
     *
     * @param shutdown if set to the string {@code "shutdown"}, this
     * data source will shutdown the database if it is running.
     */
    @Override
    public void setShutdownDatabase(String shutdown) {
        if (shutdown != null && shutdown.equalsIgnoreCase("shutdown")) {
            shutdownDatabase = shutdown;
        } else {
            shutdownDatabase = null;
        }
    }

    /**
     * @return the string {@code "shutdown"} if shutdown is set, or
     * null if not, cf.  {@link #setShutdownDatabase}.
     */
    @Override
    public String getShutdownDatabase() {
        return shutdownDatabase;
    }

    /**
     * Set {@code attributeAsPassword} property to enable passing connection
     * request attributes in the password argument of
     * {@link #getConnection(String,String)}.

     * If the property is set to {@code true} then the {@code password}
     * argument of the {@link #getConnection(String, String)}
     * method call is taken to be a list of connection attributes with the
     * same format as the {@code connectionAttributes} property.
     *
     * @param attributesAsPassword Use {@code true} to encode password
     * argument as a set of connection attributes in a connection request.
     */
    @Override
    public void setAttributesAsPassword(boolean attributesAsPassword) {
        this.attributesAsPassword = attributesAsPassword;
        update();
    }

    /**
     * Return the value of the {@code attributesAsPassword} property, cf.
     * {@link #setAttributesAsPassword}.
     * @return the value
     */
    @Override
    public boolean getAttributesAsPassword() {
        return attributesAsPassword;
    }

     // Most of our customers would be using JNDI to get the data
     // sources. Since we don't have a JNDI in the test setup to test this, we
     // are adding this method to fake it. This is getting used in XAJNDITest
     // so we can compare the two data sources.
    @Override
    public boolean equals(Object other) {

        if (other instanceof EmbeddedDataSource) {
            EmbeddedDataSource ds = (EmbeddedDataSource)other;

            boolean match = true;

            if (databaseName != null) {
                if  (!(databaseName.equals(ds.databaseName)))
                    match = false;
            } else if (ds.databaseName != null) {
                match = false;
            }

            if (dataSourceName != null) {
                if  (!(dataSourceName.equals(ds.dataSourceName))) {
                    match = false;
                }
            } else if (ds.dataSourceName != null) {
                match = false;
            }

            if (description != null) {
                if  (!(description.equals(ds.description))) {
                    match = false;
                }
            } else if (ds.description != null) {
                match = false;
            }

            if (createDatabase != null) {
                if  (!(createDatabase.equals(ds.createDatabase))) {
                    match = false;
                }
            } else if (ds.createDatabase != null) {
                match = false;
            }

            if (shutdownDatabase != null) {
                if  (!(shutdownDatabase.equals(ds.shutdownDatabase))) {
                    match = false;
                }
            } else if (ds.shutdownDatabase != null) {
                match = false;
            }

            if (connectionAttributes != null) {
                if  (!(connectionAttributes.equals(ds.connectionAttributes))) {
                    match = false;
                }
            } else if (ds.connectionAttributes != null) {
                match = false;
            }

            if (loginTimeout != ds.loginTimeout) {
                match = false;
            }

            return match;

        }

        return false;
    }

    // We don't really need this in the tests
    // (see equals), but unsafe not to
    // define hashCode if we define equals.
    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash +
            (this.description != null ?
             this.description.hashCode() : 0);
        hash = 29 * hash +
            (this.dataSourceName != null ?
             this.dataSourceName.hashCode() : 0);
        hash = 29 * hash +
            (this.databaseName != null ?
             this.databaseName.hashCode() : 0);
        hash = 29 * hash +
            (this.connectionAttributes != null ?
             this.connectionAttributes.hashCode() : 0);
        hash = 29 * hash +
            (this.createDatabase != null ?
             this.createDatabase.hashCode() : 0);
        hash = 29 * hash +
            (this.shutdownDatabase != null ?
             this.shutdownDatabase.hashCode() : 0);
        hash = 29 * hash +
            this.loginTimeout;
        return hash;
    }


    /*
     * DataSource methods - keep these non-final so that others can
     * extend Derby's classes if they choose to.
     */


    /**
     * Attempt to establish a database connection.
     *
     * @return  a Connection to the database
     * @exception SQLException if a database-access error occurs.
     */
    @Override
    public Connection getConnection() throws SQLException {
        return this.getConnection(getUser(), getPassword(), false);
    }

    /**
     * Attempt to establish a database connection with the given username and
     * password.  If the {@code attributeAsPassword} property is set to true
     * then the password argument is taken to be a list of connection
     * attributes with the same format as the {@code connectionAttributes}
     * property.
     *
     * @param username the database user on whose behalf the Connection is
     *     being made
     * @param password the user's password
     * @return  a Connection to the database
     * @exception SQLException if a database-access error occurs.
     */
    @Override
    public Connection getConnection(String username, String password)
            throws SQLException {
        return this.getConnection(username, password, true);
    }

    /**
     * Get a user connection: minion method.
     *
     * @param username the user name
     * @param password the password
     * @param requestPassword {@code true} if the password came from the
     *        getConnection() call with user and password arguments..
     * @return user connection
     * @throws SQLException
     */
    final Connection getConnection(String username,
                                   String password,
                                   boolean requestPassword)
            throws SQLException {

        Properties info = new Properties();

        if (username != null) {
            info.put(Attribute.USERNAME_ATTR, username);
        }

        if (!requestPassword || !attributesAsPassword) {
            if (password != null) {
                info.put(Attribute.PASSWORD_ATTR, password);
            }
        }

        if (createDatabase != null) {
            info.put(Attribute.CREATE_ATTR, "true");
        }

        if (shutdownDatabase != null) {
            info.put(Attribute.SHUTDOWN_ATTR, "true");
        }

        String url = jdbcurl;

        if (attributesAsPassword && requestPassword && password != null) {
            StringBuilder sb =
                new StringBuilder(url.length() + password.length() + 1);

            sb.append(url);
            sb.append(';');
            sb.append(password); // these are now request attributes on the URL

            url = sb.toString();
        }

        Connection conn =  findDriver().connect( url, info, loginTimeout );

        // JDBC driver's getConnection method returns null if
        // the driver does not handle the request's URL.
        if (conn == null) {
           throw Util.generateCsSQLException(SQLState.PROPERTY_INVALID_VALUE,
                                             Attribute.DBNAME_ATTR,
                                             getDatabaseName());
        }

        return conn;
    }

    // JDBC 4.0 java.sql.Wrapper interface methods

    /**
     * Returns false unless {@code interFace} is implemented.
     *
     * @param interFace a class defining an interface
     * @return {@code true} if this implements the interface or directly or
     *     indirectly wraps an object that does
     * @throws SQLException if an error occurs while determining
     *     whether this is a wrapper for an object with the given interface
     */
    @Override
    public boolean isWrapperFor(Class<?> interFace) throws SQLException {
        return interFace.isInstance(this);
    }

    /**
     * Returns {@code this} if this class implements the specified interface.
     *
     * @param iface a class defining an interface
     * @return an object that implements the interface
     * @throws SQLException if no object is found that implements the
     * interface
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // Derby does not implement non-standard methods on JDBC objects,
        // hence return this if this class implements the interface, or
        // throw an SQLException.
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                    iface);
        }
    }

    /**
     * Return a resource adapter. Use {@code ra} if non-null and active, else
     * get the one for the data base.
     *
     * @param ds The data source
     * @param ra The cached value if any
     * @param user The user name
     * @param password The password in clear text
     * @param requestPassword If {@code true}, use the supplied user and
     *                        password to boot the database if required
     * @return the resource adapter
     * @throws SQLException An error occurred
     */
    protected static ResourceAdapter setupResourceAdapter(
        EmbeddedXADataSourceInterface ds,
        ResourceAdapter ra,
        String user,
        String password,
        boolean requestPassword) throws SQLException {

        synchronized(ds) {
            if (ra == null || !ra.isActive()) {
                // If it is inactive, it is useless.
                ra = null;

                // DERBY-4907 make sure the database name sent to find service
                // does not include attributes.
                String dbName =
                    ((BasicEmbeddedDataSource40)ds).getShortDatabaseName();

                if (dbName != null) {
                    // see if database already booted, if it is, then
                    // don't make a connection.
                    Database database = null;

                    // if monitor is never setup by any ModuleControl,
                    // getMonitor returns null and no Derby database
                    // has been booted.
                    if (getMonitor() != null) {
                        database = (Database)
                            findService(Property.DATABASE_MODULE,
                                                dbName);
                    }

                    if (database == null) {
                        // If database is not found, try connecting to it.
                        // This boots and/or creates the database.  If
                        // database cannot be found, this throws SQLException.
                        if (requestPassword) {
                            ds.getConnection(user, password).close();
                        } else {
                            ds.getConnection().close();
                        }

                        // now try to find it again
                        database = (Database)
                            findService(Property.DATABASE_MODULE,
                                                dbName);
                    }

                    if (database != null) {
                        ra = (ResourceAdapter) database.getResourceAdapter();
                    }
                }

                if (ra == null) {
                    throw new SQLException(
                        MessageService.getTextMessage(
                            MessageId.CORE_DATABASE_NOT_AVAILABLE),
                        "08006",
                        ExceptionSeverity.DATABASE_SEVERITY);
                }

                // If database is already up, we need to set up driver
                // seperately.
                InternalDriver driver =
                    ((BasicEmbeddedDataSource40)ds).findDriver();

                if (driver == null) {
                    throw new SQLException(
                        MessageService.getTextMessage(
                            MessageId.CORE_DRIVER_NOT_AVAILABLE),
                        "08006",
                        ExceptionSeverity.DATABASE_SEVERITY);
                }
            }
        }

        return ra;
    }
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    // Cannot add @Override here: the compiler balks since this is compiled
    // with compiler level 1.6 which doesn't specify this method in the
    // JDBC interface (4.0). Add as soon as we move to minimum 1.7.
    public  Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw (SQLFeatureNotSupportedException) Util.generateCsSQLException
            ( SQLState.NOT_IMPLEMENTED, "getParentLogger" );
    }

    ////////////////////////////////////////////////////////////////////
    //
    // SECURITY
    //
    ////////////////////////////////////////////////////////////////////

    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

    /**
     * Privileged service lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object findService( final String factoryInterface, final String serviceName )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.findService( factoryInterface, serviceName );
                 }
             }
             );
    }
    
}
