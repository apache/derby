/*

   Derby - Class org.apache.derby.jdbc.ClientBaseDataSource

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.jdbc;

import java.io.Serializable;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import javax.naming.Referenceable;
import javax.naming.Reference;
import javax.naming.NamingException;
import javax.naming.StringRefAddr;

import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Connection;
import org.apache.derby.client.net.NetConfiguration;
import org.apache.derby.client.net.NetLogWriter;
import org.apache.derby.client.ClientDataSourceFactory;

/**
 * Base class for client-side DataSource implementations.
 */
public abstract class ClientBaseDataSource implements Serializable, Referenceable {
    private static final long serialVersionUID = -7660172643035173692L;
    
    // The loginTimeout jdbc 2 data source property is not supported as a jdbc 1 connection property,
    // because loginTimeout is set by the jdbc 1 api via java.sql.DriverManager.setLoginTimeout().
    // The databaseName, serverName, and portNumber data source properties are also not supported as connection properties
    // because they are extracted from the jdbc 1 database url passed on the connection request.
    // However, all other data source properties should probably also be supported as connection properties.

    //---------------------contructors/finalizers---------------------------------

    // This class is abstract, hide the default constructor
    ClientBaseDataSource() {
    }

    // ---------------------------- loginTimeout -----------------------------------
    //
    // was serialized in 1.0 release
    /**
     * The time in seconds to wait for a connection request on this data source. The default value of zero indicates
     * that either the system time out be used or no timeout limit.
     *
     * @serial
     */
    private int loginTimeout = propertyDefault_loginTimeout;
    public final static String propertyKey_loginTimeout = "loginTimeout";
    public static final int propertyDefault_loginTimeout = 0;

    public synchronized void setLoginTimeout(int seconds) {
        this.loginTimeout = seconds;
    }

    public int getLoginTimeout() {
        return this.loginTimeout;
    }

    // ---------------------------- logWriter -----------------------------------
    //
    /**
     * The log writer is declared transient, and is not serialized or stored under JNDI.
     *
     * @see #traceLevel
     */
    private transient PrintWriter logWriter;

    public synchronized void setLogWriter(PrintWriter logWriter) {
        this.logWriter = logWriter;
    }

    public PrintWriter getLogWriter() {
        return this.logWriter;
    }

    // ---------------------------- databaseName -----------------------------------
    //
    // Stores the relational database name, RDBNAME.
    // The length of the database name may be limited to 18 bytes
    // and therefore may throw an SQLException.
    //
    //
    private String databaseName;
    public final static String propertyKey_databaseName = "databaseName";

    // databaseName is not permitted in a properties object


    // ---------------------------- description ------------------------------
    // A description of this data source.
    private String description;
    public final static String propertyKey_description = "description";

    // ---------------------------- dataSourceName -----------------------------------
    //
    // A data source name;
    // used to name an underlying XADataSource,
    // or ConnectionPoolDataSource when pooling of connections is done.
    //
    private String dataSourceName;
    public final static String propertyKey_dataSourceName = "dataSourceName";

    // ---------------------------- portNumber -----------------------------------
    //
    private int portNumber = propertyDefault_portNumber;
    public final static int propertyDefault_portNumber = 1527;
    public final static String propertyKey_portNumber = "portNumber";

    // ---------------------------- serverName -----------------------------------
    //
    // Derby-410 fix.
    private String serverName = propertyDefault_serverName;
    public final static String propertyDefault_serverName = "localhost";
    public final static String propertyKey_serverName = "serverName";

    // serverName is not permitted in a properties object

    // ---------------------------- user -----------------------------------
    //
    // This property can be overwritten by specifing the
    // username parameter on the DataSource.getConnection() method
    // call.  If user is specified, then password must also be
    // specified, either in the data source object or provided on
    // the DataSource.getConnection() call.
    //
    // Each data source implementation subclass will maintain it's own <code>password</code> property.
    // This password property may or may not be declared transient, and therefore may be serialized
    // to a file in clear-text, care must taken by the user to prevent security breaches.
    // Derby-406 fix
    private String user = propertyDefault_user;
    public final static String propertyKey_user = "user";
    public final static String propertyDefault_user = "APP";

    public static String getUser(Properties properties) {
        String userString = properties.getProperty(propertyKey_user);
        return parseString(userString, propertyDefault_user);
    }

    public final static int HOLD_CURSORS_OVER_COMMIT = 1; // this matches jdbc 3 ResultSet.HOLD_CURSORS_OVER_COMMIT
    public final static int CLOSE_CURSORS_AT_COMMIT = 2;  // this matches jdbc 3 ResultSet.CLOSE_CURSORS_AT_COMMIT


    // ---------------------------- securityMechanism -----------------------------------
    //
    // The source security mechanism to use when connecting to this data source.
    // <p>
    // Security mechanism options are:
    // <ul>
    // <li> USER_ONLY_SECURITY
    // <li> CLEAR_TEXT_PASSWORD_SECURITY
    // <li> ENCRYPTED_PASSWORD_SECURITY
    // <li> ENCRYPTED_USER_AND_PASSWORD_SECURITY - both password and user are encrypted
    // </ul>
    // The default security mechanism is USER_ONLY_SECURITY.
    // <p>
    // If the application specifies a security
    // mechanism then it will be the only one attempted.
    // If the specified security mechanism is not supported by the conversation
    // then an exception will be thrown and there will be no additional retries.
    // <p>
    // This property is currently only available for the  DNC driver.
    // <p>
    // Both user and password need to be set for all security mechanism except USER_ONLY_SECURITY
    // When using USER_ONLY_SECURITY, only the user property needs to be specified.
    //
    protected short securityMechanism = propertyDefault_securityMechanism;
    // TODO default  should be  USER_ONLY_SECURITY. Change when working on
    // Network Server
    //  public final static short propertyDefault_securityMechanism = (short)
    //  org.apache.derby.client.net.NetConfiguration.SECMEC_USRIDONL;
    public final static short propertyDefault_securityMechanism = (short) NetConfiguration.SECMEC_USRIDONL;
    public final static String propertyKey_securityMechanism = "securityMechanism";


    // We use the NET layer constants to avoid a mapping for the NET driver.
    public static short getSecurityMechanism(Properties properties) {
        String securityMechanismString = properties.getProperty(propertyKey_securityMechanism);
        String passwordString = properties.getProperty(propertyKey_password);
        short setSecurityMechanism = parseShort(securityMechanismString, propertyDefault_securityMechanism);
        return getUpgradedSecurityMechanism(setSecurityMechanism, passwordString);
    }


    /**
     * Upgrade the security mechansim to USRIDPWD if it is set to USRIDONL but we have a password.
     */
    public static short getUpgradedSecurityMechanism(short securityMechanism, String password) {
        // if securityMechanism is USER_ONLY (the default) we may need
        // to change it to CLEAR_TEXT_PASSWORD in order to send the password.
        if ((password != null) && (securityMechanism == NetConfiguration.SECMEC_USRIDONL)) {
            return (short) NetConfiguration.SECMEC_USRIDPWD;
        } else {
            return securityMechanism;
        }
    }

    // ---------------------------- getServerMessageTextOnGetMessage -----------------------------------
    //
    private boolean retrieveMessageText = propertyDefault_retrieveMessageText;
    public final static boolean propertyDefault_retrieveMessageText = true;
    public final static String propertyKey_retrieveMessageText = "retrieveMessageText";


    public static boolean getRetrieveMessageText(Properties properties) {
        String retrieveMessageTextString = properties.getProperty(propertyKey_retrieveMessageText);
        return parseBoolean(retrieveMessageTextString, propertyDefault_retrieveMessageText);
    }

    // ---------------------------- traceFile -----------------------------------
    //
    private String traceFile;
    public final static String propertyKey_traceFile = "traceFile";

    public static String getTraceFile(Properties properties) {
        return properties.getProperty(propertyKey_traceFile);
    }

    // ---------------------------- traceDirectory -----------------------------------
    // For the suffix of the trace file when traceDirectory is enabled.
    private transient int traceFileSuffixIndex_ = 0;
    //
    private String traceDirectory;
    public final static String propertyKey_traceDirectory = "traceDirectory";

    public static String getTraceDirectory(Properties properties) {
        return properties.getProperty(propertyKey_traceDirectory);
    }

    // ---------------------------- traceFileAppend -----------------------------------
    //
    private boolean traceFileAppend = propertyDefault_traceFileAppend;
    public final static boolean propertyDefault_traceFileAppend = false;
    public final static String propertyKey_traceFileAppend = "traceFileAppend";

    public static boolean getTraceFileAppend(Properties properties) {
        String traceFileAppendString = properties.getProperty(propertyKey_traceFileAppend);
        return parseBoolean(traceFileAppendString, propertyDefault_traceFileAppend);
    }

    // ---------------------------- password -----------------------------------
    //
    // The password property is defined in subclasses, but the method
    // getPassword (java.util.Properties properties) is in this class to eliminate
    // dependencies on j2ee for connections that go thru the driver manager.
    public final static String propertyKey_password = "password";

    public static String getPassword(Properties properties) {
        return properties.getProperty("password");
    }

    private String password;

    synchronized public final void setPassword(String password) {
        this.password = password;
    }
    
    public final String getPassword() {
    	return password;
    }

    //------------------------ interface methods ---------------------------------

    public Reference getReference() throws NamingException {
        // This method creates a new Reference object to represent this data source.
        // The class name of the data source object is saved in the Reference,
        // so that an object factory will know that it should create an instance
        // of that class when a lookup operation is performed. The class
        // name of the object factory, org.apache.derby.client.ClientBaseDataSourceFactory,
        // is also stored in the reference.
        // This is not required by JNDI, but is recommend in practice.
        // JNDI will always use the object factory class specified in the reference when
        // reconstructing an object, if a class name has been specified.
        // See the JNDI SPI documentation
        // for further details on this topic, and for a complete description of the Reference
        // and StringRefAddr classes.
        //
        // This ClientBaseDataSource class provides several standard JDBC properties.
        // The names and values of the data source properties are also stored
        // in the reference using the StringRefAddr class.
        // This is all the information needed to reconstruct a ClientBaseDataSource object.

        Reference ref = new Reference(this.getClass().getName(), ClientDataSourceFactory.class.getName(), null);

        addBeanProperties(ref);
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
    private void addBeanProperties(Reference ref)
    {
        // Look for all the getXXX methods in the class that take no arguments.
        Method[] methods = this.getClass().getMethods();
        
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
                    Object ov = m.invoke(this, null);
                    String value = ov == null ? null : ov.toString();
                    ref.add(new StringRefAddr(propertyName, value));
                } catch (IllegalAccessException iae) {
                } catch (InvocationTargetException ite) {
                }

            }
        }
    }

    // ----------------------supplemental methods---------------------------------
    /**
     * Not an external.  Do not document in pubs. Returns all non-transient properties of a ClientBaseDataSource.
     */
    public Properties getProperties() throws SqlException {
        Properties properties = new Properties();

        Class clz = getClass();
        Field[] fields = clz.getFields();
        for (int i = 0; i < fields.length; i++) {
            String name = fields[i].getName();
            if (name.startsWith("propertyKey_")) {
                if (Modifier.isTransient(fields[i].getModifiers())) {
                    continue; // if it is transient, then skip this propertyKey.
                }
                try {
                    String propertyKey = fields[i].get(this).toString();
                    // search for property field.
                    Field propertyField;
                    clz = getClass(); // start from current class.
                    while (true) {
                        try {
                            propertyField = clz.getDeclaredField(name.substring(12));
                            break; // found the property field, so break the while loop.
                        } catch (NoSuchFieldException nsfe) {
                            // property field is not found at current level of class, so continue to super class.
                            clz = clz.getSuperclass();
                            if (clz == Object.class) {
                                throw new SqlException(new LogWriter(logWriter, traceLevel), "bug check: corresponding property field does not exist");
                            }
                            continue;
                        }
                    }

                    if (!Modifier.isTransient(propertyField.getModifiers())) {
                        // if the property is not transient:
                        // get the property.
                        propertyField.setAccessible(true);
                        Object propertyObj = propertyField.get(this);
                        String property = String.valueOf(propertyObj); // don't use toString becuase it may be null.
                        if ("password".equals(propertyKey)) {
                            StringBuffer sb = new StringBuffer(property);
                            for (int j = 0; j < property.length(); j++) {
                                sb.setCharAt(j, '*');
                            }
                            property = sb.toString();
                        }
                        // add into prperties.
                        properties.setProperty(propertyKey, property);
                    }
                } catch (IllegalAccessException e) {
                    throw new SqlException(new LogWriter(this.logWriter, this.traceLevel), "bug check: property cannot be accessed");
                }
            }
        }

        return properties;
    }

    //---------------------- helper methods --------------------------------------

    // The java.io.PrintWriter overrides the traceFile setting.
    // If neither traceFile nor jdbc logWriter are set, then null is returned.
    // logWriterInUseSuffix used only for trace directories to indicate whether
    // log writer is use is from xads, cpds, sds, ds, driver, config, reset.
    LogWriter computeDncLogWriterForNewConnection(String logWriterInUseSuffix) throws SqlException {
        return computeDncLogWriterForNewConnection(logWriter, traceDirectory, traceFile, traceFileAppend, traceLevel, logWriterInUseSuffix, traceFileSuffixIndex_++);
    }

    // Called on for connection requests.
    // The java.io.PrintWriter overrides the traceFile setting.
    // If neither traceFile, nor logWriter, nor traceDirectory are set, then null is returned.
    static LogWriter computeDncLogWriterForNewConnection(PrintWriter logWriter, String traceDirectory, String traceFile, boolean traceFileAppend, int traceLevel, String logWriterInUseSuffix, int traceFileSuffixIndex) throws SqlException {
        int globaltraceFileSuffixIndex = Configuration.traceFileSuffixIndex__++;

        // compute regular dnc log writer if there is any
        LogWriter dncLogWriter = computeDncLogWriter(logWriter, traceDirectory, traceFile, traceFileAppend, logWriterInUseSuffix, traceFileSuffixIndex, traceLevel);
        if (dncLogWriter != null) {
            return dncLogWriter;
        }
        // compute global default dnc log writer if there is any
        dncLogWriter = computeDncLogWriter(null, Configuration.traceDirectory__, Configuration.traceFile__, Configuration.traceFileAppend__, "_global", globaltraceFileSuffixIndex, Configuration.traceLevel__);
        return dncLogWriter;
    }

    // Compute a DNC log writer before a connection is created.
    static LogWriter computeDncLogWriter(PrintWriter logWriter, String traceDirectory, String traceFile, boolean traceFileAppend, String logWriterInUseSuffix, int traceFileSuffixIndex, int traceLevel) throws SqlException {
        // Otherwise, the trace file will still be created even TRACE_NONE.
        if (traceLevel == TRACE_NONE) {
            return null;
        }

        PrintWriter printWriter = computePrintWriter(logWriter, traceDirectory, traceFile, traceFileAppend, logWriterInUseSuffix, traceFileSuffixIndex);
        if (printWriter == null) {
            return null;
        }

        LogWriter dncLogWriter = new NetLogWriter(printWriter, traceLevel);
        if (printWriter != logWriter && traceDirectory != null)
        // When printWriter is an internal trace file and
        // traceDirectory is not null, each connection has
        // its own trace file and the trace file is not cached,
        // so we can close it when DNC log writer is closed.
        {
            dncLogWriter.printWriterNeedsToBeClosed_ = true;
        }
        return dncLogWriter;
    }

    // Compute a DNC log writer after a connection is created.
    // Declared public for use by am.Connection.  Not a public external.
    public static LogWriter computeDncLogWriter(Connection connection, PrintWriter logWriter, String traceDirectory, String traceFile, boolean traceFileAppend, String logWriterInUseSuffix, int traceFileSuffixIndex, int traceLevel) throws SqlException {
        // Otherwise, the trace file will still be created even TRACE_NONE.
        if (traceLevel == TRACE_NONE) {
            return null;
        }

        PrintWriter printWriter = computePrintWriter(logWriter, traceDirectory, traceFile, traceFileAppend, logWriterInUseSuffix, traceFileSuffixIndex);
        if (printWriter == null) {
            return null;
        }

        LogWriter dncLogWriter = connection.agent_.newLogWriter_(printWriter, traceLevel);
        if (printWriter != logWriter && traceDirectory != null)
        // When printWriter is an internal trace file and
        // traceDirectory is not null, each connection has
        // its own trace file and the trace file is not cached,
        // so we can close it when DNC log writer is closed.
        {
            dncLogWriter.printWriterNeedsToBeClosed_ = true;
        }
        return dncLogWriter;
    }

    // This method handles all the override semantics.
    // The logWriter overrides the traceFile, and traceDirectory settings.
    // If neither traceFile, nor logWriter, nor traceDirectory are set, then null is returned.
    static PrintWriter computePrintWriter(PrintWriter logWriter, String traceDirectory, String traceFile, boolean traceFileAppend, String logWriterInUseSuffix, int traceFileSuffixIndex) throws SqlException {
        if (logWriter != null)  // java.io.PrintWriter is specified
        {
            return logWriter;
        } else { // check trace file setting.
            if (traceDirectory != null) {
                String fileName;
                if (traceFile == null) {
                    fileName = traceDirectory + "/" + logWriterInUseSuffix + "_" + traceFileSuffixIndex;
                } else {
                    fileName = traceDirectory + "/" + traceFile + logWriterInUseSuffix + "_" + traceFileSuffixIndex;
                }
                return LogWriter.getPrintWriter(fileName, true); // no file append and not enable caching.
            } else if (traceFile != null) {
                return LogWriter.getPrintWriter(traceFile, traceFileAppend);
            }
        }
        return null;
    }

    private static boolean parseBoolean(String boolString, boolean defaultBool) {
        if (boolString != null) {
            return (boolString.equalsIgnoreCase("true") || boolString.equalsIgnoreCase("yes"));
        }
        return defaultBool;
    }

    private static String parseString(String string, String defaultString) {
        if (string != null) {
            return string;
        }
        return defaultString;
    }

    private static short parseShort(String shortString, short defaultShort) {
        if (shortString != null) {
            return Short.parseShort(shortString);
        }
        return defaultShort;
    }

    private static int parseInt(String intString, int defaultInt) {
        if (intString != null) {
            return Integer.parseInt(intString);
        }
        return defaultInt;
    }

    // tokenize "property=value;property=value..." and returns new properties object
    //This method is used both by ClientDriver to parse the url and
    // ClientDataSource.setConnectionAttributes
    static Properties tokenizeAttributes(String attributeString, Properties properties) throws SqlException {
        Properties augmentedProperties;

        if (attributeString == null) {
            return properties;
        }

        if (properties != null) {
            augmentedProperties = (Properties) properties.clone();
        } else {
            augmentedProperties = new Properties();
        }
        try {
            StringTokenizer attrTokenizer = new StringTokenizer(attributeString, ";");
            while (attrTokenizer.hasMoreTokens()) {
                String v = attrTokenizer.nextToken();

                int eqPos = v.indexOf('=');
                if (eqPos == -1) {
                    throw new SqlException(null, "Invalid attribute syntax: " + attributeString);
                }

                augmentedProperties.setProperty((v.substring(0, eqPos)).trim(), (v.substring(eqPos + 1)).trim());
            }
        } catch (NoSuchElementException e) {
            // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
            throw new SqlException(null, e, "Invalid attribute syntax: " + attributeString);
        }
        checkBoolean(augmentedProperties, propertyKey_retrieveMessageText);
        return augmentedProperties;

    }

    private static void checkBoolean(Properties set, String attribute) throws SqlException {
        final String[] booleanChoices = {"true", "false"};
        checkEnumeration(set, attribute, booleanChoices);
    }


    private static void checkEnumeration(Properties set, String attribute, String[] choices) throws SqlException {
        String value = set.getProperty(attribute);
        if (value == null) {
            return;
        }

        for (int i = 0; i < choices.length; i++) {
            if (value.toUpperCase(java.util.Locale.ENGLISH).equals(choices[i].toUpperCase(java.util.Locale.ENGLISH))) {
                return;
            }
        }

// The attribute value is invalid. Construct a string giving the choices for
// display in the error message.
        String choicesStr = "{";
        for (int i = 0; i < choices.length; i++) {
            if (i > 0) {
                choicesStr += "|";
            }
            choicesStr += choices[i];
        }

        throw new SqlException(null, "JDBC attribute " + attribute +
                "has an invalid value " + value +
                " Valid values are " + choicesStr);
    }

    /*
     * Properties to be seen by Bean - access thru reflection.
     */

    // -- Stardard JDBC DataSource Properties

    public synchronized void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }


    public synchronized void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public String getDataSourceName() {
        return this.dataSourceName;
    }

    public synchronized void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }


    public synchronized void setPortNumber(int portNumber) {
        this.portNumber = portNumber;
    }

    public int getPortNumber() {
        return this.portNumber;
    }

    public synchronized void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getServerName() {
        return this.serverName;
    }


    public synchronized void setUser(String user) {
        this.user = user;
    }

    public String getUser() {
        return this.user;
    }

    synchronized public void setRetrieveMessageText(boolean retrieveMessageText) {
        this.retrieveMessageText = retrieveMessageText;
    }

    public boolean getRetrieveMessageText() {
        return this.retrieveMessageText;
    }

    // ---------------------------- securityMechanism -----------------------------------
    /**
     * The source security mechanism to use when connecting to this data source.
     * <p/>
     * Security mechanism options are: <ul> <li> USER_ONLY_SECURITY <li> CLEAR_TEXT_PASSWORD_SECURITY <li>
     * ENCRYPTED_PASSWORD_SECURITY <li> ENCRYPTED_USER_AND_PASSWORD_SECURITY - both password and user are encrypted
     * </ul> The default security mechanism is USER_ONLY SECURITY
     * <p/>
     * If the application specifies a security mechanism then it will be the only one attempted. If the specified
     * security mechanism is not supported by the conversation then an exception will be thrown and there will be no
     * additional retries.
     * <p/>
     * This property is currently only available for the  DNC driver.
     * <p/>
     * Both user and password need to be set for all security mechanism except USER_ONLY_SECURITY
     */
    // We use the NET layer constants to avoid a mapping for the NET driver.
    public final static short USER_ONLY_SECURITY = (short) NetConfiguration.SECMEC_USRIDONL;
    public final static short CLEAR_TEXT_PASSWORD_SECURITY = (short) NetConfiguration.SECMEC_USRIDPWD;
    public final static short ENCRYPTED_PASSWORD_SECURITY = (short) NetConfiguration.SECMEC_USRENCPWD;
    public final static short ENCRYPTED_USER_AND_PASSWORD_SECURITY = (short) NetConfiguration.SECMEC_EUSRIDPWD;

    synchronized public void setSecurityMechanism(short securityMechanism) {
        this.securityMechanism = securityMechanism;
    }

    public short getSecurityMechanism() {
        return getUpgradedSecurityMechanism(securityMechanism, password);
    }

    protected String connectionAttributes = null;
    public final static String propertyKey_connectionAttributes = "connectionAttributes";

    /**
     * Set this property to pass in more Derby specific connection URL attributes.
     * <BR>
     * Any attributes that can be set using a property of this DataSource implementation
     * (e.g user, password) should not be set in connectionAttributes. Conflicting
     * settings in connectionAttributes and properties of the DataSource will lead to
     * unexpected behaviour. 
     *
     * @param prop set to the list of Cloudscape connection attributes separated by semi-colons.   E.g., to specify an
     *             encryption bootPassword of "x8hhk2adf", and set upgrade to true, do the following: <PRE>
     *             ds.setConnectionAttributes("bootPassword=x8hhk2adf;upgrade=true"); </PRE> See Derby documentation for
     *             complete list.
     */
    public final void setConnectionAttributes(String prop) {
        connectionAttributes = prop;
    }

    /**
     * @return Derby specific connection URL attributes
     */
    public final String getConnectionAttributes() {
        return connectionAttributes;
    }


    // ---------------------------- traceLevel -----------------------------------
    //

    public final static int TRACE_NONE = 0x0;
    public final static int TRACE_CONNECTION_CALLS = 0x1;
    public final static int TRACE_STATEMENT_CALLS = 0x2;
    public final static int TRACE_RESULT_SET_CALLS = 0x4;
    public final static int TRACE_DRIVER_CONFIGURATION = 0x10;
    public final static int TRACE_CONNECTS = 0x20;
    public final static int TRACE_PROTOCOL_FLOWS = 0x40;
    public final static int TRACE_RESULT_SET_META_DATA = 0x80;
    public final static int TRACE_PARAMETER_META_DATA = 0x100;
    public final static int TRACE_DIAGNOSTICS = 0x200;
    public final static int TRACE_XA_CALLS = 0x800;
    public final static int TRACE_ALL = 0xFFFFFFFF;

    public final static int propertyDefault_traceLevel = TRACE_ALL;
    public final static String propertyKey_traceLevel = "traceLevel";

    protected int traceLevel = propertyDefault_traceLevel;

    public static int getTraceLevel(Properties properties) {
        String traceLevelString = properties.getProperty(propertyKey_traceLevel);
        return parseInt(traceLevelString, propertyDefault_traceLevel);
    }

    synchronized public void setTraceLevel(int traceLevel) {
        this.traceLevel = traceLevel;
    }

    public int getTraceLevel() {
        return this.traceLevel;
    }


    public synchronized void setTraceFile(String traceFile) {
        this.traceFile = traceFile;
    }

    public String getTraceFile() {
        return this.traceFile;
    }


    public synchronized void setTraceDirectory(String traceDirectory) {
        this.traceDirectory = traceDirectory;
    }

    public String getTraceDirectory() {
        return this.traceDirectory;
    }

    synchronized public void setTraceFileAppend(boolean traceFileAppend) {
        this.traceFileAppend = traceFileAppend;
    }

    public boolean getTraceFileAppend() {
        return this.traceFileAppend;
    }




    // --- private helper methods


    /**
     * The dataSource keeps individual fields for the values that are relevant to the client. These need to be updated
     * when set connection attributes is called.
     */
    void updateDataSourceValues(Properties prop) {
        if (prop == null) {
            return;
        }
        
        if (prop.containsKey(propertyKey_user)) {
            setUser(getUser(prop));
        }
        if (prop.containsKey(propertyKey_securityMechanism)) {
            setSecurityMechanism(getSecurityMechanism(prop));
        }
        if (prop.containsKey(propertyKey_traceFile)) {
            setTraceFile(getTraceFile(prop));
        }
        if (prop.containsKey(propertyKey_traceDirectory)) {
            setTraceDirectory(getTraceDirectory(prop));
        }
        if (prop.containsKey(propertyKey_traceFileAppend)) {
            setTraceFileAppend(getTraceFileAppend(prop));
        }
        if (prop.containsKey(propertyKey_securityMechanism)) {
            setSecurityMechanism(getSecurityMechanism(prop));
        }
        if (prop.containsKey(propertyKey_retrieveMessageText)) {
            setRetrieveMessageText(getRetrieveMessageText(prop));
        }
    }


}


