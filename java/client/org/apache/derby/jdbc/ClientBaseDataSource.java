/*

   Derby - Class org.apache.derby.jdbc.ClientBaseDataSource

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

import java.io.Serializable;
import java.io.PrintWriter;
import java.io.File;
import java.security.AccessController;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.sql.SQLException;
import javax.naming.Referenceable;
import javax.naming.Reference;
import javax.naming.NamingException;
import javax.naming.StringRefAddr;

import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Connection;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.net.NetConfiguration;
import org.apache.derby.client.net.NetLogWriter;
import org.apache.derby.client.ClientDataSourceFactory;
import org.apache.derby.shared.common.error.ExceptionUtil;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * Base class for client-side DataSource implementations.
 */
public abstract class ClientBaseDataSource implements Serializable, Referenceable {
    private static final long serialVersionUID = -7660172643035173692L;

    // Spec requires DH algorithm with 32bytes prime to be used 
    // Not all JCE implementations have support for this. E.g.
    // Sun JCE does not support DH(prime of 32bytes).
    // store information if client JVM has JCE loaded that 
    // can support the necessary algorithms required for EUSRIDPWD
    // (encrypted userid and password) security mechanism
    // this information is needed to decide if security mechanism 
    // can be upgraded to EUSRIDPWD or not
    // See getUpgradedSecurityMechanism()
    static final boolean SUPPORTS_EUSRIDPWD;
    
    static
    {
        boolean supports_eusridpwd = false;
        try
        {
            // The EncryptionManager class will instantiate objects of the required 
            // security algorithms that are needed for EUSRIDPWD
            // An exception will be thrown if support is not available
            // in the JCE implementation in the JVM in which the client
            // is loaded.
            new org.apache.derby.client.am.EncryptionManager(null);
            supports_eusridpwd = true;
        }catch(Exception e)
        {
            // if an exception is thrown, ignore exception.
            // set SUPPORTS_EUSRIDPWD to false indicating that the client 
            // does not support EUSRIDPWD security mechanism
            supports_eusridpwd = false;
        }
        SUPPORTS_EUSRIDPWD = supports_eusridpwd;
    }
    
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
    private int loginTimeout;

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
    
    // databaseName is not permitted in a properties object


    // ---------------------------- description ------------------------------
    // A description of this data source.
    private String description;
    
    // ---------------------------- dataSourceName -----------------------------------
    //
    // A data source name;
    // used to name an underlying XADataSource,
    // or ConnectionPoolDataSource when pooling of connections is done.
    //
    private String dataSourceName;
    
    // ---------------------------- portNumber -----------------------------------
    //
    private int portNumber = propertyDefault_portNumber;
    public final static int propertyDefault_portNumber = 1527;
    
    // ---------------------------- serverName -----------------------------------
    //
    // Derby-410 fix.
    private String serverName = propertyDefault_serverName;
    public final static String propertyDefault_serverName = "localhost";
   
    // serverName is not permitted in a properties object

    //---------------------- client SSL ----------------

    /** The constant indicating that SSL encryption won't be used. */
    public final static int SSL_OFF = 0;
    private final static String SSL_OFF_STR = "off";
    /** The constant indicating that SSL encryption will be used. */
    public final static int SSL_BASIC = 1;
    private final static String SSL_BASIC_STR = "basic";
    /**
     * The constant indicating that SSL encryption with peer authentication
     * will be used.
     */
    public final static int SSL_PEER_AUTHENTICATION = 2;
    private final static String SSL_PEER_AUTHENTICATION_STR =
            "peerAuthentication";

    /**
     * Parses the string and returns the corresponding constant for the SSL
     * mode denoted.
     * <p>
     * Valid values are <tt>off</tt>, <tt>basic</tt> and
     * <tt>peerAuthentication</tt>.
     *
     * @param s string denoting the SSL mode
     * @return A constant indicating the SSL mode denoted by the string. If the
     *      string is {@code null}, {@link #SSL_OFF} is returned.
     * @throws SqlException if the string has an invalid value
     */
    public static final int getSSLModeFromString(String s) 
        throws SqlException
    {
        
		if (s != null){
			if (s.equalsIgnoreCase(SSL_OFF_STR)) {
				return SSL_OFF;
            } else if (s.equalsIgnoreCase(SSL_BASIC_STR)) {
				return SSL_BASIC;
			} else if (s.equalsIgnoreCase(SSL_PEER_AUTHENTICATION_STR)) {
				return SSL_PEER_AUTHENTICATION;
			} else {
                throw new SqlException(null,
                        new ClientMessageId(SQLState.INVALID_ATTRIBUTE),
                        Attribute.SSL_ATTR, s, SSL_OFF_STR + ", " +
                        SSL_BASIC_STR + ", " + SSL_PEER_AUTHENTICATION_STR);
			}
		} else {
			// Default
			return SSL_OFF;
		}
    }
    
    /**
     * Returns the SSL mode specified by the property object.
     *
     * @param properties data source properties
     * @return A constant indicating the SSL mode to use. Defaults to
     *      {@link #SSL_OFF} if the SSL attribute isn't specified.
     * @throws SqlException if an invalid value for the SSL mode is specified
     *      in the property object
     */
    public static final int getClientSSLMode(Properties properties)
        throws SqlException
    {
        return getSSLModeFromString(properties.getProperty(Attribute.SSL_ATTR));
    }

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
    public final static String propertyDefault_user = "APP";

    public static String getUser(Properties properties) {
        String userString = properties.getProperty(Attribute.USERNAME_ATTR);
        return parseString(userString, propertyDefault_user);
    }

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
    // <li> STRONG_PASSWORD_SUBSTITUTE_SECURITY
    // </ul>
    // The default security mechanism is USER_ONLY_SECURITY.
    // <p>
    // If the application specifies a security
    // mechanism then it will be the only one attempted.
    // If the specified security mechanism is not supported by the conversation
    // then an exception will be thrown and there will be no additional retries.
    // <p>
    // This property is currently only available for the DNC driver.
    // <p>
    // Both user and password need to be set for all security mechanism except USER_ONLY_SECURITY
    // When using USER_ONLY_SECURITY, only the user property needs to be specified.
    //
    
    // constant to indicate that the security mechanism has not been 
    // explicitly set, either on connection request when using DriverManager 
    // or on the Client DataSource object
    private final static short SECMEC_HAS_NOT_EXPLICITLY_SET = 0;
    
    // Security Mechanism can be specified explicitly either when obtaining a 
    // connection via a DriverManager or via Datasource. 
    // Via DriverManager, securityMechanism can be set on the connection request using 
    // the 'securityMechanism' attribute. 
    // Via DataSource, securityMechanism can be set by calling setSecurityMechanism()
    // on the ClientDataSource
    // If the security mechanism is not explicitly set as mentioned above, in that case
    // the Client will try to upgrade the security mechanism to a more secure one, if possible.
    // @see #getUpgradedSecurityMechanism
    // Therefore, need to keep track if the securityMechanism has been explicitly set 
    protected short securityMechanism = SECMEC_HAS_NOT_EXPLICITLY_SET;

    //  Default security mechanism is USER_ONLY_SECURITY.
    public final static short propertyDefault_securityMechanism =
                                    (short) NetConfiguration.SECMEC_USRIDONL;

    
    // We use the NET layer constants to avoid a mapping for the NET driver.
    /**
     * Return security mechanism if it is set, else upgrade the security mechanism
     * if possible and return the upgraded security mechanism
     * @param properties Look in the properties if securityMechanism is set or not
     * if set, return this security mechanism
     * @return security mechanism 
     */
    public static short getSecurityMechanism(Properties properties) {
        short secmec;
        String securityMechanismString =
            properties.getProperty(Attribute.CLIENT_SECURITY_MECHANISM);

        if ( securityMechanismString != null )
        {
            // security mechanism has been set, do not override, but instead return
            // the security mechanism that has been set (DERBY-962)
            secmec = Short.parseShort(securityMechanismString);
        }
        else
        {
            // if securityMechanismString is null, this means that 
            // security mechanism has not been set explicitly and not available in 
            // properties. Hence, do an upgrade of security mechanism if possible
            // The logic for upgrade of security mechanism uses information about 
            // if password is available or not, so pass this information also.
            String passwordString = properties.getProperty(Attribute.PASSWORD_ATTR);
            secmec = getUpgradedSecurityMechanism(passwordString);
        }
        return secmec;
    }

    /**
     * This method has logic to upgrade security mechanism to a better (more secure)
     * one if it is possible. Currently derby server only has support for USRIDPWD,
     * USRIDONL, EUSRIDPWD and USRSSBPWD (10.2+) - this method only considers these
     * possibilities. USRIDPWD, EUSRIDPWD and USRSSBPWD require a password, USRIDONL
     * is the only security mechanism which does not require password.
     * 1. if password is not available, then security mechanism possible is USRIDONL
     * 2. if password is available,then USRIDPWD is returned.
     *
     * @param password password argument 
     * @return upgraded security mechanism if possible
     */
    public static short getUpgradedSecurityMechanism(String password) {
        // if password is null, in that case the only acceptable security 
        // mechanism is USRIDONL, which is the default security mechanism. 
        if ( password == null )
            return propertyDefault_securityMechanism;

        // when we have support for more security mechanisms on server 
        // and client, we should update this upgrade logic to pick 
        // secure security mechanisms before trying out the USRIDPWD
        
        /*
        // -----------------------
        // PLEASE NOTE: 
        // When DERBY-1517, DERBY-1755 is fixed, there might be a way to use EUSRIDPWD 
        // when both client and server vm's have support for it. Hence the below
        // if statement is commented out.
        if (SUPPORTS_EUSRIDPWD)
            return (short)NetConfiguration.SECMEC_EUSRIDPWD;
        else 
            // IMPORTANT NOTE:
            // --------------
            // If DERBY-1517 can be fixed, we should default to
            // SECMEC_USRSSBPWD (strong password substitute).
            // Until then, connecting with a 10.2+ client to
            // a derby server < 10.2, and hence does not support
            // SECMEC_USRSSBPWD as a SECMEC, will cause a DRDA protocol
            // exception, as described in DERBY-926).
            // 
            // return (short)NetConfiguration.SECMEC_USRSSBPWD;
         // ----------------------
         */
         return (short)NetConfiguration.SECMEC_USRIDPWD;

    }

    // ---------------------------- getServerMessageTextOnGetMessage -----------------------------------
    //
    private boolean retrieveMessageText = propertyDefault_retrieveMessageText;
    public final static boolean propertyDefault_retrieveMessageText = true;

    public static boolean getRetrieveMessageText(Properties properties) {
        String retrieveMessageTextString = properties.getProperty(Attribute.CLIENT_RETIEVE_MESSAGE_TEXT);
        return parseBoolean(retrieveMessageTextString, propertyDefault_retrieveMessageText);
    }

    // ---------------------------- traceFile -----------------------------------
    //
    private String traceFile;
    
    public static String getTraceFile(Properties properties) {
        return properties.getProperty(Attribute.CLIENT_TRACE_FILE);
    }

    // ---------------------------- traceDirectory -----------------------------------
    // For the suffix of the trace file when traceDirectory is enabled.
    private transient int traceFileSuffixIndex_ = 0;
    //
    private String traceDirectory;
    
    /**
     * Check if derby.client.traceDirectory is provided as a JVM property. 
     * If yes, then we use that value. If not, then we look for traceDirectory 
     * in the the properties parameter.
     *
     * @param properties jdbc url properties 
     * @return value of traceDirectory property
     */
    public static String getTraceDirectory(Properties properties) {
    	String traceDirectoryString;
       
    	traceDirectoryString  = readSystemProperty(Attribute.CLIENT_JVM_PROPERTY_PREFIX+Attribute.CLIENT_TRACE_DIRECTORY);

		if (traceDirectoryString == null) 
			return properties.getProperty(Attribute.CLIENT_TRACE_DIRECTORY);
		else
			return traceDirectoryString;
    }
    
    
    /**
     * Read the value of the passed system property.
     * @param key name of the system property
     * @return value of the system property, null if there is no permission to read the property
     */
    private static String readSystemProperty(final String key) {
    	//Using an anonymous class to read the system privilege because the
    	//method java.security.AccessController.doPrivileged requires an 
    	//instance of a class(which implements java.security.PrivilegedAction). 
    	//Since readSystemProperty method is static, we can't simply pass "this"  
    	//to doPrivileged method and have ClientBaseDataSource implement 
    	//PrivilegedAction. To get around the static nature of method 
    	//readSystemProperty, have an anonymous class implement PrivilegeAction.
    	//This class will read the system property in it's run method and
    	//return the value to the caller.
    	return (String )AccessController.doPrivileged
    	    (new java.security.PrivilegedAction(){
    		    public Object run(){
                    try {
                        return System.getProperty(key);
                    } catch (SecurityException se) {
                        // We do not want the connection to fail if the user does not have permission to 
                        // read the property, so if a security exception occurs, just return null and 
                        // continue with the connection.  
                        return null;
                    }
    		    }
    	    }
    	    );
    }

    // ---------------------------- traceFileAppend -----------------------------------
    //
    private boolean traceFileAppend = propertyDefault_traceFileAppend;
    public final static boolean propertyDefault_traceFileAppend = false;
    
    public static boolean getTraceFileAppend(Properties properties) {
        String traceFileAppendString = properties.getProperty(Attribute.CLIENT_TRACE_APPEND);
        return parseBoolean(traceFileAppendString, propertyDefault_traceFileAppend);
    }

    // ---------------------------- password -----------------------------------
    //
    // The password property is defined in subclasses, but the method
    // getPassword (java.util.Properties properties) is in this class to eliminate
    // dependencies on j2ee for connections that go thru the driver manager.

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
                    fileName = traceDirectory + File.separator + logWriterInUseSuffix + "_" + traceFileSuffixIndex;
                } else {
                    fileName = traceDirectory + File.separator + traceFile + logWriterInUseSuffix + "_" + traceFileSuffixIndex;
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
                    throw new SqlException(null, 
                        new ClientMessageId(SQLState.INVALID_ATTRIBUTE_SYNTAX),
                        attributeString);
                }
                
                augmentedProperties.setProperty((v.substring(0, eqPos)).trim(), (v.substring(eqPos + 1)).trim());
            }
        } catch (NoSuchElementException e) {
            // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
            throw new SqlException(null, 
                new ClientMessageId(SQLState.INVALID_ATTRIBUTE_SYNTAX),
                attributeString, e);
        }
        checkBoolean(augmentedProperties, Attribute.CLIENT_RETIEVE_MESSAGE_TEXT);
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

        throw new SqlException(null, 
            new ClientMessageId(SQLState.INVALID_ATTRIBUTE),
            attribute, value, choicesStr);
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
     * Security mechanism options are: <ul>
     * <li> USER_ONLY_SECURITY
     * <li> CLEAR_TEXT_PASSWORD_SECURITY
     * <li> ENCRYPTED_PASSWORD_SECURITY
     * <li> ENCRYPTED_USER_AND_PASSWORD_SECURITY - both password and user are encrypted
     * <li> STRONG_PASSWORD_SUBSTITUTE_SECURITY
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
    public final static short STRONG_PASSWORD_SUBSTITUTE_SECURITY = (short) NetConfiguration.SECMEC_USRSSBPWD;

    /**
     * Sets the security mechanism.
     * @param securityMechanism to set
     */
    synchronized public void setSecurityMechanism(short securityMechanism) {
        this.securityMechanism = securityMechanism;
    }

    /**
     * Return the security mechanism.
     * If security mechanism has not been set explicitly on datasource,
     * then upgrade the security mechanism to a more secure one if possible.
     * @see #getUpgradedSecurityMechanism(String)
     * @return the security mechanism
     */
    public short getSecurityMechanism() {
        return getSecurityMechanism(getPassword());
    }

    /**
     * Return the security mechanism for this datasource object.
     * If security mechanism has not been set explicitly on datasource,
     * then upgrade the security mechanism to a more secure one if possible.
     * @param password  password of user
     * @see #getUpgradedSecurityMechanism(String)
     * @return the security mechanism
     */
    public short getSecurityMechanism(String password) {
        
        // if security mechanism has not been set explicitly on
        // datasource, then upgrade the security mechanism if possible
        // DERBY-962
        if ( securityMechanism == SECMEC_HAS_NOT_EXPLICITLY_SET )
            return getUpgradedSecurityMechanism(password);
        
        return securityMechanism;
    }

    // ----------------------- ssl

    private int sslMode;

    /**
     * Specifices the SSL encryption mode to use.
     * <p>
     * Valid values are <tt>off</tt>, <tt>basic</tt> and
     * <tt>peerAuthentication</tt>.
     *
     * @param mode the SSL mode to use (<tt>off</tt>, <tt>basic</tt> or
     *      <tt>peerAuthentication</tt>)
     * @throws SqlException if the specified mode is invalid
     */
    public void setSsl(String mode) 
        throws SqlException
    {
        sslMode = getSSLModeFromString(mode);
    }

    /**
     * Returns the SSL encryption mode specified for the data source.
     *
     * @return <tt>off</tt>, <tt>basic</tt> or <tt>peerAuthentication</tt>.
     */
    public String getSsl() {
        switch(sslMode) {
        case SSL_OFF: 
        default: 
            return SSL_OFF_STR;
        case SSL_BASIC: 
            return SSL_BASIC_STR;
        case SSL_PEER_AUTHENTICATION: 
            return SSL_PEER_AUTHENTICATION_STR;
        }
    }

    // ----------------------- set/getCreate/ShutdownDatabase ---------------------------
    /**
     * Set to true if the database should be created.
     */
    private boolean createDatabase;

    /**
     * Set to true if the database should be shutdown.
     */
    private boolean shutdownDatabase;
    
    /**
     * Set this property to create a new database.  If this property is not
     * set, the database (identified by databaseName) is assumed to be already
     * existing.
     * @param create if set to the string "create", this data source will try
     *               to create a new database of databaseName, or boot the 
     *               database if one by that name already exists.
     * 
     */
    public final void setCreateDatabase(String create) {
        if (create != null && create.equalsIgnoreCase("create"))
            this.createDatabase = true;
        else // reset
            this.createDatabase = false;
    }
    
    /** @return "create" if create is set, or null if not 
     */
    public final String getCreateDatabase() {
        String createstr=null;
        if (createDatabase)
            createstr="create";
        return createstr;
    }
    
    /**
     * Set this property if one wishes to shutdown the database identified by
     * databaseName. 
     * @param shutdown if set to the string "shutdown", this data source will 
     *                 shutdown the database if it is running.
     * 
     */
    public final void setShutdownDatabase(String shutdown) {
        if (shutdown != null && shutdown.equalsIgnoreCase("shutdown"))
            this.shutdownDatabase = true;
        else // reset
            this.shutdownDatabase = false;
    }

    /** @return "shutdown" if shutdown is set, or null if not 
     */
    public final String getShutdownDatabase() {
        String shutdownstr=null;
        if (shutdownDatabase)
        {
            shutdownstr = "shutdown"; 
        }           
        return shutdownstr;
    }

    protected String connectionAttributes = null;

    /**
     * Set this property to pass in more Derby specific connection URL attributes.
     * <BR>
     * Any attributes that can be set using a property of this DataSource implementation
     * (e.g user, password) should not be set in connectionAttributes. Conflicting
     * settings in connectionAttributes and properties of the DataSource will lead to
     * unexpected behaviour. 
     *
     * @param prop set to the list of Derby connection attributes separated by semi-colons.   E.g., to specify an
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

    protected int traceLevel = propertyDefault_traceLevel;

    /**
     * Check if derby.client.traceLevel is provided as a JVM property. 
     * If yes, then we use that value. If not, then we look for traceLevel 
     * in the the properties parameter.
     *
     * @param properties jdbc url properties 
     * @return value of traceLevel property
     */
    public static int getTraceLevel(Properties properties) {
    	String traceLevelString;
    	traceLevelString  = readSystemProperty(Attribute.CLIENT_JVM_PROPERTY_PREFIX+Attribute.CLIENT_TRACE_LEVEL);
		if (traceLevelString == null) 
			traceLevelString = properties.getProperty(Attribute.CLIENT_TRACE_LEVEL);
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

    /**
     * Returns the maximum number of JDBC prepared statements a connection is
     * allowed to cache.
     * <p>
     * A basic data source will always return zero. If statement caching is
     * required, use a {@link javax.sql.ConnectionPoolDataSource}.
     * <p>
     * This method is used internally by Derby to determine if statement
     * pooling is to be enabled or not.
     *
     * @return Maximum number of statements to cache, or <code>0</code> if
     *      caching is disabled (default).
     */
    public int maxStatementsToPool() {
        return 0;
    }

    // --- private helper methods


    /**
     * The dataSource keeps individual fields for the values that are relevant to the client. These need to be updated
     * when set connection attributes is called.
     */
    void updateDataSourceValues(Properties prop) 
        throws SqlException
    {
        if (prop == null) {
            return;
        }
        
        if (prop.containsKey(Attribute.USERNAME_ATTR)) {
            setUser(getUser(prop));
        }
        if (prop.containsKey(Attribute.CLIENT_SECURITY_MECHANISM)) {
            setSecurityMechanism(getSecurityMechanism(prop));
        }
        if (prop.containsKey(Attribute.CLIENT_TRACE_FILE)) {
            setTraceFile(getTraceFile(prop));
        }
        if (prop.containsKey(Attribute.CLIENT_TRACE_DIRECTORY)) {
            setTraceDirectory(getTraceDirectory(prop));
        }
        if (prop.containsKey(Attribute.CLIENT_TRACE_APPEND)) {
            setTraceFileAppend(getTraceFileAppend(prop));
        }
        if (prop.containsKey(Attribute.CLIENT_RETIEVE_MESSAGE_TEXT)) {
            setRetrieveMessageText(getRetrieveMessageText(prop));
        }
        if (prop.containsKey(Attribute.SSL_ATTR)) {
            sslMode = getClientSSLMode(prop);
        }
    }

    /**
     * Handles common error situations that can happen when trying to
     * obtain a physical connection to the server, and which require special
     * handling.
     * <p>
     * If this method returns normally, the exception wasn't handled and should
     * be handled elsewhere or be re-thrown.
     *
     * @param logWriter log writer, may be {@code null}
     * @param sqle exception to handle
     * @throws SQLException handled exception (if any)
     */
    protected final void handleConnectionException(LogWriter logWriter,
                                                   SqlException sqle)
            throws SQLException {
        // See DERBY-4070
        if (sqle.getSQLState().equals(
                ExceptionUtil.getSQLStateFromIdentifier(
                    SQLState.INVALID_ATTRIBUTE_SYNTAX))) {
            // Wrap this in SQLState.MALFORMED_URL exception to be
            // consistent with the embedded driver.
            throw new SqlException(logWriter,
                    new ClientMessageId(SQLState.MALFORMED_URL),
                    constructUrl(), sqle).getSQLException();

        }
    }

    /**
     * Constructs the JDBC connection URL from the state of the data source.
     *
     * @return The JDBC connection URL.
     */
    private String constructUrl() {
        StringBuffer sb = new StringBuffer(64);
        // To support subSubProtocols, the protocol addition below must be
        // changed.
        sb.append(Attribute.DNC_PROTOCOL);
        sb.append(serverName);
        sb.append(':');
        sb.append(portNumber);
        sb.append('/');
        sb.append(databaseName);
        if (connectionAttributes != null) {
            sb.append(';');
            sb.append(connectionAttributes);
        }
        return sb.toString();
    }
}
