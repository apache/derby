/*
 *
 * Derby - Class org.apache.derbyTesting.junit.DatabasePropertyTestSetup
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

/**
 * Test decorator to set a set of database properties on setUp
 * and restore them to the previous values on tearDown.
 *
 */
public class DatabasePropertyTestSetup extends BaseJDBCTestSetup {
	
	private Properties newValues;
	private Properties oldValues;
    private final boolean staticProperties;
    
    /**
     * Decorator to change the lock time outs.
     * If either time is less than zero then that property is
     * not modified by this decorator.
     * The change is implemented by an instanceof DatabasePropertyTestSetup
     * and thus is reset by the tearDown method.
     * 
     * @param test Test to decorate
     * @param deadlockTime Time in seconds for derby.locks.deadlockTimeout.
     * @param waitTime Time in seconds for derby.locks.waitTimeout
     */
    public static Test setLockTimeouts(Test test, int deadlockTime, int waitTime)
    {
        final Properties properties = new Properties();
        if (deadlockTime >= 0)
        {
            properties.setProperty("derby.locks.deadlockTimeout",
                Integer.toString(deadlockTime));
        }
        if (waitTime >= 0) {
            properties.setProperty("derby.locks.waitTimeout",
                Integer.toString(waitTime));
        }
        
        // No change, no point to the decorator.
        if (properties.isEmpty())
            return test;

        return new DatabasePropertyTestSetup(test, properties, true);
    }
    
    /**
     * Decorate a test so that the database has authentication enabled
     * using the BUILTIN provider and the set of users passed in.
     * The password for each user is set to the user's name as set
     * in the users array with the value of passwordToken appended.
     * <BR>
     * The user names in the users array are treated as SQL identifiers
     * since that is the defined behaviour for derby.user.username.
     * This means that the quoted identifiers can be passed, examples
     * are (users[i] is shown as the contents of the Java string) with
     * a password suffix of T63:
     * <UL>
     * <LI>users[i]=fred - normal user name FRED, password fredT63
     * <LI>users[i]=FRED - normal user name FRED, passeword FREDT63
     * <LI>users[i]="FRED" - normal user name FRED, passeword "FREDT63"
     * <LI>users[i]="fred" - normal user name fred, passeword "fredT63"
     * </UL>
     * Thus with a quoted identifier the password will include the quotes.
     * Note bug DERBY-3150 exists which means that the normalized user name
     * to password mapping does not exist, thus a connection request must be
     * made with the values passed in the users array, not any other form of the
     * name.
     * <BR>
     * The decorated test can use BaseJDBCTestCase.openUserConnection(String user)
     * method to simplify using authentication.
     * <P>
     * Assumption is that no authentication was enabled upon entry.
     * <P>
     * Current user is set to the first user in the list users[0].
     * <P>
     * The authentication is removed by the decorator's tearDown method.
     * @param test Test to be decorated.
     * @param users Set of users for authentication.
     * @return Decorated test.
     */
    public static Test builtinAuthentication(Test test, String[] users,
            String passwordToken)
    {
        final Properties userProps = new Properties();
        final Properties authProps = new Properties();
        
        authProps.setProperty("derby.connection.requireAuthentication", "true");
        authProps.setProperty("derby.authentication.provider", "BUILTIN");
        
        for (int i = 0; i < users.length; i++)
        {
            String user = users[i];
            userProps.setProperty("derby.user." + user,
                    TestConfiguration.getPassword(user, passwordToken));
        }
        
        // Need to setup the decorators carefully.
        // Need execution in this order:
        // 1) add user definitions (no authentication enabled)
        // 2) switch to a valid user
        // 3) enable authentication with database reboot
        // 4) disable authentication with database reboot
        // 5) switch back to previous user
        // 6) remove user defintions.
        //
        // Combining steps 1,3 does not work as no shutdown request
        // is possible for step 4 as no valid users would be defined!
        //
        // Note the decorators are executed in order from
        // outer (added last) to inner.
        
        test = new DatabasePropertyTestSetup(test, authProps, true);
        test = new ChangeUserSetup(test, users[0],
                TestConfiguration.getPassword(users[0], passwordToken),
                passwordToken);
        test = new DatabasePropertyTestSetup(test, userProps, false);
        
        return test;
    }
    
    /**
     * Decorate a test so that the database has authentication enabled
     * using the BUILTIN provider and the set of users passed in.
     * The password for each user is set to the user's name with
     * the value of passwordToken appended.
     * <BR>
     * The decorated test can use BaseJDBCTestCase.openUserConnection(String user)
     * method to simplify using authentication.
     * <P>
     * Assumption is that no authentication was enabled upon entry.
     * <P>
     * Current user is set to the first user in the list users[0].
     * <P>
     * In contrast to plain builtinAuthentication, here the
     * authentication nor users are *NOT* removed by the decorator's
     * tearDown method.
     * @param test Test to be decorated.
     * @param users Set of users for authentication.
     * @return Decorated test.
     */
    public static Test builtinAuthenticationNoTeardown(Test test, String[] users,
            String passwordToken)
    {
        final Properties userProps = new Properties();
        final Properties authProps = new Properties();

        authProps.setProperty("derby.connection.requireAuthentication", "true");
        authProps.setProperty("derby.authentication.provider", "BUILTIN");

        for (int i = 0; i < users.length; i++)
        {
            String user = users[i];
            userProps.setProperty("derby.user." + user,
                    TestConfiguration.getPassword(user, passwordToken));
        }

        test = getNoTeardownInstance(test, authProps, true);
        test = new ChangeUserSetup(test, users[0],
                TestConfiguration.getPassword(users[0], passwordToken),
                passwordToken);
        test = getNoTeardownInstance(test, userProps, false);
        return test;
    }

    static DatabasePropertyTestSetup getNoTeardownInstance(
        Test test, Properties p, boolean staticp)
    {
        return new DatabasePropertyTestSetup(test, p, staticp) {
                protected void tearDown()
                        throws java.lang.Exception {
                    // We don't want to reset the properties, but we should
                    // still clear the reference to the default connection to
                    // allow it to be garbage collected.
                    clearConnection();
                }
            };
    }

    /**
     * Decorate a test so that it sets a single database property
     * at setUp and resets it at tearDown. Shorthand for
     * using DatabasePropertyTestSetup when only a single property is needed.
     * Does not perform a reboot of the database.
     */
    public static Test singleProperty(Test test, String property, String value)
    {
        return singleProperty(test, property, value, false);
    }
    /**
     * Decorate a test so that it sets a single database property
     * at setUp and resets it at tearDown. Shorthand for
     * using DatabasePropertyTestSetup when only a single property is needed.
     * Optinally reboots the database.
     */
    public static Test singleProperty(Test test, String property, String value,
            boolean reboot)
    {
        final Properties properties = new Properties();
        properties.setProperty(property, value);

        return new DatabasePropertyTestSetup(test, properties, reboot);
    }    
	
	/**
	 * Create a test decorator that sets and restores the passed
	 * in properties. Assumption is that the contents of
	 * properties and values will not change during execution.
	 * @param test test to be decorated
	 * @param newValues properties to be set
	 */
	public DatabasePropertyTestSetup(Test test,
			Properties newValues)
	{
        this(test, newValues, false);
    }
    
    /**
     * Create a test decorator that sets and restores the passed
     * in properties. Assumption is that the contents of
     * properties and values will not change during execution.
     * @param test test to be decorated
     * @param newValues properties to be set
     * @param staticProperties True if database needs to be shutdown after
     * setting properties in setUp() and tearDown method().
     */
    public DatabasePropertyTestSetup(Test test,
            Properties newValues, boolean staticProperties)
    {
		super(test);
		this.newValues = newValues;
		this.oldValues = new Properties();
        this.staticProperties = staticProperties;
	}

	/**
	 * For each property store the current value and
	 * replace it with the new value, unless there is no change.
	 */
    protected void setUp()
    throws java.lang.Exception
    {
    	setProperties(newValues);
        if (staticProperties) {
            TestConfiguration.getCurrent().shutdownDatabase();
        }
    }

    /**
     * Revert the properties to their values prior to the setUp call.
     */
    protected void tearDown()
    throws java.lang.Exception
    {
        Connection conn = getConnection();
        try {
            clearProperties(conn);
        } catch (SQLException sqle) {
            // To try to prevent the error situation of DERBY-5686, which
            // cascades to many test failures, catch ERROR 25502, and if it occurs
            // try to gather some information, close the connection,
            // and retry the clearing of the properties on a new connection
            if (sqle.getSQLState().equals("25502")) {
                // firstly, check on the state of the connection when we
                // get this error
                System.out.println("Apparently this is a read-only connection in teardown()? Get some data:");
                System.out.println("conn.isClosed: " + conn.isClosed());
                System.out.println("conn.isReadOnly: " + conn.isReadOnly());
                System.out.println("conn.getHoldability: " + conn.getHoldability());
                System.out.println("conn.getTransactionIsolation: " + conn.getTransactionIsolation());
                System.out.println("conn.getAutoCommit: " + conn.getAutoCommit());
                // now try to close the connection, then try open a new one, 
                // and try to executeUpdate again.
                try {
                    conn.close();
                } catch (SQLException isqle) {
                    if (sqle.getSQLState()=="25001")
                    {
                        // the transaction is still active. let's commit what we have.
                        conn.commit();
                        conn.close();
                    } else {
                        System.out.println("close failed - see SQLState.");
                        throw sqle;
                    }
                }
                Connection conn2 = getConnection();
                // check if this second connection is read-only
                if (conn2.isReadOnly())
                {
                    System.out.println("Sorry, conn2 is also read-only, won't retry");
                    // give up
                    throw sqle;
                }
                else
                {   
                    // retry
                    System.out.println("retrying clearing the Properties");
                    clearProperties(conn2);
                }
            }
            else if(!sqle.getSQLState().equals(SQLStateConstants.PROPERTY_UNSUPPORTED_CHANGE))
        		throw sqle;
        }
    	// and then reset nay old values which will cause the commit.
    	setProperties(oldValues);
        super.tearDown();
        newValues = null;
        oldValues = null;
        if (staticProperties) {
            TestConfiguration.getCurrent().shutdownDatabase();
        }
    }
    
    private void clearProperties(Connection conn) throws SQLException
    {
        conn.setAutoCommit(false);
        CallableStatement setDBP =  conn.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, NULL)");
        // Clear all the system properties set by the new set
        // that will not be reset by the old set. Ignore any 
        // invalid property values.
        for (Enumeration e = newValues.propertyNames(); e.hasMoreElements();)
        {
            String key = (String) e.nextElement();
            if (oldValues.getProperty(key) == null)
            {
                setDBP.setString(1, key);
                setDBP.executeUpdate();
            }
        }
    }

    private void setProperties(Properties values) throws SQLException
    {
        Connection conn = getConnection();
        try {
            attemptSetProperties(values, conn);
        } catch (SQLException sqle) {
            // To try to prevent the error situation of DERBY-5686, which
            // cascades to many test failures, catch ERROR 25502, and if it occurs
            // try to gather some information, close the connection,
            // and retry the clearing of the properties on a new connection
            if (sqle.getSQLState().equals("25502")) {
                // firstly, check on the state of the connection when we
                // get this error
                System.out.println("Apparently this is a read-only connection? Get some data:");
                System.out.println("conn.isClosed: " + conn.isClosed());
                System.out.println("conn.isReadOnly: " + conn.isReadOnly());
                System.out.println("conn.getHoldability: " + conn.getHoldability());
                System.out.println("conn.getTransactionIsolation: " + conn.getTransactionIsolation());
                System.out.println("conn.getAutoCommit: " + conn.getAutoCommit());
                // now try to close the connection, then try open a new one, 
                // and try to executeUpdate again.
                try {
                    conn.close();
                } catch (SQLException isqle) {
                    if (sqle.getSQLState()=="25001")
                    {
                        // the transaction is still active. let's commit what we have.
                        conn.commit();
                        conn.close();
                    } else {
                        System.out.println("close failed - see SQLState.");
                        throw sqle;
                    }
                }
                Connection conn2 = getConnection();

                // check if this second connection is read-only
                if (conn2.isReadOnly())
                {
                    System.out.println("Sorry, conn2 is also read-only, won't retry");
                    // give up
                    throw sqle;
                }
                else
                {   
                    // retry
                    System.out.println("retrying to set the Properties");
                    attemptSetProperties(values, conn2);
                }
            }
        }
    }
    
    private void attemptSetProperties(Properties values, Connection coonn) throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        
        PreparedStatement getDBP =  conn.prepareStatement(
            "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(?)");
        CallableStatement setDBP =  conn.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        
        
    	for (Enumeration e = values.propertyNames(); e.hasMoreElements();)
    	{
    		final String key = (String) e.nextElement();
    		final String value = values.getProperty(key);
            
            getDBP.setString(1, key);
            ResultSet rs = getDBP.executeQuery();
            rs.next();
            String old = rs.getString(1);
            rs.close();
                		
    		boolean change;
    		if (old != null)
    		{
                // set, might need to be changed.
                change = !old.equals(value);
                
                // If we are not processing the oldValues
                // then store in the oldValues. Reference equality is ok here.
    			if (change && (values != oldValues))
    			   oldValues.setProperty(key, old);
    		}
    		else {
    			// notset, needs to be set
    			change = true;
    		}
    		
    		if (change) {
                setDBP.setString(1, key);
                setDBP.setString(2, value);
                setDBP.executeUpdate();
   		    }
    	}
        conn.commit();
        getDBP.close();
        setDBP.close();
        conn.close();
    }
}
