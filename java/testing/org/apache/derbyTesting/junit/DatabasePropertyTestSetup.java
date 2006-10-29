/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.DatabasePropertyTestSetup
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
     * @return
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
     * The password for each user is set to the user's name with 
     * the value of passwordToken appended.
     * <P>
     * Assumption is that no authentication was enabled upon entry.
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
            userProps.setProperty("derby.user." + user, user.concat(passwordToken));
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
        test = new ChangeUserSetup(test, users[0], users[0].concat(passwordToken));
        test = new DatabasePropertyTestSetup(test, userProps, false);
        
        return test;
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
            try {
                TestConfiguration.getCurrent().getDefaultConnection(
                        "shutdown=true");
                fail("Database failed to shut down");
            } catch (SQLException e) {
                 BaseJDBCTestCase.assertSQLState("Database shutdown", "08006", e);
            }
        }
    }

    /**
     * Revert the properties to their values prior to the setUp call.
     */
    protected void tearDown()
    throws java.lang.Exception
    {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        CallableStatement setDBP =  conn.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, NULL)");
    	// Clear all the system properties set by the new set
    	// that will not be reset by the old set. Ignore any 
        // invalid property values.
        try {
        	for (Enumeration e = newValues.propertyNames(); e.hasMoreElements();)
        	{
        		String key = (String) e.nextElement();
        		if (oldValues.getProperty(key) == null)
        		{
        			setDBP.setString(1, key);
        			setDBP.executeUpdate();
        		}
        	}
        } catch (SQLException sqle) {
        	if(!sqle.getSQLState().equals(SQLStateConstants.PROPERTY_UNSUPPORTED_CHANGE))
        		throw sqle;
        }
    	// and then reset nay old values which will cause the commit.
    	setProperties(oldValues);
        super.tearDown();
        newValues = null;
        oldValues = null;
        if (staticProperties) {
            try {
                TestConfiguration.getCurrent().getDefaultConnection(
                        "shutdown=true");
                fail("Database failed to shut down");
            } catch (SQLException e) {
                BaseJDBCTestCase.assertSQLState("Database shutdown", "08006", e);
            }
        }
    }
    
    private void setProperties(Properties values) throws SQLException
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
