/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.SystemPropertyTestSetup
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

import java.security.PrivilegedActionException;
import java.util.Enumeration;
import java.util.Properties;


import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * Test decorator to set a set of system properties on setUp
 * and restore them to the previous values on tearDown.
 *
 */
public class SystemPropertyTestSetup extends TestSetup {
	
	protected Properties newValues;
	private Properties oldValues;
	private boolean staticProperties;
	
	/**
	 * Create a test decorator that sets and restores the passed
	 * in properties. Assumption is that the contents of
	 * properties and values will not change during execution.
	 * @param test test to be decorated
	 * @param newValues properties to be set
	 */
	public SystemPropertyTestSetup(Test test,
//IC see: https://issues.apache.org/jira/browse/DERBY-1764
			Properties newValues,
			boolean staticProperties)
	{
		super(test);
		this.newValues = newValues;
		this.staticProperties = staticProperties;
	}

	/**
	 * Create a test decorator that sets and restores 
	 * System properties.  Do not shutdown engine after
	 * setting properties
	 * @param test
	 * @param newValues
	 */
	public SystemPropertyTestSetup(Test test,
			Properties newValues)
	{
		super(test);
		this.newValues = newValues;
		this.staticProperties = false;
	}

    /**
     * Decorate a test so that it sets a single system property in
     * {@code setUp()} and resets it in {@code tearDown()}. The engine is
     * not shut down after the property is set.
     */
    public static Test singleProperty(Test test, String property, String value)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6233
        return singleProperty(test, property, value, false);
    }

    /**
     * Decorate a test so that it sets a single system property in
     * {@code setUp()} and resets it in {@code tearDown()}. The engine is
     * shut down after the property is set if {@code staticProperty} is
     * {@code true}.
     */
    public static Test singleProperty(Test test, String property, String value,
            boolean staticProperty)
    {
        Properties properties = new Properties();
        properties.setProperty(property, value);
        return new SystemPropertyTestSetup(test, properties, staticProperty);
    }

	/**
	 * For each property store the current value and
	 * replace it with the new value, unless there is no change.
	 */
    protected void setUp()
    throws java.lang.Exception
    {
    	//DERBY-5663 Getting NPE when trying to set 
    	// derby.language.logStatementText property to true inside a junit 
    	// suite.
    	//The same instance of SystemPropertyTestSetup can be used again
    	// and hence we want to make sure that oldValues is not null as set
    	// in the tearDown() method. If we leave it null, we will run into NPE
    	// during the tearDown of SystemPropertyTestSetup during the 
    	// decorator's reuse.
		this.oldValues = new Properties();

        // Shutdown engine so static properties take effect.
        // Shutdown the engine before setting the properties. This
        // is because the properties may change authentication settings
        // to NATIVE authentication and we may be missing a credentials DB.
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (staticProperties) {
            // Use deregister == false here lest in client server mode
            // we try to connect to a network server without an embedded
            // driver registered. Issue seen with ConstraintCharacteristicsTest.
            TestConfiguration.getCurrent().shutdownEngine(false);
        }
        
    	setProperties(newValues);
    }

    /**
     * Revert the properties to their values prior to the setUp call.
     */
    protected void tearDown()
    throws java.lang.Exception
    {
        // Shut down the engine to restore any static properties. Do that
        // before the properties are reset to their old values, since the
        // engine shutdown may rely on some of the system properties. For
        // example, the system properties could contain the user database
        // (in derby.user.* style properties), and clearing those first
        // would lead to "invalid authentication" errors when attempting
        // to shut down the engine.
        try {
            if (staticProperties) {
                TestConfiguration.getCurrent().shutdownEngine();
            }
        } finally {
            restoreOldPropertyValues();
            oldValues = null;
        }
    }

    private void restoreOldPropertyValues() throws Exception {
    	// Clear all the system properties set by the new set
    	// that will not be reset by the old set.
       	for (Enumeration e = newValues.propertyNames(); e.hasMoreElements();)
       	{
       		String key = (String) e.nextElement();
       		if (oldValues.getProperty(key) == null)
       		    BaseTestCase.removeSystemProperty(key);
       	}
    	// and then reset nay old values
    	setProperties(oldValues);
    }

    private void setProperties(Properties values)
        throws PrivilegedActionException
    {
    	for (Enumeration e = values.propertyNames(); e.hasMoreElements();)
    	{
    		String key = (String) e.nextElement();
    		String value = values.getProperty(key);
    		String old = BaseTestCase.getSystemProperty(key);
    		
    		boolean change;
    		if (old != null)
    		{
                // set, might need to be changed.
                change = !old.equals(value);
                
                //Reference equality is ok here.
//IC see: https://issues.apache.org/jira/browse/DERBY-5342
    			if (values != oldValues)
    			   oldValues.setProperty(key, old);
    		}
    		else {
    			// notset, needs to be set
    			change = true;
    		}
    		
    		if (change) {
    			BaseTestCase.setSystemProperty(key, value);
    		}
    	}
    }
}
