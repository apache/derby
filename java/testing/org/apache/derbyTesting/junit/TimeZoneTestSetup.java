/*
 * Derby - Class org.apache.derbyTesting.functionTests.util.TimeZoneTestSetup
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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.TimeZone;
import junit.framework.Test;

/**
 * Decorator that changes the default timezone of the runtime environment
 * for the duration of the test.
 */
public class TimeZoneTestSetup extends BaseTestSetup {
    /** Original timezone. */
    private TimeZone savedDefault;
    /** The timezone to use as default while running the test. */
    private TimeZone requestedDefault;

    /**
     * Wrap a test in a decorator that changes the default timezone.
     * @param test the test to decorate
     * @param timeZoneID the ID of the timezone to use
     */
    public TimeZoneTestSetup(Test test, String timeZoneID) {
        this(test, TimeZone.getTimeZone(timeZoneID));
    }

    /**
     * Wrap a test in a decorator that changes the default timezone.
     * @param test the test to decorate
     * @param zone the timezone to use
     */
    public TimeZoneTestSetup(Test test, TimeZone zone) {
        super(test);
        this.requestedDefault = zone;
    }

    /**
     * Set the timezone.
     */
    protected void setUp() {
        savedDefault = TimeZone.getDefault();
        setDefault(requestedDefault);
    }

    /**
     * Reset the timezone.
     */
    protected void tearDown() {
        setDefault(savedDefault);
        savedDefault = null;
        requestedDefault = null;
    }
    
    private void setDefault(final TimeZone tz) throws SecurityException{
        if (tz== null) {
            throw new IllegalArgumentException("tz cannot be <null>");
        }
        AccessController.doPrivileged(
                new PrivilegedAction() {
                    public Object run() throws SecurityException {
                        TimeZone.setDefault(tz);
                        return null;
                    }});
    }
}
