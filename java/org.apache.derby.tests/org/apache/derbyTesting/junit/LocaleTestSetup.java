/*
 *
 * Derby - Class org.apache.derbyTesting.junit.LocaleTestSetup
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
import java.util.Locale;

import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * This decorator allows the usage of different locales on the tests
 */
public class LocaleTestSetup extends TestSetup {
	private Locale oldLocale;
	private Locale newLocale;
	
	public LocaleTestSetup(Test test, Locale newLocale) {
		super(test);
		
		oldLocale = Locale.getDefault();
		this.newLocale = newLocale;
	}
	
	/**
	 * Set up the new locale for the test
	 */
	protected void setUp() {
        setDefaultLocale(newLocale);
	}
	
	/**
	 * Revert the locale back to the old one
	 */
	protected void tearDown() {
        setDefaultLocale(oldLocale);
	}

    public static void setDefaultLocale(final Locale locale) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                Locale.setDefault(locale);
                return null;
            }
        });
    }
}
