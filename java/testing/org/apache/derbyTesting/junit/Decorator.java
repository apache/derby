/*

   Derby - Class org.apache.derbyTesting.junit.Decorator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.junit;

import java.util.Properties;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Utility class that provides static methods to decorate tests.
 * Used as a central collection point for decorators than cannot
 * be simply expressed as a TestSetup class. Typically the
 * decorators will be collections of other decorators
 */
public class Decorator {

    private Decorator() {
        super();
    }

    /**
     * Decorate a set of tests to use an encrypted
     * single use database. This is to run tests
     * using encryption as a general test and
     * not specific tests of how encryption is handled.
     * E.g. tests of setting various URL attributes
     * would be handled in a specific test.
     * <BR>
     * The database will use the default encryption
     * algorithm.
     * <BR>
     * A boot password (phrase) is used with a random
     * set of characters and digits 16 characters long.
     * 
     * @param test test to decorate
     * @return decorated tests
     */
    public static Test encryptedDatabase(Test test)
    {
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("no encryption support");

        Properties attributes = new Properties();
        attributes.setProperty("dataEncryption", "true");
        attributes.setProperty("bootPassword", getBootPhrase(16));

        return attributesDatabase(attributes, test);
    }
    
    /**
     * Decorate a set of tests to use an encrypted
     * single use database. This is to run tests
     * using encryption as a general test and
     * not specific tests of how encryption is handled.
     * E.g. tests of setting various URL attributes
     * would be handled in a specific test.
     * <BR>
     * The database will use the specified encryption
     * algorithm.
     * <BR>
     * A boot password (phrase) is used with a random
     * set of characters and digits 64 characters long.
     * 
     * @param test test to decorate
     * @return decorated tests
     */
    public static Test encryptedDatabase(Test test, final String algorithm)
    {
        Properties attributes = new Properties();
        attributes.setProperty("dataEncryption", "true");
        attributes.setProperty("bootPassword", getBootPhrase(64));
        attributes.setProperty("encryptionAlgorithm", algorithm);

        return attributesDatabase(attributes, test);
    }
    
    private static String getBootPhrase(int length)
    {
        Random rand = new Random();
        
        char[] bp = new char[length];
        for (int i = 0; i < bp.length; ) {
            char c = (char) rand.nextInt();
            if (Character.isLetterOrDigit(c))
            {
                bp[i++] = c;
            }
        }
        
        return new String(bp);
    }
    
    /**
     * Decorate a set of tests to use an single
     * use database with TERRITORY_BASED collation
     * set to the passed in locale.
     * @param locale Locale used to set territory JDBC attribute. If null
     * then only collation=TERRITORY_BASED will be set.
     */
    public static Test territoryCollatedDatabase(Test test, final String locale)
    {
        Properties attributes = new Properties();
        attributes.setProperty("collation", "TERRITORY_BASED");
        
        if (locale != null)
            attributes.setProperty("territory", locale);
        
        return attributesDatabase(attributes, test);
    }
    
    /**
     * Decorate a set of tests to use an single
     * use database with TERRITORY_BASED:SECONDARY collation
     * set to the passed in locale.
     * @param locale Locale used to set territory JDBC attribute. If null
     * then only collation=TERRITORY_BASED:SECONDARY will be set.
     */
    public static Test territoryCollatedCaseInsensitiveDatabase(Test test, final String locale)
    {
        Properties attributes = new Properties();
        attributes.setProperty("collation", "TERRITORY_BASED:SECONDARY");

        if (locale != null)
            attributes.setProperty("territory", locale);

        return attributesDatabase(attributes, test);
    }

    /**
     * Decorate a test (or suite of tests) to use a single use database
     * as the default database with a specified set connection attributes.
     * 
     * @param attributes properties to set in the connection URL or in the
     * connectionAttributes of a data source when connecting to the database
     * @param test Test to decorate
     * @return Decorated test
     */
    private static Test attributesDatabase(
            final Properties attributes, Test test)
    {
        test = new ChangeConfigurationSetup(test) {
            TestConfiguration getNewConfiguration(TestConfiguration old) {
                return old.addConnectionAttributes(attributes);
            }
        };
        
        return TestConfiguration.singleUseDatabaseDecorator(test);
    }
}
