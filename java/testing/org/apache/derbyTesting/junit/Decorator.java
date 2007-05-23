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

import java.sql.SQLException;
import java.util.Random;

import javax.sql.DataSource;

import junit.extensions.TestSetup;
import junit.framework.Test;

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
     * using encrpyption as a general test and
     * not specific tests of how encryption is handled.
     * E.g. tests of setting various URL attributes
     * would be handled in a specific test.
     * <BR>
     * The database will use the default encryption
     * algorithm.
     * <BR>
     * A boot password (phrase) is used with a random
     * set of characters and digits 16 characters long.
     * <BR>
     * The database is created during the setUp of the decorator.
     * 
     * @param test test to decorate
     * @return decorated tests
     */
    public static Test encryptedDatabase(Test test)
    {
        test = new BaseTestSetup(test) {
            
            /**
             * Create an encrypted database using a
             * JDBC data source.
             */
            protected void setUp() throws SQLException
            {
                DataSource ds = JDBCDataSource.getDataSource();
                               
                JDBCDataSource.setBeanProperty(ds,
                        "createDatabase", "create");
                JDBCDataSource.setBeanProperty(ds,
                        "connectionAttributes",
                        "dataEncryption=true;bootPassword=" + bootPhrase);
                
                ds.getConnection().close();
            }
        };
        
        return TestConfiguration.singleUseDatabaseDecorator(test);
    }
    
    /**
     * Decorate a set of tests to use an encrypted
     * single use database. This is to run tests
     * using encrpyption as a general test and
     * not specific tests of how encryption is handled.
     * E.g. tests of setting various URL attributes
     * would be handled in a specific test.
     * <BR>
     * The database will use the specified encryption
     * algorithm.
     * <BR>
     * A boot password (phrase) is used with a random
     * set of characters and digits 64 characters long.
     * <BR>
     * The database is created during the setUp of the decorator.

     * 
     * @param test test to decorate
     * @return decorated tests
     */
    public static Test encryptedDatabase(Test test, final String algorithm)
    {
        test = new BaseTestSetup(test) {
            
            /**
             * Create an encrypted database using a
             * JDBC data source.
             */
            protected void setUp() throws SQLException
            {
                String bootPhrase = getBootPhrase(64);
                DataSource ds = JDBCDataSource.getDataSource();
                        
                JDBCDataSource.setBeanProperty(ds,
                        "createDatabase", "create");
                JDBCDataSource.setBeanProperty(ds,
                        "connectionAttributes",
                        "dataEncryption=true;bootPassword=" + bootPhrase +
                        ";encryptionAlgorithm=" + algorithm);
                
                ds.getConnection().close();
            }
        };
        
        return TestConfiguration.singleUseDatabaseDecorator(test);
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
}
