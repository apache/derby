/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ClassLoadingTest

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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;

import junit.framework.Test;

import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.services.loader.ClassFactory;

/**
 * Test limitations on class loading.
 */
public class ClassLoadingTest extends BaseJDBCTestCase
{

    public ClassLoadingTest(String name)
    {
        super( name );
    }

    public static Test suite(){
        return TestConfiguration.embeddedSuite( ClassLoadingTest.class );
    }

    /**
     * Test that you can't use Derby's custom class loader to load arbitrary
     * class files. See DERBY-6654.
     **/
    public void test_01_6654()
        throws Exception
    {
        ByteArray               classBytes = new ByteArray( new byte[] { (byte) 1 } );
        Connection  conn = getConnection();
        LanguageConnectionContext lcc = ConstraintCharacteristicsTest.getLCC( conn );
        ClassFactory    classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
        String      className1 = "BadClassName";
        String      className2 = "bad.class.Name";

        vet6654( classFactory, className1, classBytes );
        vet6654( classFactory, className2, classBytes );
    }
    private void    vet6654( ClassFactory classFactory, String className, ByteArray classBytes )
        throws Exception
    {
        try {
            classFactory.loadGeneratedClass( className, classBytes );
            fail( "Should not have been able to load class " + className );
        }
        catch (IllegalArgumentException iae) { println( "Caught expected IllegalArgumentException" ); }
    }

}
