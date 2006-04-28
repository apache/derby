/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.SqlExceptionTest
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derbyTesting.functionTests.util.BaseTestCase;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.shared.common.reference.SQLState;
import java.sql.SQLException;
import java.io.IOException;

/**
 * This is used for testing the SqlException class.  This test can be added
 * to.  My itch right now is to verify that exception chaining is working
 * correctly.
 */

public class SqlExceptionTest extends BaseTestCase
{    
    public SqlExceptionTest(String name)
    {
        super(name);
    }
    
    /**
     * Makes sure exception chaining works correctly (DERBY-1117)
     */
    public void testChainedException() {
        IOException ioe = new IOException("Test exception");
        SqlException sqle = new SqlException(null,
            new ClientMessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION),
            ioe);
        SQLException javae = sqle.getSQLException();
        
        // The underlying SqlException is the first cause; the IOException
        // should be the second cause        
        assertEquals(sqle, javae.getCause());
        assertEquals(ioe, javae.getCause().getCause());
        assertNull(sqle.getNextException());
    }
    
    /**
     * Make sure a SQLException is chained as a nextSQLException()
     * rather than as a chained exception
     */
    public void testNextException() {
        SQLException nexte = new SQLException("test");
        SqlException sqle = new SqlException(null,
            new ClientMessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION),
            nexte);
        SQLException javae = sqle.getSQLException();
        
        assertEquals(sqle, javae.getCause());
        assertNull(javae.getCause().getCause());
        assertEquals(nexte, javae.getNextException());
        
        // Make sure exception chaining works with Derby's SqlException
        // just as well as java.sql.SQLException
        SqlException internalException = 
            new SqlException(null, 
                new ClientMessageId("08000"));
        
        javae = new SqlException(null, 
            new ClientMessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION),
            internalException).getSQLException();
        
        assertNotNull(javae.getNextException());
        assertEquals(javae.getNextException().getSQLState(), "08000");
    }
}
