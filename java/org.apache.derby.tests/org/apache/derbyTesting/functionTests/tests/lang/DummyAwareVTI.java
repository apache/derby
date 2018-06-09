/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DummyAwareVTI

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

import java.sql.SQLException;

import org.apache.derby.vti.AwareVTI;
import org.apache.derby.vti.StringColumnVTI;
import org.apache.derby.vti.VTIContext;

/**
 * A VTI which reports its context
 */
public class DummyAwareVTI extends StringColumnVTI
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private VTIContext  _context;
    private int     _rowCount = 0;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  DummyAwareVTI()
    {
        super( new String[] { "SCHEMA_NAME", "VTI_NAME", "STATEMENT_TEXT" } );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // StringColumnVTI BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public boolean  next()
    {
        if ( _rowCount > 0 ) { return false; }

        _rowCount++;

        return true;
    }

    public  void    close() {}

    protected String getRawColumn(int columnNumber)
        throws java.sql.SQLException
    {
        switch( columnNumber )
        {
        case 1: return getContext().vtiSchema();
        case 2: return getContext().vtiTable();
        case 3: return getContext().statementText();

        default: throw new SQLException( "Illegal columnNumber " + columnNumber );
        }
    }

}
