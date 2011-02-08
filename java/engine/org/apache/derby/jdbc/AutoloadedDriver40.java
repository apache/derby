/*

   Derby - Class org.apache.derby.jdbc.AutoloadedDriver40

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

package org.apache.derby.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import org.apache.derby.impl.jdbc.SQLExceptionFactory40;
import org.apache.derby.impl.jdbc.Util;

/**
 * Adds driver functionality which is only visible from JDBC 4.0 onward.
 */
public class AutoloadedDriver40 extends AutoloadedDriver
{
	static
	{
        registerMe( new AutoloadedDriver40() );
        Util.setExceptionFactory (new SQLExceptionFactory40 ());
	}

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  Logger getParentLogger()
        throws SQLFeatureNotSupportedException
    {
        throw (SQLFeatureNotSupportedException) Util.notImplemented( "getParentLogger()" );
    }
}


