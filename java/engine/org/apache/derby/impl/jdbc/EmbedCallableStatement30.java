/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedCallableStatement30

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.jdbc;

import java.math.BigDecimal;

import java.sql.ParameterMetaData;
import java.sql.SQLException;


import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.EmbedConnection;


/**
 * This class extends the EmbedCallableStatement class from Local20
 * in order to support new methods and classes that come with JDBC 3.0.
 *
 * @see org.apache.derby.impl.jdbc.EmbedCallableStatement
 *
 */
public class EmbedCallableStatement30 extends EmbedCallableStatement20
{

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
	public EmbedCallableStatement30 (EmbedConnection conn, String sql,
								   int resultSetType,
								   int resultSetConcurrency,
								   int resultSetHoldability)
		throws SQLException
	{
		super(conn, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	/*
	 * Note: all the JDBC 3.0 Prepared statement methods are duplicated
	 * in here because this class inherits from Local20/EmbedCallableStatement, which
	 * inherits from Local/EmbedCallableStatement.  This class should inherit from a
	 * local30/PreparedStatement.  Since java does not allow multiple inheritance,
	 * duplicate the code here.
	 */

	/**
    * JDBC 3.0
    *
    * Retrieves the number, types and properties of this PreparedStatement
    * object's parameters.
    *
    * @return a ParameterMetaData object that contains information about the
    * number, types and properties of this PreparedStatement object's parameters.
    * @exception SQLException if a database access error occurs
	*/
	public ParameterMetaData getParameterMetaData()
    throws SQLException
	{
		checkStatus();
		if (preparedStatement == null)
			return null;
		
		return new EmbedParameterMetaData30(
				getParms(), preparedStatement.getParameterTypes());
	}

}











