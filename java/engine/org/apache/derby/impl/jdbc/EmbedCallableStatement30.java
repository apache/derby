/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;

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











