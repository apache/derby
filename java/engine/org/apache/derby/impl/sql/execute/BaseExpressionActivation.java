/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.catalog.types.UserDefinedTypeIdImpl;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.Context;

import org.apache.derby.iapi.jdbc.ConnectionContext;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.reference.Attribute;

import org.apache.derby.iapi.sql.ResultSet;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * BaseExpressionActivation
 *
 * Support needed by Expression evaluators (Filters) and by
 * ResultSet materializers (Activations)
 */
public abstract class BaseExpressionActivation
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	
	//
	// constructors
	//
	BaseExpressionActivation()
	{
		super();
	}


	/**
	 * Get the minimum value of 4 input values.  If less than 4 values, input
	 * NULL.  If more than 4 input values, call this multiple times to
	 * accumulate results.  Also have judge's type as parameter to have a base
	 * upon which the comparison is based.  An example use is for code 
	 * generation in bug 3858.
	 * 
	 * @param v1		1st value
	 * @param v2		2nd value
	 * @param v3		3rd value
	 * @param v4		4th value
	 * @param judgeTypeFormatId		type format id of the judge
	 * @param judgeUserJDBCTypeId	JDBC type id if judge is user type;
	 *								-1 if not user type
	 *
	 * @return	The minimum value of the 4.
	 */
	public static DataValueDescriptor minValue(DataValueDescriptor v1,
											  DataValueDescriptor v2,
											  DataValueDescriptor v3,
											  DataValueDescriptor v4,
											  int judgeTypeFormatId,
											  int judgeUserJDBCTypeId)
										throws StandardException
	{
		DataValueDescriptor judge;
		if (judgeUserJDBCTypeId == -1)
			judge = (DataValueDescriptor) new TypeId(judgeTypeFormatId, null).getNull();
		else
			judge = (DataValueDescriptor) new TypeId(judgeTypeFormatId, new UserDefinedTypeIdImpl()).getNull();
			
		DataValueDescriptor minVal = v1;
		if (v2 != null && judge.lessThan(v2, minVal).equals(true))
			minVal = v2;
		if (v3 != null && judge.lessThan(v3, minVal).equals(true))
			minVal = v3;
		if (v4 != null && judge.lessThan(v4, minVal).equals(true))
			minVal = v4;
		return minVal;
	}


	/**
	 * Get the maximum value of 4 input values.  If less than 4 values, input
	 * NULL.  If more than 4 input values, call this multiple times to
	 * accumulate results.  Also have judge's type as parameter to have a base
	 * upon which the comparison is based.  An example use is for code 
	 * generation in bug 3858.
	 * 
	 * @param v1		1st value
	 * @param v2		2nd value
	 * @param v3		3rd value
	 * @param v4		4th value
	 * @param judgeTypeFormatId		type format id of the judge
	 * @param judgeUserJDBCTypeId	JDBC type id if judge is user type;
	 *								-1 if not user type
	 *
	 * @return	The maximum value of the 4.
	 */
	public static DataValueDescriptor maxValue(DataValueDescriptor v1,
											  DataValueDescriptor v2,
											  DataValueDescriptor v3,
											  DataValueDescriptor v4,
											  int judgeTypeFormatId,
											  int judgeUserJDBCTypeId)
										throws StandardException
	{
		DataValueDescriptor judge;
		if (judgeUserJDBCTypeId == -1)
			judge =  new TypeId(judgeTypeFormatId, null).getNull();
		else
			judge =  new TypeId(judgeTypeFormatId, new UserDefinedTypeIdImpl()).getNull();

		DataValueDescriptor maxVal = v1;
		if (v2 != null && judge.greaterThan(v2, maxVal).equals(true))
			maxVal = v2;
		if (v3 != null && judge.greaterThan(v3, maxVal).equals(true))
			maxVal = v3;
		if (v4 != null && judge.greaterThan(v4, maxVal).equals(true))
			maxVal = v4;
		return maxVal;
	}

}
