/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.ResultSet;

import java.util.Vector;

/**
	A pre-compiled activation that supports a single ResultSet with
	a single constant action. All the execution logic is contained
	in the constant action.

 */
public final class ConstantActionActivation extends BaseActivation
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

	public int getExecutionCount() { return 0;}
	public void setExecutionCount(int count) {}

	public Vector getRowCountCheckVector() {return null;}
	public void setRowCountCheckVector(Vector v) {}

	public int getStalePlanCheckInterval() { return Integer.MAX_VALUE; }
	public void setStalePlanCheckInterval(int count) {}

	public ResultSet execute() throws StandardException {

		throwIfClosed("execute");
		startExecution();

		if (resultSet == null)
			resultSet = getResultSetFactory().getDDLResultSet(this);
		return resultSet;
	}
	public void postConstructor(){}
}
