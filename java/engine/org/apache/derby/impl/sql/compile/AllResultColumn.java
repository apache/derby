/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

/**
 * An AllResultColumn represents a "*" result column in a SELECT
 * statement.  It gets replaced with the appropriate set of columns
 * at bind time.
 *
 * @author Jerry Brenner
 */

public class AllResultColumn extends ResultColumn
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	private TableName		tableName;

	/**
	 * This initializer is for use in the parser for a "*".
	 * 
	 * @param tableName	Dot expression qualifying "*"
	 *
	 * @return	The newly constructed AllResultColumn
	 */
	public void init(Object tableName)
	{
		this.tableName = (TableName) tableName;
	}

	/** 
	 * Return the full table name qualification for this node
	 *
	 * @return Full table name qualification as a String
	 */
	public String getFullTableName()
	{
		if (tableName == null)
		{
			return null;
		}
		else
		{
			return tableName.getFullTableName();
		}
	}

	/**
	 * Make a copy of this ResultColumn in a new ResultColumn
	 *
	 * @return	A new ResultColumn with the same contents as this one
	 *
	 * @exception StandardException		Thrown on error
	 */
	ResultColumn cloneMe() throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(columnDescriptor == null,
					"columnDescriptor is expected to be non-null");
		}

		return (ResultColumn) getNodeFactory().getNode(
									C_NodeTypes.ALL_RESULT_COLUMN,
									tableName,
									getContextManager());
	}
}
