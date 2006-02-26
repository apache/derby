/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementColumnPermission

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * This class describes a column permission used (required) by a statement.
 */

public class StatementColumnPermission extends StatementTablePermission
{
	private FormatableBitSet columns;

	/**
	 * Constructor for StatementColumnPermission. Creates an instance of column permission requested
	 * for the given access.
	 * 
	 * @param tableUUID	UUID of the table
	 * @param privType	Access privilege requested
	 * @param columns	List of columns
	 *
	 */
	public StatementColumnPermission(UUID tableUUID, int privType, FormatableBitSet columns)
	{
		super( tableUUID, privType);
		this.columns = columns;
	}

	/**
	 * Return list of columns that need access
	 *
	 * @return	FormatableBitSet of columns
	 */
	public FormatableBitSet getColumns()
	{
		return columns;
	}

	/**
	 * Method to check if another instance of column access descriptor matches this.
	 * Used to ensure only one access descriptor for a table/columns of given privilege is created.
	 *
	 * @param obj	Another instance of StatementPermission
	 *
	 * @return	true if match
	 */
	public boolean equals( Object obj)
	{
		if( obj instanceof StatementColumnPermission)
		{
			StatementColumnPermission other = (StatementColumnPermission) obj;
			if( ! columns.equals( other.columns))
				return false;
			return super.equals( obj);
		}
		return false;
	}
	
	/**
	 * @param tc the TransactionController
	 * @param dd A DataDictionary
	 * @param authorizationId A user
	 * @param forGrant
	 *
	 * @exception StandardException if the permission has not been granted
	 */
	public void check(TransactionController tc,
					   DataDictionary dd,
					   String authorizationId,
					   boolean forGrant)
		throws StandardException
	{
		if( hasPermissionOnTable(dd, authorizationId, forGrant))
			return;
		FormatableBitSet permittedColumns = null;
		FormatableBitSet grantablePermittedColumns = null;
		FormatableBitSet publicPermittedColumns = null;
		FormatableBitSet publicPrantablePermittedColumns = null;
		if( ! forGrant)
		{
			permittedColumns = addPermittedColumns( dd,
													false /* non-grantable permissions */,
													Authorizer.PUBLIC_AUTHORIZATION_ID,
													permittedColumns);
			permittedColumns = addPermittedColumns( dd,
													false /* non-grantable permissions */,
													authorizationId,
													permittedColumns);
		}
		permittedColumns = addPermittedColumns( dd,
												true /* grantable permissions */,
												Authorizer.PUBLIC_AUTHORIZATION_ID,
												permittedColumns);
		permittedColumns = addPermittedColumns( dd,
												true /* grantable permissions */,
												authorizationId,
												permittedColumns);
												
		for( int i = columns.anySetBit(); i >= 0; i = columns.anySetBit( i))
		{
			if( permittedColumns != null && permittedColumns.get(i))
				continue;

			// No permission on this column.
			TableDescriptor td = getTableDescriptor( dd);
			ColumnDescriptor cd = td.getColumnDescriptor( i + 1);
			if( cd == null)
				throw StandardException.newException( SQLState.AUTH_INTERNAL_BAD_UUID, "column");
			throw StandardException.newException( forGrant ? SQLState.AUTH_NO_COLUMN_PERMISSION_FOR_GRANT
												  : SQLState.AUTH_NO_COLUMN_PERMISSION,
												  authorizationId,
												  getPrivName(),
												  cd.getColumnName(),
												  td.getSchemaName(),
												  td.getName());
		}
	} // end of check

	/**
	 * Add one user's set of permitted columns to a list of permitted columns.
	 */
	private FormatableBitSet addPermittedColumns( DataDictionary dd,
												  boolean forGrant,
												  String authorizationId,
												  FormatableBitSet permittedColumns)
		throws StandardException
	{
		if( permittedColumns != null && permittedColumns.getNumBitsSet() == permittedColumns.size())
			return permittedColumns;
		ColPermsDescriptor perms = dd.getColumnPermissions( tableUUID, privType, false, authorizationId);
		if( perms != null)
		{
			if( permittedColumns == null)
				return perms.getColumns();
			permittedColumns.or( perms.getColumns());
		}
		return permittedColumns;
	} // end of addPermittedColumns
}
