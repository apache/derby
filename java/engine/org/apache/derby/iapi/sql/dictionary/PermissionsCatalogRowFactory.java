/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.PermissionsCatalogRowFactory

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
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.StringDataValue;

public abstract class PermissionsCatalogRowFactory extends CatalogRowFactory
{
    public static final String AUTHORIZATION_ID_TYPE = "VARCHAR";
    public static final boolean AUTHORIZATION_ID_IS_BUILTIN_TYPE = true;
    public static final int AUTHORIZATION_ID_LENGTH = Limits.MAX_IDENTIFIER_LENGTH;

    public PermissionsCatalogRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                        boolean convertIdToLower)
    {
        super(uuidf,ef,dvf,convertIdToLower);
    }

    protected DataValueDescriptor getAuthorizationID( String value)
    {
        return getDataValueFactory().getVarcharDataValue( value);
    }

    protected DataValueDescriptor getNullAuthorizationID()
    {
        return getDataValueFactory().getNullVarchar( (StringDataValue) null);
    }

    /**
     * Extract an internal authorization ID from a row.
     *
     * @param row
     * @param columnPos 1 based
     *
     * @return The internal authorization ID
     */
    protected String getAuthorizationID( ExecRow row, int columnPos)
        throws StandardException
    {
        return row.getColumn( columnPos).getString();
    }
    
    /**
     * @return the index number of the primary key index.
     */
    public abstract int getPrimaryIndexNumber();

    /**
     * Build an index key row from a permission descriptor. A key row does not include the RowLocation column.
     *
     * @param indexNumber
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     *
     * @exception StandardException standard error policy
     */
    public abstract ExecIndexRow buildIndexKeyRow( int indexNumber,
                                                   PermissionsDescriptor perm)
        throws StandardException;

    /**
     * Or a set of permissions in with a row from this catalog table
     *
     * @param row an existing row
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     *
     * @return The number of columns that were changed.
     *
     * @exception StandardException standard error policy
     */
    abstract public int orPermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException;

    /**
     * Remove a set of permissions from a row from this catalog table
     *
     * @param row an existing row
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     *
     * @return -1 if there are no permissions left in the row, otherwise the number of columns that were changed.
     *
     * @exception StandardException standard error policy
     */
    abstract public int removePermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException;

    /**
     * Set the uuid of the passed permission descriptor to the uuid of the row
     * from the system table. DataDictionary will make this call before calling 
     * the dependency manager to send invalidation messages to the objects 
     * dependent on the permission descriptor's uuid.
     * 
     * @param row The row from the system table for the passed permission descriptor
     * @param perm Permission descriptor
     * @throws StandardException
     */
    abstract public void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm) throws StandardException;
}
