/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.PermissionsCatalogRowFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.StringDataValue;

abstract class PermissionsCatalogRowFactory extends CatalogRowFactory
{
    PermissionsCatalogRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
    {
        super(uuidf,ef,dvf);
    }

    DataValueDescriptor getAuthorizationID( String value)
    {
        return new SQLVarchar(value);
    }

    DataValueDescriptor getNullAuthorizationID()
    {
        return new SQLVarchar();
    }

    /**
     * Extract an internal authorization ID from a row.
     *
     * @param row
     * @param columnPos 1 based
     *
     * @return The internal authorization ID
     */
    String getAuthorizationID( ExecRow row, int columnPos)
        throws StandardException
    {
        return row.getColumn( columnPos).getString();
    }

    /**
     * Build an index key row from a permission descriptor. A key row does not include the RowLocation column.
     *
     * @param indexNumber
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     *
     * @exception StandardException standard error policy
     */
    abstract ExecIndexRow buildIndexKeyRow( int indexNumber,
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
    abstract int orPermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
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
    abstract int removePermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
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
    abstract void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm) throws StandardException;
}
