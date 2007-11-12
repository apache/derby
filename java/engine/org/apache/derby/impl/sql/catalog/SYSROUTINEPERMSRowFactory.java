/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSROUTINEPERMSRowFactory

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

import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoutinePermsDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

/**
 * Factory for creating a SYSROUTINEPERMS row.
 *
 */

public class SYSROUTINEPERMSRowFactory extends PermissionsCatalogRowFactory
{
	static final String TABLENAME_STRING = "SYSROUTINEPERMS";

    // Column numbers for the SYSROUTINEPERMS table. 1 based
    private static final int ROUTINEPERMSID_COL_NUM = 1;
    private static final int GRANTEE_COL_NUM = 2;
    private static final int GRANTOR_COL_NUM = 3;
    private static final int ALIASID_COL_NUM = 4;
    private static final int GRANTOPTION_COL_NUM = 5;
    private static final int COLUMN_COUNT = 5;

    static final int GRANTEE_ALIAS_GRANTOR_INDEX_NUM = 0;
    public static final int ROUTINEPERMSID_INDEX_NUM = 1;
    public static final int ALIASID_INDEX_NUM = 2;

	private static final int[][] indexColumnPositions = 
	{ 
		{ GRANTEE_COL_NUM, ALIASID_COL_NUM, GRANTOR_COL_NUM},
		{ ROUTINEPERMSID_COL_NUM },
		{ ALIASID_COL_NUM }
	};

    public static final int GRANTEE_COL_NUM_IN_GRANTEE_ALIAS_GRANTOR_INDEX = 1;

    private static final boolean[] indexUniqueness = { true, true, false };

    private	static final String[] uuids =
    {
        "2057c01b-0103-0e39-b8e7-00000010f010" // catalog UUID
		,"185e801c-0103-0e39-b8e7-00000010f010"	// heap UUID
		,"c065801d-0103-0e39-b8e7-00000010f010"	// index1
		,"40f70088-010c-4c2f-c8de-0000000f43a0" // index2
		,"08264012-010c-bc85-060d-000000109ab8" // index3
    };

    SYSROUTINEPERMSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo( COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexUniqueness, uuids);
	}

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException
	{
		UUID oid;
        String routinePermID = null;
        DataValueDescriptor grantee = null;
        DataValueDescriptor grantor = null;
        String routineID = null;
        
        if( td == null)
        {
            grantee = getNullAuthorizationID();
            grantor = getNullAuthorizationID();
        }
        else
        {
            RoutinePermsDescriptor rpd = (RoutinePermsDescriptor) td;
            oid = rpd.getUUID();
            if ( oid == null )
            {
				oid = getUUIDFactory().createUUID();
				rpd.setUUID(oid);
            }
            routinePermID = oid.toString();
            grantee = getAuthorizationID( rpd.getGrantee());
            grantor = getAuthorizationID( rpd.getGrantor());
            if( rpd.getRoutineUUID() != null)
                routineID = rpd.getRoutineUUID().toString();
        }
		ExecRow row = getExecutionFactory().getValueRow( COLUMN_COUNT);
		row.setColumn( ROUTINEPERMSID_COL_NUM, new SQLChar(routinePermID));
        row.setColumn( GRANTEE_COL_NUM, grantee);
        row.setColumn( GRANTOR_COL_NUM, grantor);
        row.setColumn( ALIASID_COL_NUM, new SQLChar(routineID));
        row.setColumn( GRANTOPTION_COL_NUM, new SQLChar("N"));
        return row;
    } // end of makeRow
            
	/** builds a tuple descriptor from a row */
	public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary	dataDictionary)
		throws StandardException
    {
        if( SanityManager.DEBUG)
            SanityManager.ASSERT( row.nColumns() == COLUMN_COUNT,
                                  "Wrong size row passed to SYSROUTINEPERMSRowFactory.buildDescriptor");

        String routinePermsUUIDString = row.getColumn(ROUTINEPERMSID_COL_NUM).getString();
        UUID routinePermsUUID = getUUIDFactory().recreateUUID(routinePermsUUIDString);
        String aliasUUIDString = row.getColumn( ALIASID_COL_NUM).getString();
        UUID aliasUUID = getUUIDFactory().recreateUUID(aliasUUIDString);

        RoutinePermsDescriptor routinePermsDesc =
	        new RoutinePermsDescriptor( dataDictionary,
                    getAuthorizationID( row, GRANTEE_COL_NUM),
                    getAuthorizationID( row, GRANTOR_COL_NUM),
                    aliasUUID);
        routinePermsDesc.setUUID(routinePermsUUID);
			return routinePermsDesc;
    } // end of buildDescriptor

	/** builds a column list for the catalog */
	public SystemColumn[] buildColumnList()
    {
         return new SystemColumn[] {
             SystemColumnImpl.getUUIDColumn("ROUTINEPERMSID", false),
             SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
             SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
             SystemColumnImpl.getUUIDColumn("ALIASID", false),
             SystemColumnImpl.getIndicatorColumn("GRANTOPTION")
         };
    }

	/**
	 * builds an index key row given for a given index number.
	 */
  	public ExecIndexRow buildIndexKeyRow( int indexNumber,
                                          PermissionsDescriptor perm) 
  		throws StandardException
    {
        ExecIndexRow row = null;
        
        switch( indexNumber)
        {
        case GRANTEE_ALIAS_GRANTOR_INDEX_NUM:
            // RESOLVE We do not support the FOR GRANT OPTION, so rougine permission rows are unique on the
            // grantee and alias UUID columns. The grantor column will always have the name of the owner of the
            // routine. So the index key, used for searching the index, only has grantee and alias UUID columns.
            // It does not have a grantor column.
            //
            // If we support FOR GRANT OPTION then there may be multiple routine permissions rows for a
            // (grantee, aliasID) combination. Since there is only one kind of routine permission (execute)
            // execute permission checking need not worry about multiple routine permission rows for a
            // (grantee, aliasID) combination, it only cares whether there are any. Grant and revoke must
            // look through multiple rows to see if the current user has grant/revoke permission and use
            // the full key in checking for the pre-existence of the permission being granted or revoked.
            row = getExecutionFactory().getIndexableRow( 2);
            row.setColumn(1, getAuthorizationID( perm.getGrantee()));
            String routineUUIDStr = ((RoutinePermsDescriptor) perm).getRoutineUUID().toString();
            row.setColumn(2, new SQLChar(routineUUIDStr));
            break;
        case ROUTINEPERMSID_INDEX_NUM:
            row = getExecutionFactory().getIndexableRow( 1);
            String routinePermsUUIDStr = perm.getObjectID().toString();
            row.setColumn(1, new SQLChar(routinePermsUUIDStr));
            break;
        case ALIASID_INDEX_NUM:
            row = getExecutionFactory().getIndexableRow( 1);
            routineUUIDStr = ((RoutinePermsDescriptor) perm).getRoutineUUID().toString();
            row.setColumn(1, new SQLChar(routineUUIDStr));
            break;
        }
        return row;
    } // end of buildIndexKeyRow
    
    public int getPrimaryKeyIndexNumber()
    {
        return GRANTEE_ALIAS_GRANTOR_INDEX_NUM;
    }

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
    public int orPermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException
    {
        // There is only one kind of routine permission: execute or not. So the row would not exist
        // unless execute permission is there.
        // This changes if we implement WITH GRANT OPTION.
        return 0;
    }

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
    public int removePermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException
    {
        return -1; // There is only one kind of routine privilege so delete the whole row.
    } // end of removePermissions
    
	/** 
	 * @see PermissionsCatalogRowFactory#setUUIDOfThePassedDescriptor
	 */
    public void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm)
    throws StandardException
    {
        DataValueDescriptor existingPermDVD = row.getColumn(ROUTINEPERMSID_COL_NUM);
        perm.setUUID(getUUIDFactory().recreateUUID(existingPermDVD.getString()));
    }
}
