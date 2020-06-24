/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSPERMSRowFactory

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.dictionary.PermissionsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.PermDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.shared.common.sanity.SanityManager;

import java.sql.Types;

/**
 * Factory for creating a SYSPERMS row.
 */

public class SYSPERMSRowFactory extends PermissionsCatalogRowFactory {
    private static final String TABLENAME_STRING = "SYSPERMS";

    private static final int SYSPERMS_COLUMN_COUNT = 7;
    /* Column #s for sysinfo (1 based) */
    private static final int SYSPERMS_PERMISSIONID = 1;
    private static final int SYSPERMS_OBJECTTYPE = 2;
    private static final int SYSPERMS_OBJECTID = 3;
    private static final int SYSPERMS_PERMISSION = 4;
    private static final int SYSPERMS_GRANTOR = 5;
    private static final int SYSPERMS_GRANTEE = 6;
    private static final int SYSPERMS_IS_GRANTABLE = 7;

    private static final int[][] indexColumnPositions =
            {
                    {SYSPERMS_PERMISSIONID},
                    {SYSPERMS_OBJECTID},
                    {SYSPERMS_GRANTEE, SYSPERMS_OBJECTID, SYSPERMS_GRANTOR},
            };

    // index numbers
    public static final int PERMS_UUID_IDX_NUM = 0;
    public static final int PERMS_OBJECTID_IDX_NUM = 1;
    public static final int GRANTEE_OBJECTID_GRANTOR_INDEX_NUM = 2;

    private static final boolean[] uniqueness = { true, false, true };

    private static final String[] uuids = {
            "9810800c-0121-c5e1-a2f5-00000043e718", // catalog UUID
            "6ea6ffac-0121-c5e3-f286-00000043e718", // heap UUID
            "5cc556fc-0121-c5e6-4e43-00000043e718",  // PERMS_UUID_IDX_NUM
            "7a92cf84-0122-51e6-2c5e-00000047b548",   // PERMS_OBJECTID_IDX_NUM
            "9810800c-0125-8de5-3aa0-0000001999e8",   // GRANTEE_OBJECTID_GRANTOR_INDEX_NUM
    };



    /**
     * Constructor
     *
     * @param uuidf UUIDFactory
     * @param ef    ExecutionFactory
     * @param dvf   DataValueFactory
     */
    SYSPERMSRowFactory(UUIDFactory uuidf,
                       ExecutionFactory ef,
                       DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSPERMS_COLUMN_COUNT, TABLENAME_STRING,
                indexColumnPositions, uniqueness, uuids);
    }

    /**
     * builds an index key row given for a given index number.
     */
    public ExecIndexRow buildIndexKeyRow(int indexNumber,
                                         PermissionsDescriptor perm)
            throws StandardException {
        ExecIndexRow row = null;

        switch (indexNumber) {
        case GRANTEE_OBJECTID_GRANTOR_INDEX_NUM:
            // RESOLVE We do not support the FOR GRANT OPTION, so generic permission rows are unique on the
            // grantee and object UUID columns. The grantor column will always have the name of the owner of the
            // object. So the index key, used for searching the index, only has grantee and object UUID columns.
            // It does not have a grantor column.
            row = getExecutionFactory().getIndexableRow( 2 );
            row.setColumn(1, getAuthorizationID( perm.getGrantee()));
            String protectedObjectsIDStr = ((PermDescriptor) perm).getPermObjectId().toString();
            row.setColumn(2, new SQLChar(protectedObjectsIDStr));
            break;

        case PERMS_UUID_IDX_NUM:
                row = getExecutionFactory().getIndexableRow(1);
                String permUUIDStr = ((PermDescriptor) perm).getUUID().toString();
                row.setColumn(1, new SQLChar(permUUIDStr));
                break;
        }
        return row;
    } // end of buildIndexKeyRow

    public int getPrimaryKeyIndexNumber()
    {
        return GRANTEE_OBJECTID_GRANTOR_INDEX_NUM;
    }

    /**
     * Or a set of permissions in with a row from this catalog table
     *
     * @param row         an existing row
     * @param perm        a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     * @return The number of columns that were changed.
     * @throws StandardException standard error policy
     */
    public int orPermissions(ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
            throws StandardException {
        return 0;
    }

    /**
     * Remove a set of permissions from a row from this catalog table
     *
     * @param row         an existing row
     * @param perm        a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     * @return -1 if there are no permissions left in the row, otherwise the number of columns that were changed.
     * @throws StandardException standard error policy
     */
    public int removePermissions(ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
            throws StandardException {
        return -1; // There is only one kind of privilege per row so delete the whole row.
    } // end of removePermissions

	/** 
	 * @see PermissionsCatalogRowFactory#setUUIDOfThePassedDescriptor
	 */
    void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm) throws StandardException
    {
        DataValueDescriptor existingPermDVD = row.getColumn(SYSPERMS_PERMISSIONID);
        perm.setUUID(getUUIDFactory().recreateUUID(existingPermDVD.getString()));
    }

    /**
     * Make a SYSPERMS row
     *
     * @param td     a permission descriptor
     * @param parent unused
     * @return Row suitable for inserting into SYSPERMS.
     * @throws org.apache.derby.shared.common.error.StandardException
     *          thrown on failure
     */
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
            throws StandardException {
        ExecRow row;
        String permIdString = null;
        String objectType = "SEQUENCE";
        String objectIdString = null;
        String permission = "USAGE";
        String grantor = null;
        String grantee = null;
        boolean grantable = false;


        if (td != null) {
            PermDescriptor sd = (PermDescriptor) td;
            UUID pid = sd.getUUID();
            if ( pid == null )
            {
				pid = getUUIDFactory().createUUID();
				sd.setUUID(pid);
            }
            permIdString = pid.toString();

            objectType = sd.getObjectType();

            UUID oid = sd.getPermObjectId();
            objectIdString = oid.toString();

            permission = sd.getPermission();
            grantor = sd.getGrantor();
            grantee = sd.getGrantee();
            grantable = sd.isGrantable();
        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSPERMS_COLUMN_COUNT);

        /* 1st column is UUID */
        row.setColumn(1, new SQLChar(permIdString));

        /* 2nd column is OBJECTTYPE */
        row.setColumn(2, new SQLVarchar(objectType));

        /* 3rd column is OBJECTID */
        row.setColumn(3, new SQLChar(objectIdString));

        /* 4nd column is OBJECTTYPE */
        row.setColumn(4, new SQLChar(permission));

        /* 5nd column is GRANTOR */
        row.setColumn(5, new SQLVarchar(grantor));

        /* 6nd column is GRANTEE */
        row.setColumn(6, new SQLVarchar(grantee));

        /* 7nd column is IS_GRANTABLE */
        row.setColumn(7, new SQLChar(grantable ? "Y" : "N"));

        return row;
    }

    ///////////////////////////////////////////////////////////////////////////
    //
    //  ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make an  Tuple Descriptor out of a SYSPERMS row
     *
     * @param row                   a SYSPERMS row
     * @param parentTupleDescriptor unused
     * @param dd                    dataDictionary
     * @return a  descriptor equivalent to a SYSPERMS row
     * @throws org.apache.derby.shared.common.error.StandardException
     *          thrown on failure
     */
    public TupleDescriptor buildDescriptor
            (ExecRow row,
             TupleDescriptor parentTupleDescriptor,
             DataDictionary dd)
            throws StandardException {

        DataValueDescriptor col;
        PermDescriptor descriptor;
        String permIdString;
        String objectType;
        String objectIdString;
        String permission;
        String grantor;
        String grantee;
        String isGrantable;

        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(row.nColumns() == SYSPERMS_COLUMN_COUNT,
                    "Wrong number of columns for a SYSPERMS row");
        }

        // first column is uuid of this permission descriptor (char(36))
        col = row.getColumn(SYSPERMS_PERMISSIONID);
        permIdString = col.getString();

        // second column is objectType (varchar(36))
        col = row.getColumn(SYSPERMS_OBJECTTYPE);
        objectType = col.getString();

        // third column is objectid (varchar(36))
        col = row.getColumn(SYSPERMS_OBJECTID);
        objectIdString = col.getString();

        // fourth column is permission (varchar(128))
        col = row.getColumn(SYSPERMS_PERMISSION);
        permission = col.getString();

        // fifth column is grantor auth Id (varchar(128))
        col = row.getColumn(SYSPERMS_GRANTOR);
        grantor = col.getString();

        // sixth column is grantee auth Id (varchar(128))
        col = row.getColumn(SYSPERMS_GRANTEE);
        grantee = col.getString();

        // seventh column is isGrantable (char(1))
        col = row.getColumn(SYSPERMS_IS_GRANTABLE);
        isGrantable = col.getString();

        descriptor = ddg.newPermDescriptor
                (getUUIDFactory().recreateUUID(permIdString),
                        objectType,
                        getUUIDFactory().recreateUUID(objectIdString),
                        permission,
                        grantor,
                        grantee,
                        isGrantable.equals("Y") ? true : false);

        return descriptor;
    }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList()
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        throws StandardException
    {
        return new SystemColumn[]{
                SystemColumnImpl.getUUIDColumn("UUID", false),
                SystemColumnImpl.getColumn("OBJECTTYPE", Types.VARCHAR, false, 36),
                SystemColumnImpl.getUUIDColumn("OBJECTID", false),
                SystemColumnImpl.getColumn("PERMISSION", Types.CHAR, false, 36),
                SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
                SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
                SystemColumnImpl.getIndicatorColumn("ISGRANTABLE")
        };
    }
}
