/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSROLESRowFactory

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
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Factory for creating a SYSROLES row.
 */

public class SYSROLESRowFactory extends CatalogRowFactory
{
    private static final String TABLENAME_STRING = "SYSROLES";

    private static final int SYSROLES_COLUMN_COUNT = 6;
    /* Column #s for sysinfo (1 based) */
    private static final int SYSROLES_ROLE_UUID = 1;
    private static final int SYSROLES_ROLEID = 2;
    private static final int SYSROLES_GRANTEE = 3;
    private static final int SYSROLES_GRANTOR = 4;
    private static final int SYSROLES_WITHADMINOPTION = 5;
    static final int SYSROLES_ISDEF = 6;

    private static final int[][] indexColumnPositions =
    {
        {SYSROLES_ROLEID, SYSROLES_GRANTEE, SYSROLES_GRANTOR},
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
        {SYSROLES_ROLEID, SYSROLES_ISDEF},
        {SYSROLES_ROLE_UUID}
    };

    static final int SYSROLES_ROLEID_COLPOS_IN_INDEX_ID_EE_OR = 1;
    static final int SYSROLES_GRANTEE_COLPOS_IN_INDEX_ID_EE_OR = 2;

    // (role)ID_(grant)EE_(grant)OR
    static final int SYSROLES_INDEX_ID_EE_OR_IDX = 0;
    // (role)ID_(is)DEF
    static final int SYSROLES_INDEX_ID_DEF_IDX = 1;
    // UUID
    static final int SYSROLES_INDEX_UUID_IDX = 2;

    private static  final   boolean[]   uniqueness = {
        true,
        false, // many rows have same roleid and is not a definition
        true};

    private static final String[] uuids = {
        "e03f4017-0115-382c-08df-ffffe275b270", // catalog UUID
        "c851401a-0115-382c-08df-ffffe275b270", // heap UUID
        "c065801d-0115-382c-08df-ffffe275b270", // SYSROLES_INDEX_ID_EE_OR
        "787c0020-0115-382c-08df-ffffe275b270", // SYSROLES_INDEX_ID_DEF
        "629f8094-0116-d8f9-5f97-ffffe275b270"  // SYSROLES_INDEX_UUID
    };

    /**
     * Constructor
     *
     * @param uuidf UUIDFactory
     * @param ef    ExecutionFactory
     * @param dvf   DataValueFactory
     */
    SYSROLESRowFactory(UUIDFactory uuidf,
                       ExecutionFactory ef,
                       DataValueFactory dvf)
    {
        super(uuidf,ef,dvf);
        initInfo(SYSROLES_COLUMN_COUNT, TABLENAME_STRING,
                 indexColumnPositions, uniqueness, uuids );
    }

    /**
     * Make a SYSROLES row
     *
     * @param td a role grant descriptor
     * @param parent unused
     *
     * @return  Row suitable for inserting into SYSROLES.
     *
     * @exception   StandardException thrown on failure
     */

    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
        throws StandardException
    {
        ExecRow                 row;
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
        String                  oid_string = null;
        String                  roleid = null;
        String                  grantee = null;
        String                  grantor = null;
        boolean                 wao = false;
        boolean                 isdef = false;

        if (td != null)
        {
            RoleGrantDescriptor rgd = (RoleGrantDescriptor)td;
//IC see: https://issues.apache.org/jira/browse/DERBY-3137

            roleid = rgd.getRoleName();
            grantee = rgd.getGrantee();
            grantor = rgd.getGrantor();
            wao = rgd.isWithAdminOption();
            isdef = rgd.isDef();
            UUID oid = rgd.getUUID();
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
            oid_string = oid.toString();
        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSROLES_COLUMN_COUNT);

        /* 1st column is UUID */
        row.setColumn(1, new SQLChar(oid_string));

        /* 2nd column is ROLEID */
        row.setColumn(2, new SQLVarchar(roleid));

        /* 3rd column is GRANTEE */
        row.setColumn(3, new SQLVarchar(grantee));

        /* 4th column is GRANTOR */
        row.setColumn(4, new SQLVarchar(grantor));

        /* 5th column is WITHADMINOPTION */
        row.setColumn(5, new SQLChar(wao ? "Y" : "N"));

        /* 6th column is ISDEF */
        row.setColumn(6, new SQLChar(isdef ? "Y" : "N"));

        return row;
    }


    ///////////////////////////////////////////////////////////////////////////
    //
    //  ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make an  Tuple Descriptor out of a SYSROLES row
     *
     * @param row                   a SYSROLES row
     * @param parentTupleDescriptor unused
     * @param dd                    dataDictionary
     *
     * @return  a  descriptor equivalent to a SYSROLES row
     *
     * @exception   StandardException thrown on failure
     */
    public TupleDescriptor buildDescriptor
        (ExecRow                 row,
         TupleDescriptor         parentTupleDescriptor,
         DataDictionary          dd )
        throws StandardException {

        DataValueDescriptor         col;
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
        RoleGrantDescriptor              descriptor;
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
        String                      oid_string;
        String                      roleid;
        String                      grantee;
        String                      grantor;
        String                      wao;
        String                      isdef;
        DataDescriptorGenerator     ddg = dd.getDataDescriptorGenerator();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row.nColumns() == SYSROLES_COLUMN_COUNT,
                                 "Wrong number of columns for a SYSROLES row");
        }

        // first column is uuid of this role grant descriptor (char(36))
        col = row.getColumn(1);
        oid_string = col.getString();
//IC see: https://issues.apache.org/jira/browse/DERBY-3137

        // second column is roleid (varchar(128))
        col = row.getColumn(2);
        roleid = col.getString();

        // third column is grantee (varchar(128))
        col = row.getColumn(3);
        grantee = col.getString();

        // fourth column is grantor (varchar(128))
        col = row.getColumn(4);
        grantor = col.getString();

        // fifth column is withadminoption (char(1))
        col = row.getColumn(5);
        wao = col.getString();

        // sixth column is isdef (char(1))
        col = row.getColumn(6);
        isdef = col.getString();

//IC see: https://issues.apache.org/jira/browse/DERBY-3137
        descriptor = ddg.newRoleGrantDescriptor
            (getUUIDFactory().recreateUUID(oid_string),
             roleid,
             grantee,
             grantor,
             wao.equals("Y") ? true: false,
             isdef.equals("Y") ? true: false);

        return descriptor;
    }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[]   buildColumnList()
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        throws StandardException
    {
        return new SystemColumn[] {
//IC see: https://issues.apache.org/jira/browse/DERBY-3137
            SystemColumnImpl.getUUIDColumn("UUID", false),
            SystemColumnImpl.getIdentifierColumn("ROLEID", false),
            SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
            SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
            SystemColumnImpl.getIndicatorColumn("WITHADMINOPTION"),
            SystemColumnImpl.getIndicatorColumn("ISDEF"),
        };
    }
}
