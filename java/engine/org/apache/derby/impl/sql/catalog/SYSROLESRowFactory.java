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

import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.RoleDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Factory for creating a SYSROLES row.
 */

public class SYSROLESRowFactory extends CatalogRowFactory
{
    private static final String TABLENAME_STRING = "SYSROLES";

    private static final int SYSROLES_COLUMN_COUNT = 5;
    /* Column #s for sysinfo (1 based) */
    private static final int SYSROLES_ROLEID = 1;
    private static final int SYSROLES_GRANTEE = 2;
    private static final int SYSROLES_GRANTOR = 3;
    private static final int SYSROLES_WITHADMINOPTION = 4;
    private static final int SYSROLES_ISDEF = 5;

    static final int SYSROLES_INDEX1_ID = 0;
    static final int SYSROLES_INDEX2_ID = 1;


    private static final int[][] indexColumnPositions =
    {
        {SYSROLES_ROLEID, SYSROLES_GRANTEE, SYSROLES_GRANTOR},
        {SYSROLES_ROLEID, SYSROLES_ISDEF}
    };

    static final int SYSROLES_ROLEID_IN_INDEX1 = 1;
    static final int SYSROLES_GRANTEE_IN_INDEX1 = 2;

    private static  final   boolean[]   uniqueness = {true,false};

    private static final String[] uuids = {
        "e03f4017-0115-382c-08df-ffffe275b270", // catalog UUID
        "c851401a-0115-382c-08df-ffffe275b270", // heap UUID
        "c065801d-0115-382c-08df-ffffe275b270", // SYSROLES_INDEX1
        "787c0020-0115-382c-08df-ffffe275b270"  // SYSROLES_INDEX2
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
     * @param td a role descriptor
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
        String                  roleid = null;
        String                  grantee = null;
        String                  grantor = null;
        boolean                 wao = false;
        boolean                 isdef = false;

        if (td != null)
        {
            RoleDescriptor roleDescriptor = (RoleDescriptor)td;

            roleid = roleDescriptor.getRoleName();
            grantee = roleDescriptor.getGrantee();
            grantor = roleDescriptor.getGrantor();
            wao = roleDescriptor.isWithAdminOption();
            isdef = roleDescriptor.isDef();
        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSROLES_COLUMN_COUNT);

        /* 1st column is ROLEID */
        row.setColumn(1, new SQLVarchar(roleid));

        /* 2nd column is GRANTEE */
        row.setColumn(2, new SQLVarchar(grantee));

        /* 3rd column is GRANTOR */
        row.setColumn(3, new SQLVarchar(grantor));

        /* 4th column is WITHADMINOPTION */
        row.setColumn(4, new SQLChar(wao ? "Y" : "N"));

        /* 4th column is ISDEF */
        row.setColumn(5, new SQLChar(isdef ? "Y" : "N"));

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
        RoleDescriptor              descriptor;
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

        // first column is roleid (varchar(128))
        col = row.getColumn(1);
        roleid = col.getString();

        // second column is grantee (varchar(128))
        col = row.getColumn(2);
        grantee = col.getString();

        // third column is grantor (varchar(128))
        col = row.getColumn(3);
        grantor = col.getString();

        // fourth column is withadminoption (char(1))
        col = row.getColumn(4);
        wao = col.getString();

        // fifth column is isdef (char(1))
        col = row.getColumn(5);
        isdef = col.getString();

        descriptor = ddg.newRoleDescriptor(roleid,
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
    {
        return new SystemColumn[] {
            SystemColumnImpl.getIdentifierColumn("ROLEID", false),
            SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
            SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
            SystemColumnImpl.getIndicatorColumn("WITHADMINOPTION"),
            SystemColumnImpl.getIndicatorColumn("ISDEF"),
        };
    }
}
