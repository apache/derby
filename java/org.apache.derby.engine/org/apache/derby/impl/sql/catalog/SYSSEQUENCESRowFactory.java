/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSSEQUENCESRowFactory

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
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import java.sql.Types;

/**
 * Factory for creating a SYSSEQUENCES row. The contract of this table is this:
 * if the CURRENTVALUE column is null, then the sequence is exhausted and
 * no more values can be generated from it.
 */

public class SYSSEQUENCESRowFactory extends CatalogRowFactory
{
    public static final String TABLENAME_STRING = "SYSSEQUENCES";

    public static final int SYSSEQUENCES_COLUMN_COUNT = 10;
    /* Column #s for sysinfo (1 based) */
    public static final int SYSSEQUENCES_SEQUENCEID = 1;
    public static final int SYSSEQUENCES_SEQUENCENAME = 2;
    public static final int SYSSEQUENCES_SCHEMAID = 3;
    public static final int SYSSEQUENCES_SEQUENCEDATATYPE = 4;
    public static final int SYSSEQUENCES_CURRENT_VALUE = 5;
    public static final int SYSSEQUENCES_START_VALUE = 6;
    public static final int SYSSEQUENCES_MINIMUM_VALUE = 7;
    public static final int SYSSEQUENCES_MAXIMUM_VALUE = 8;
    public static final int SYSSEQUENCES_INCREMENT = 9;
    public static final int SYSSEQUENCES_CYCLE_OPTION = 10;

    private static final int[][] indexColumnPositions =
            {
                    {SYSSEQUENCES_SEQUENCEID},
                    {SYSSEQUENCES_SCHEMAID, SYSSEQUENCES_SEQUENCENAME}
            };

    // (Sequence)_ID
    static final int SYSSEQUENCES_INDEX1_ID = 0;
    // (seqeqnce)_NAME_SCHEMAID
    static final int SYSSEQUENCES_INDEX2_ID = 1;

    private static final boolean[] uniqueness = null;

    private static final String[] uuids = {
            "9810800c-0121-c5e2-e794-00000043e718", // catalog UUID
            "6ea6ffac-0121-c5e6-29e6-00000043e718", // heap UUID
            "7a92cf84-0121-c5fa-caf1-00000043e718", // INDEX1
            "6b138684-0121-c5e9-9114-00000043e718"  // INDEX2
    };


    /**
     * Constructor
     *
     * @param uuidf UUIDFactory
     * @param ef    ExecutionFactory
     * @param dvf   DataValueFactory
     */
    SYSSEQUENCESRowFactory(UUIDFactory uuidf,
                           ExecutionFactory ef,
                           DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSSEQUENCES_COLUMN_COUNT, TABLENAME_STRING,
                indexColumnPositions, uniqueness, uuids);
    }

    /**
     * Make a SYSSEQUENCES row
     *
     * @param td     a sequence descriptor
     * @param parent unused
     * @return Row suitable for inserting into SYSSEQUENCES.
     * @throws org.apache.derby.shared.common.error.StandardException
     *          thrown on failure
     */

    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
            throws StandardException {
        ExecRow row;
        String oidString = null;
        String sequenceName = null;
        String schemaIdString = null;
        TypeDescriptor typeDesc = null;
        Long currentValue = null;
        long startValue = 0;
        long minimumValue = 0;
        long maximumValue = 0;
        long increment = 0;
        boolean canCycle = false;


        if (td != null) {
            SequenceDescriptor sd = (SequenceDescriptor) td;

            UUID oid = sd.getUUID();
            oidString = oid.toString();
            sequenceName = sd.getSequenceName();

            UUID schemaId = sd.getSchemaId();
            schemaIdString = schemaId.toString();

            typeDesc = sd.getDataType().getCatalogType();

            currentValue = sd.getCurrentValue();
            startValue = sd.getStartValue();
            minimumValue = sd.getMinimumValue();
            maximumValue = sd.getMaximumValue();
            increment = sd.getIncrement();
            canCycle = sd.canCycle();
        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSSEQUENCES_COLUMN_COUNT);

        /* 1st column is UUID */
        row.setColumn(SYSSEQUENCES_SEQUENCEID, new SQLChar(oidString));

        /* 2nd column is SEQUENCENAME */
        row.setColumn(SYSSEQUENCES_SEQUENCENAME, new SQLVarchar(sequenceName));

        /* 3nd column is SCHEMAID */
        row.setColumn(SYSSEQUENCES_SCHEMAID, new SQLChar(schemaIdString));

        /* 4th column is SEQUENCEDATATYPE */
        row.setColumn(SYSSEQUENCES_SEQUENCEDATATYPE, new UserType(typeDesc));

        /* 5th column is CURRENTVALUE */
        SQLLongint curVal;
        if ( currentValue == null ) { curVal = new SQLLongint(); }
        else { curVal = new SQLLongint( currentValue.longValue() ); }
        row.setColumn(SYSSEQUENCES_CURRENT_VALUE, curVal );

        /* 6th column is STARTVALUE */
        row.setColumn(SYSSEQUENCES_START_VALUE, new SQLLongint(startValue));

        /* 7th column is MINIMUMVALUE */
        row.setColumn(SYSSEQUENCES_MINIMUM_VALUE, new SQLLongint(minimumValue));

        /* 8th column is MAXIMUMVALUE */
        row.setColumn(SYSSEQUENCES_MAXIMUM_VALUE, new SQLLongint(maximumValue));

        /* 9th column is INCREMENT */
        row.setColumn(SYSSEQUENCES_INCREMENT, new SQLLongint(increment));

        /* 10th column is CYCLEOPTION */
        row.setColumn(SYSSEQUENCES_CYCLE_OPTION, new SQLChar(canCycle ? "Y" : "N"));

        return row;
    }

    /**
     * Make an  Tuple Descriptor out of a SYSSEQUENCES row
     *
     * @param row                   a SYSSEQUENCES row
     * @param parentTupleDescriptor unused
     * @param dd                    dataDictionary
     * @return a  descriptor equivalent to a SYSSEQUENCES row
     * @throws org.apache.derby.shared.common.error.StandardException
     *          thrown on failure
     */
    public TupleDescriptor buildDescriptor
            (ExecRow row,
             TupleDescriptor parentTupleDescriptor,
             DataDictionary dd)
            throws StandardException {

        DataValueDescriptor col;
        SequenceDescriptor descriptor;
        UUID ouuid;
        String sequenceName;
        UUID suuid;
        Long currentValue;
        long startValue;
        long minimumValue;
        long maximumValue;
        long increment;
        String cycleOption;

        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(row.nColumns() == SYSSEQUENCES_COLUMN_COUNT,
                    "Wrong number of columns for a SYSSEQUENCES row");
        }

        // first column is uuid of this sequence descriptor (char(36))
        col = row.getColumn(SYSSEQUENCES_SEQUENCEID);
        String oidString = col.getString();
        ouuid = getUUIDFactory().recreateUUID(oidString);

        // second column is sequenceName (varchar(128))
        col = row.getColumn(SYSSEQUENCES_SEQUENCENAME);
        sequenceName = col.getString();

        // third column is uuid of this sequence descriptors schema (char(36))
        col = row.getColumn(SYSSEQUENCES_SCHEMAID);
        String schemaIdString = col.getString();
        suuid = getUUIDFactory().recreateUUID(schemaIdString);

        // fourth column is the data type of this sequene generator
        /*
          ** What is stored in the column is a TypeDescriptorImpl, which
          ** points to a BaseTypeIdImpl.  These are simple types that are
          ** intended to be movable to the client, so they don't have
          ** the entire implementation.  We need to wrap them in DataTypeServices
          ** and TypeId objects that contain the full implementations for
          ** language processing.
          */
        TypeDescriptor catalogType = (TypeDescriptor) row.getColumn(SYSSEQUENCES_SEQUENCEDATATYPE).
                getObject();
        DataTypeDescriptor dataTypeServices =
                DataTypeDescriptor.getType(catalogType);

        col = row.getColumn(SYSSEQUENCES_CURRENT_VALUE);
        if ( col.isNull() ) { currentValue = null; }
        else { currentValue = col.getLong(); }
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

        col = row.getColumn(SYSSEQUENCES_START_VALUE);
        startValue = col.getLong();

        col = row.getColumn(SYSSEQUENCES_MINIMUM_VALUE);
        minimumValue = col.getLong();

        col = row.getColumn(SYSSEQUENCES_MAXIMUM_VALUE);
        maximumValue = col.getLong();

        col = row.getColumn(SYSSEQUENCES_INCREMENT);
        increment = col.getLong();

        col = row.getColumn(SYSSEQUENCES_CYCLE_OPTION);
        cycleOption = col.getString();

        descriptor = ddg.newSequenceDescriptor
                (dd.getSchemaDescriptor(suuid, null),
                        ouuid,
                        sequenceName,
                        dataTypeServices,
                        currentValue,
                        startValue,
                        minimumValue,
                        maximumValue,
                        increment,
                        cycleOption.equals("Y") ? true : false);

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

                SystemColumnImpl.getUUIDColumn("SEQUENCEID", false),
                SystemColumnImpl.getIdentifierColumn("SEQUENCENAME", false),
                SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
                SystemColumnImpl.getJavaColumn("SEQUENCEDATATYPE",
                        "org.apache.derby.catalog.TypeDescriptor", false),
                SystemColumnImpl.getColumn("CURRENTVALUE", Types.BIGINT, true),
                SystemColumnImpl.getColumn("STARTVALUE", Types.BIGINT, false),
                SystemColumnImpl.getColumn("MINIMUMVALUE", Types.BIGINT, false),
                SystemColumnImpl.getColumn("MAXIMUMVALUE", Types.BIGINT, false),
                SystemColumnImpl.getColumn("INCREMENT", Types.BIGINT, false),
                SystemColumnImpl.getIndicatorColumn("CYCLEOPTION")
        };
    }
}

