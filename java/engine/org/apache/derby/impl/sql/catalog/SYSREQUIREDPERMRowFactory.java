/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSREQUIREDPERMRowFactory

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.RequiredPermDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Statistics;

import java.sql.Timestamp;

/**
 * Factory for creating a SYSREQUIREDPERM row.
 *
 */

public class SYSREQUIREDPERMRowFactory extends CatalogRowFactory
{
	static final String TABLENAME_STRING = "SYSREQUIREDPERM";

    // Column numbers for the SYSREQUIREDPERM table. 1 based
    private static final int OPERATOR_COL_NUM = 1;
    private static final int OPERATORTYPE_COL_NUM = 2;
    private static final int PERMTYPE_COL_NUM = 3;
    private static final int OBJECT_COL_NUM = 4;
    private static final int COLUMNS_COL_NUM = 5;
    private static final int COLUMN_COUNT = 5;

    static final int OPERATOR_AND_TYPE_INDEX_NUM = 0;

    private	static final String[] uuids =
    {
        "80840021-0103-0e39-b8e7-00000010f010" // catalog UUID
		,"888c4022-0103-0e39-b8e7-00000010f010"	// heap UUID
		,"a094c023-0103-0e39-b8e7-00000010f010"	// index
    };
	private static final int[][] indexColumnPositions = 
	{ 
		{ OPERATOR_COL_NUM, OPERATORTYPE_COL_NUM}
	};
	private static final String[][] indexColumnNames =
	{
		{"OPERATOR", "OPERATORTYPE"}
	};
    private static final boolean[] indexUniqueness = { false};

    private SystemColumn[] columnList;

    public SYSREQUIREDPERMRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                     boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo( COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexColumnNames, indexUniqueness, uuids);
	}

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException
	{
        String operatorId = null;
        String operatorType = null;
        String permType = null;
        String objectId = null;
        FormatableBitSet columns = null;
        
        if( td != null)
        {
            RequiredPermDescriptor rpd = (RequiredPermDescriptor) td;
            if( rpd.getOperatorUUID() != null)
                operatorId = rpd.getOperatorUUID().toString();
            operatorType = rpd.getOperatorType();
            permType = rpd.getPermType();
            if( rpd.getObjectUUID() != null)
                objectId = rpd.getObjectUUID().toString();
            columns = rpd.getColumns();
        }
        ExecRow row = getExecutionFactory().getValueRow( COLUMN_COUNT);
        row.setColumn( OPERATOR_COL_NUM, dvf.getCharDataValue( operatorId));
        row.setColumn( OPERATORTYPE_COL_NUM, dvf.getCharDataValue( operatorType));
        row.setColumn( PERMTYPE_COL_NUM, dvf.getCharDataValue( permType));
        row.setColumn( OBJECT_COL_NUM, dvf.getCharDataValue( objectId));
        row.setColumn( COLUMNS_COL_NUM, dvf.getDataValue( (Object) columns));
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
                                  "Wrong size row passed to SYSREQUIREDPERMRowFactory.buildDescriptor");

        String operatorUUIDString = row.getColumn( OPERATOR_COL_NUM).getString();
        UUID operatorUUID = getUUIDFactory().recreateUUID(operatorUUIDString);
        String operatorType  = row.getColumn( OPERATORTYPE_COL_NUM).getString();
        String permType  = row.getColumn( PERMTYPE_COL_NUM).getString();
        String objectUUIDString = row.getColumn( OBJECT_COL_NUM).getString();
        UUID objectUUID = getUUIDFactory().recreateUUID(objectUUIDString);
        FormatableBitSet columns = (FormatableBitSet) row.getColumn( COLUMNS_COL_NUM).getObject();
        if( SanityManager.DEBUG)
        {
            SanityManager.ASSERT( "V".equals( operatorType) || "T".equals( operatorType) || "C".equals( operatorType),
                                  "Invalid operatorType, " + operatorType
                                  + ", passed to SYSREQUIREDPERMRowFactory.buildDescriptor");
            SanityManager.ASSERT( "S".equals( permType) || "D".equals( permType) || "I".equals( permType) ||
                                  "U".equals( permType) || "E".equals( permType),
                                  "Invalid permType, " + permType
                                  + ", passed to SYSREQUIREDPERMRowFactory.buildDescriptor");
        }
        return new RequiredPermDescriptor( operatorUUID, operatorType, permType, objectUUID, columns);
    } // end of buildDescriptor

	/** builds a column list for the catalog */
	public SystemColumn[] buildColumnList()
    {
		if (columnList == null)
        {
            columnList = new SystemColumn[ COLUMN_COUNT];

            columnList[ OPERATOR_COL_NUM - 1] =
              new SystemColumnImpl( convertIdCase( "OPERATOR"),
                                    OPERATOR_COL_NUM,
                                    0, // precision
                                    0, // scale
                                    false, // nullability
                                    "CHAR", // dataType
                                    true, // built-in type
                                    36);
            columnList[ OPERATORTYPE_COL_NUM - 1] =
              new SystemColumnImpl( convertIdCase( "OPERATORTYPE"),
                                    OPERATORTYPE_COL_NUM,
                                    0, // precision
                                    0, // scale
                                    false, // nullability
                                    "CHAR", // dataType
                                    true, // built-in type
                                    1);
            columnList[ PERMTYPE_COL_NUM - 1] =
              new SystemColumnImpl( convertIdCase( "PERMTYPE"),
                                    PERMTYPE_COL_NUM,
                                    0, // precision
                                    0, // scale
                                    false, // nullability
                                    "CHAR", // dataType
                                    true, // built-in type
                                    1);
            columnList[ OBJECT_COL_NUM - 1] =
              new SystemColumnImpl( convertIdCase( "OBJECT"),
                                    OBJECT_COL_NUM,
                                    0, // precision
                                    0, // scale
                                    false, // nullability
                                    "CHAR", // dataType
                                    true, // built-in type
                                    36);
            columnList[ COLUMNS_COL_NUM - 1] =
              new SystemColumnImpl( convertIdCase( "COLUMNS"),
                                    COLUMNS_COL_NUM,
                                    0, // precision
                                    0, // scale
                                    false, // nullability
                                    "org.apache.derby.iapi.services.io.FormatableBitSet", // datatype
                                    false,							// built-in type
                                    DataTypeDescriptor.MAXIMUM_WIDTH_UNKNOWN // maxLength
                  );
        }
		return columnList;
    } // end of buildColumnList

	/**
	 * builds an empty row given for a given index number.
	 */
  	public ExecIndexRow buildEmptyIndexRow(int indexNumber,
                                           RowLocation rowLocation) 
  		throws StandardException
    {
        ExecIndexRow row = null;
        
        switch( indexNumber)
        {
        case OPERATOR_AND_TYPE_INDEX_NUM:
            row = getExecutionFactory().getIndexableRow( 3);
            row.setColumn(1, getDataValueFactory().getNullChar( (StringDataValue) null)); // operator UUID
            row.setColumn(2, getDataValueFactory().getNullChar( (StringDataValue) null)); // operator type
            break;

        default:
            if( SanityManager.DEBUG)
                SanityManager.THROWASSERT( "Invalid index number passed to SYSREQUIREDPERMRowFactory.buildEmptyIndexRow");
            return null;
        }
        row.setColumn( row.nColumns(), rowLocation);
        return row;
    } // end of buildEmptyIndexRow
}
