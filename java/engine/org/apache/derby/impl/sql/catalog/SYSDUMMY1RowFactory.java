/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSDUMMY1RowFactory

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

/**
 * Factory for creating a SYSDUMMY1 row.
 *
 * @version 0.01
 *
 */

public class SYSDUMMY1RowFactory extends CatalogRowFactory
{
	static final String TABLENAME_STRING = "SYSDUMMY1";

	/* column #s for sysstatistics (1 based) */
	
	/* only column
	*/
	protected static final int 	SYSDUMMY1_IBMREQD = 1;

	protected static final int SYSDUMMY1_COLUMN_COUNT = 1;

	private static final String[] uuids =
	{
		"c013800d-00f8-5b70-bea3-00000019ed88", // catalog UUID
		"c013800d-00f8-5b70-fee8-000000198c88"  // heap UUID.
	};
	/*
	 * STATE
	 */
	private	SystemColumn[]		columnList;

	/*
	 *	CONSTRUCTORS
	 */
    public	SYSDUMMY1RowFactory(UUIDFactory uuidf, 
									ExecutionFactory ef, 
									DataValueFactory dvf,
                                    boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		
		initInfo(SYSDUMMY1_COLUMN_COUNT, TABLENAME_STRING, 
				 null, null, null, uuids);
	}


  /**
	 * Make a SYSDUMMY1 row
	 *
	 * @param emptyRow	Make an empty row if this parameter is true
	 * @param statDescriptor Descriptor from which to create the
	 * statistic. 
	 *
	 * @return	Row suitable for inserting into SYSSTATISTICS.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException					
	{
		ExecRow row = getExecutionFactory().getValueRow(SYSDUMMY1_COLUMN_COUNT);
		
		row.setColumn(1, dvf.getCharDataValue("Y"));
		return row;
	}
	
	public TupleDescriptor buildDescriptor(
		 ExecRow 			row,
		 TupleDescriptor    parentDesc,
		 DataDictionary 	dd)
		throws StandardException
		 
	{
		return null;
	}

  	public ExecIndexRow	buildEmptyIndexRow(int indexNumber, RowLocation rowLocation)
  		throws StandardException
	{
		return null;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[] buildColumnList()
	{
		if (columnList != null)
			return columnList;

		columnList = new SystemColumn[SYSDUMMY1_COLUMN_COUNT];
		
		columnList[0] = new SystemColumnImpl(
						   convertIdCase( "IBMREQD"),			// column name
						   SYSDUMMY1_IBMREQD,    // column number
						   0,					// precision
						   0,					// scale
						   true,				// nullability
						   "CHAR",				// dataType
						   true,				// built-in type
						   1					// maxLength
						   );
		
		return columnList;
	}


}	
