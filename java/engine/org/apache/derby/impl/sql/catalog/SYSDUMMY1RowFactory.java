/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/

{ private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;
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
