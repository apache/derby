/* IBM Confidential
 *
 * Product ID: 5697-F53
 *

 * Copyright 2000, 2001WESTHAM

 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SubCheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.ReferencedColumns;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

import java.util.Properties;

/**
 * Factory for creating a SYSCHECKS row.
 *
 * @author jerry
 */

public class SYSCHECKSRowFactory extends CatalogRowFactory
{
	private  static final String	TABLENAME_STRING = "SYSCHECKS";

	protected static final int		SYSCHECKS_COLUMN_COUNT = 3;
	protected static final int		SYSCHECKS_CONSTRAINTID = 1;
	protected static final int		SYSCHECKS_CHECKDEFINITION = 2;
	protected static final int		SYSCHECKS_REFERENCEDCOLUMNS = 3;

	// Column widths
	protected static final int		SYSCHECKS_CONSTRAINTID_WIDTH = 36;
	protected static final int		SYSCHECKS_INDEX1_ID = 0;

	// index is unique.
    private	static	final	boolean[]	uniqueness = null;

	private static final int[][] indexColumnPositions =
	{	
		{SYSCHECKS_CONSTRAINTID}
	};

	private static final String[][] indexColumnNames =
	{
		{"CONSTRAINTID"}
	};

	private	static	final	String[]	uuids =
	{
		 "80000056-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000059-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000058-00d0-fd77-3ed8-000a0a0b1900"	// SYSCHECKS_INDEX1 UUID
	};



	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public
	SYSCHECKSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSCHECKS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexColumnNames, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSCHECKS row
	 *
	 * @param td CheckConstraintDescriptorImpl
	 *
	 * @return	Row suitable for inserting into SYSCHECKS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecIndexRow			row;
		ReferencedColumns rcd = null;
		String					checkDefinition = null;
		String					constraintID = null;

		if (td != null)
		{
			CheckConstraintDescriptor cd = (CheckConstraintDescriptor)td;
			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			constraintID = cd.getUUID().toString();

			checkDefinition = cd.getConstraintText();

			rcd = cd.getReferencedColumnsDescriptor();
		}

		/* Build the row */
		row = getExecutionFactory().getIndexableRow(SYSCHECKS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSCHECKS_CONSTRAINTID, dvf.getCharDataValue(constraintID));

		/* 2nd column is CHECKDEFINITION */
		row.setColumn(SYSCHECKS_CHECKDEFINITION,
				dvf.getLongvarcharDataValue(checkDefinition));

		/* 3rd column is REFERENCEDCOLUMNS
		 *  (user type org.apache.derby.catalog.ReferencedColumns)
		 */
		row.setColumn(SYSCHECKS_REFERENCEDCOLUMNS,
			dvf.getDataValue(rcd));

		return row;
	}

	/**
	 * Builds an empty index row.
	 *
	 *	@param	indexNumber	Index to build empty row for.
	 *  @param  rowLocation	Row location for last column of index row
	 *
	 * @return corresponding empty index row
	 * @exception   StandardException thrown on failure
	 */
	public ExecIndexRow	buildEmptyIndexRow( int indexNumber,
											RowLocation rowLocation) 
			throws StandardException
	{
		int  ncols = getIndexColumnCount(indexNumber);
		ExecIndexRow row = getExecutionFactory().getIndexableRow(ncols + 1);

		row.setColumn(ncols + 1, rowLocation);

		switch( indexNumber )
		{
		    case SYSCHECKS_INDEX1_ID:
				/* 1st column is CONSTRAINTID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));
				break;
		}	// end switch

		return	row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ViewDescriptor out of a SYSCHECKS row
	 *
	 * @param row a SYSCHECKS row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		SubCheckConstraintDescriptor checkDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSCHECKS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSCHECKS row");
		}

		DataValueDescriptor		col;
		DataDescriptorGenerator ddg;
		ReferencedColumns	referencedColumns;
		String				constraintText;
		String				constraintUUIDString;
		UUID				constraintUUID;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSCHECKS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is CHECKDEFINITION */
		col = row.getColumn(SYSCHECKS_CHECKDEFINITION);
		constraintText = col.getString();

		/* 3rd column is REFERENCEDCOLUMNS */
		col = row.getColumn(SYSCHECKS_REFERENCEDCOLUMNS);
		referencedColumns =
			(ReferencedColumns) col.getObject();

		/* now build and return the descriptor */

		checkDesc = new SubCheckConstraintDescriptor(
										constraintUUID,
										constraintText,
										referencedColumns);
		return checkDesc;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
	{
		int						index = 0;
		int						columnNumber = 1;
		SystemColumn[]			columnList = new SystemColumn[SYSCHECKS_COLUMN_COUNT];

		// describe columns

		columnList[index++] = new SystemColumnImpl(	
							convertIdCase( "CONSTRAINTID"),			// name 
							SYSCHECKS_CONSTRAINTID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );
		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "CHECKDEFINITION"),		// column name
							SYSCHECKS_CHECKDEFINITION,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"LONG VARCHAR",	    // dataType
							true,				// built-in type
							TypeId.LONGVARCHAR_MAXWIDTH // maxLength
			               );
		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "REFERENCEDCOLUMNS"),		// column name
							SYSCHECKS_REFERENCEDCOLUMNS,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"org.apache.derby.catalog.ReferencedColumns",	// datatype
							false,				// built-in type
							TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN
												// maxLength
			               );


		return	columnList;
	}

}
