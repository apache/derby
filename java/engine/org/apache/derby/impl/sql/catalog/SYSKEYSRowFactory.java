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

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.catalog.IndexDescriptor;

/**
 * Factory for creating a SYSKEYS row.
 *
 * @author jerry
 */

public class SYSKEYSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSKEYS";

	protected static final int		SYSKEYS_COLUMN_COUNT = 2;
	protected static final int		SYSKEYS_CONSTRAINTID = 1;
	protected static final int		SYSKEYS_CONGLOMERATEID = 2;

	protected static final int		SYSKEYS_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = null;

	private static final int[][] indexColumnPositions =
	{
		{SYSKEYS_CONSTRAINTID}
	};

	private static String[][] indexColumnNames =
	{
		{"CONSTRAINTID"}
	};

	private	static	final	String[]	uuids =
	{
		 "80000039-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"8000003c-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000003b-00d0-fd77-3ed8-000a0a0b1900"	// SYSKEYS_INDEX1
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSKEYSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSKEYS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexColumnNames, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSKEYS row
	 *
	 * @param emptyRow	Make an empty row if this parameter is true
	 * @param constraint	key constraint descriptor
	 *
	 * @return	Row suitable for inserting into SYSKEYS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		UUID						oid;
		String					constraintID = null;
		String					conglomerateID = null;

		if (td != null)
		{
			KeyConstraintDescriptor	constraint = (KeyConstraintDescriptor)td;

			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			oid = constraint.getUUID();
			constraintID = oid.toString();

			conglomerateID = constraint.getIndexUUIDString();
		}

		/* Insert info into syskeys */

		/* RESOLVE - It would be nice to require less knowledge about syskeys
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSKEYS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSKEYS_CONSTRAINTID, dvf.getCharDataValue(constraintID));
		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		row.setColumn(SYSKEYS_CONGLOMERATEID, dvf.getCharDataValue(conglomerateID));

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
		ExecIndexRow			row = null;

		switch( indexNumber )
		{
		    case SYSKEYS_INDEX1_ID:
				
				/* Build the row */
				row = getExecutionFactory().getIndexableRow(2);

				/* 1st column is CONSTRAINTID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

				row.setColumn(2, rowLocation);

				break;

		    default:

				if (SanityManager.DEBUG)
					SanityManager.NOTREACHED();
				return null;


		}	// end switch

		return	row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SubConstraintDescriptor out of a SYSKEYS row
	 *
	 * @param row a SYSKEYS row
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
		SubKeyConstraintDescriptor keyDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSKEYS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSKEYS row");
		}

		DataValueDescriptor		col;
		DataDescriptorGenerator ddg;
		UUID					constraintUUID;
		UUID					conglomerateUUID;
		String				constraintUUIDString;
		String				conglomerateUUIDString;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSKEYS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(SYSKEYS_CONGLOMERATEID);
		conglomerateUUIDString = col.getString();
		conglomerateUUID = getUUIDFactory().recreateUUID(conglomerateUUIDString);

		/* now build and return the descriptor */

		keyDesc =  new SubKeyConstraintDescriptor(
										constraintUUID,
										conglomerateUUID);
		return keyDesc;
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
		SystemColumn[]			columnList = new SystemColumn[SYSKEYS_COLUMN_COUNT];

		// describe columns

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "CONSTRAINTID"),			// column name
							SYSKEYS_CONSTRAINTID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "CONGLOMERATEID"),	// column name
							SYSKEYS_CONGLOMERATEID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );

		return	columnList;
	}

}
