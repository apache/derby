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

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Factory for creating a SYSSCHEMAS row.
 *
 *
 * @version 0.1
 * @author Jamie
 */

public class SYSSCHEMASRowFactory extends CatalogRowFactory
{
	private	static	final	String	TABLENAME_STRING = "SYSSCHEMAS";

	public	static	final	int		SYSSCHEMAS_COLUMN_COUNT = 3;
	/* Column #s for sysinfo (1 based) */
	public	static	final	int		SYSSCHEMAS_SCHEMAID = 1;
	public	static	final	int		SYSSCHEMAS_SCHEMANAME = 2;
	public	static	final	int		SYSSCHEMAS_SCHEMAAID = 3;

	protected static final int		SYSSCHEMAS_INDEX1_ID = 0;
	protected static final int		SYSSCHEMAS_INDEX2_ID = 1;


	private static final int[][] indexColumnPositions =
	{
		{SYSSCHEMAS_SCHEMANAME},
		{SYSSCHEMAS_SCHEMAID}
	};
	
	private static final String[][] indexColumnNames =
	{
		{"SCHEMANAME"},
		{"SCHEMAID"}
	};

    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		 "80000022-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"8000002a-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000024-00d0-fd77-3ed8-000a0a0b1900"	// SYSSCHEMAS_INDEX1
		,"80000026-00d0-fd77-3ed8-000a0a0b1900"	// SYSSCHEMAS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSSCHEMASRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSSCHEMAS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, indexColumnNames, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSSCHEMAS row
	 *
	 * @param emptyRow			Make an empty row if this parameter is true
	 * @param schemaDescriptor	In-memory tuple to be converted to a disk row.
	 *
	 * @return	Row suitable for inserting into SYSSCHEMAS.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException
	{
		DataTypeDescriptor		dtd;
		ExecRow    				row;
		DataValueDescriptor		col;
		String					name = null;
		UUID						oid = null;
		String					uuid = null;	
		String					aid = null;

		if (td != null)
		{
			SchemaDescriptor	schemaDescriptor = (SchemaDescriptor)td;

			name = schemaDescriptor.getSchemaName();
			oid = schemaDescriptor.getUUID();
			if ( oid == null )
		    {
				oid = getUUIDFactory().createUUID();
				schemaDescriptor.setUUID(oid);
			}
			uuid = oid.toString();

			aid = schemaDescriptor.getAuthorizationId();
		}

		/* Build the row to insert */
		row = getExecutionFactory().getValueRow(SYSSCHEMAS_COLUMN_COUNT);

		/* 1st column is SCHEMAID */
		row.setColumn(1, dvf.getCharDataValue(uuid));

		/* 2nd column is SCHEMANAME */
		row.setColumn(2, dvf.getVarcharDataValue(name));

		/* 3rd column is SCHEMAAID */
		row.setColumn(3, dvf.getVarcharDataValue(aid));

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
		int ncols = getIndexColumnCount(indexNumber);
		ExecIndexRow row = getExecutionFactory().getIndexableRow(ncols + 1);

		row.setColumn(ncols + 1, rowLocation);


		switch( indexNumber )
		{
		    case SYSSCHEMAS_INDEX1_ID:
				/* 1st column is SCHEMANAME (varchar(128)) */
				row.setColumn(1, getDataValueFactory().getVarcharDataValue((String) null));
				break;

		    case SYSSCHEMAS_INDEX2_ID:
				/* 1st column is SCHEMAID (UUID - char(36)) */
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
	 * Make an  Tuple Descriptor out of a SYSSCHEMAS row
	 *
	 * @param row 					a SYSSCHEMAS row
	 * @param parentTupleDescriptor	unused
	 * @param dd 					dataDictionary
	 *
	 * @return	a  descriptor equivalent to a SYSSCHEMAS row
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		DataValueDescriptor			col;
		SchemaDescriptor			descriptor;
		String						name;
		UUID							id;
		String						aid;
		String						uuid;
		DataDescriptorGenerator		ddg = dd.getDataDescriptorGenerator();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row.nColumns() == SYSSCHEMAS_COLUMN_COUNT, 
								 "Wrong number of columns for a SYSSCHEMAS row");
		}

		// first column is schemaid (UUID - char(36))
		col = row.getColumn(1);
		uuid = col.getString();
		id = getUUIDFactory().recreateUUID(uuid);

		// second column is schemaname (varchar(128))
		col = row.getColumn(2);
		name = col.getString();

		// third column is auid (varchar(128))
		col = row.getColumn(3);
		aid = col.getString();

		descriptor = ddg.newSchemaDescriptor(name, aid, id);

		return descriptor;
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
		SystemColumn[]			columnList = new SystemColumn[SYSSCHEMAS_COLUMN_COUNT];

		// describe columns

		columnList[index++] = new SystemColumnImpl(	
							convertIdCase( "SCHEMAID"),			// name 
							SYSSCHEMAS_SCHEMAID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );
		columnList[index++] = 
					new SystemColumnImpl(		// SQL IDENTIFIER
							convertIdCase( "SCHEMANAME"),		// column name
							SYSSCHEMAS_SCHEMANAME,	// column number
							false				// nullability
							);

		columnList[index++] = 
					new SystemColumnImpl(		// SQL IDENTIFIER
							convertIdCase( "AUTHORIZATIONID"),	// column name
							SYSSCHEMAS_SCHEMAAID,	// column number
							false				// nullability
							);


		return	columnList;
	}
}
