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

import org.apache.derby.iapi.sql.dictionary.SystemColumn;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

/**
 * Factory for creating a SYSTABLES row.
 *
 *
 * @version 0.1
 * @author Rick Hillegas (extracted from DataDictionaryImpl).
 */

public class SYSTABLESRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSTABLES";

	protected static final int		SYSTABLES_COLUMN_COUNT = 5;
	/* Column #s for systables (1 based) */
	protected static final int		SYSTABLES_TABLEID = 1;
	protected static final int		SYSTABLES_TABLENAME = 2;
	protected static final int		SYSTABLES_TABLETYPE = 3;
	protected static final int		SYSTABLES_SCHEMAID = 4;
	protected static final int		SYSTABLES_LOCKGRANULARITY = 5;

	protected static final int		SYSTABLES_INDEX1_ID = 0;
	protected static final int		SYSTABLES_INDEX1_TABLENAME = 1;
	protected static final int		SYSTABLES_INDEX1_SCHEMAID = 2;

	protected static final int		SYSTABLES_INDEX2_ID = 1;
	protected static final int		SYSTABLES_INDEX2_TABLEID = 1;
	
	// all indexes are unique.

	private	static	final	String[]	uuids =
	{
		 "80000018-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000028-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000001a-00d0-fd77-3ed8-000a0a0b1900"	// SYSTABLES_INDEX1
		,"8000001c-00d0-fd77-3ed8-000a0a0b1900"	// SYSTABLES_INDEX2
	};

	private static final int[][] indexColumnPositions = 
	{ 
		{ SYSTABLES_TABLENAME, SYSTABLES_SCHEMAID},
		{ SYSTABLES_TABLEID }
	};

	private static final String[][] indexColumnNames =
	{
		{"TABLENAME", "SCHEMAID"},
		{"TABLEID"}
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSTABLESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSTABLES_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexColumnNames, (boolean[]) null, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SYSTABLES row
	 *
	 * @param emptyRow	Make an empty row if this parameter is true
	 * @param descriptor	Table descriptor
	 * @param schema	Schema descriptor
	 *
	 * @return	Row suitable for inserting into SYSTABLES.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td,
						   TupleDescriptor	parent)
					throws StandardException
	{
		UUID						oid;
		String	   				tabSType = null;
		int	   					tabIType;
		ExecRow        			row;
		String					lockGranularity = null;
		String					tableID = null;
		String					schemaID = null;
		String					tableName = null;


		if (td != null)
		{
			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			TableDescriptor descriptor = (TableDescriptor)td;
			SchemaDescriptor schema = (SchemaDescriptor)parent;

			oid = descriptor.getUUID();
			if ( oid == null )
		    {
				oid = getUUIDFactory().createUUID();
				descriptor.setUUID(oid);
			}
			tableID = oid.toString();
			
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(schema != null, 
							"Schema should not be null unless empty row is true");
				if (schema.getUUID() == null)
				{
					SanityManager.THROWASSERT("schema " + schema + " has a null OID");
				}
			}
		
			schemaID = schema.getUUID().toString();

			tableName = descriptor.getName();

			/* RESOLVE - Table Type should really be a char in the descriptor
			 * T, S, V instead of 0, 1, 2
			 */
			tabIType = descriptor.getTableType();
			switch (tabIType)
			{
			    case TableDescriptor.BASE_TABLE_TYPE:
					tabSType = "T";
					break;
			    case TableDescriptor.SYSTEM_TABLE_TYPE:
					tabSType = "S";
					break;
			    case TableDescriptor.VIEW_TYPE:
					tabSType = "V";
					break;		

			    default:
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT("invalid table type");
			}
			char[] lockGChar = new char[1];
			lockGChar[0] = descriptor.getLockGranularity();
			lockGranularity = new String(lockGChar);
		}

		/* Insert info into systables */

		/* RESOLVE - It would be nice to require less knowledge about systables
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSTABLES_COLUMN_COUNT);

		/* 1st column is TABLEID (UUID - char(36)) */
		row.setColumn(SYSTABLES_TABLEID, dvf.getCharDataValue(tableID));

		/* 2nd column is NAME (varchar(30)) */
		row.setColumn(SYSTABLES_TABLENAME, dvf.getVarcharDataValue(tableName));

		/* 3rd column is TABLETYPE (char(1)) */
		row.setColumn(SYSTABLES_TABLETYPE, dvf.getCharDataValue(tabSType));

		/* 4th column is SCHEMAID (UUID - char(36)) */
		row.setColumn(SYSTABLES_SCHEMAID, dvf.getCharDataValue(schemaID));

		/* 5th column is LOCKGRANULARITY (char(1)) */
		row.setColumn(SYSTABLES_LOCKGRANULARITY, dvf.getCharDataValue(lockGranularity));

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
		    case SYSTABLES_INDEX1_ID:
				/* 1st column is TABLENAME (varchar(128)) */
				row.setColumn(1, getDataValueFactory().getVarcharDataValue((String) null));

				/* 2nd column is SCHEMAID (UUID - char(36)) */
				row.setColumn(2, getDataValueFactory().getCharDataValue((String) null));

				break;

		    case SYSTABLES_INDEX2_ID:
				/* 1st column is TABLEID (UUID - char(36)) */
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
	 * Make a TableDescriptor out of a SYSTABLES row
	 *
	 * @param row a SYSTABLES row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @return	a table descriptor equivalent to a SYSTABLES row
	 *
	 * @exception   StandardException thrown on failure
	 */

	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(row.nColumns() == SYSTABLES_COLUMN_COUNT, "Wrong number of columns for a SYSTABLES row");

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		String	tableUUIDString; 
		String	schemaUUIDString; 
		int		tableTypeEnum;
		String	lockGranularity;
		String	tableName, tableType;
		DataValueDescriptor	col;
		UUID		tableUUID;
		UUID		schemaUUID;
		SchemaDescriptor	schema;
		TableDescriptor		tabDesc;

		/* 1st column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSTABLES_TABLEID);
		tableUUIDString = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);


		/* 2nd column is TABLENAME (varchar(128)) */
		col = row.getColumn(SYSTABLES_TABLENAME);
		tableName = col.getString();

		/* 3rd column is TABLETYPE (char(1)) */
		col = row.getColumn(SYSTABLES_TABLETYPE);
		tableType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tableType.length() == 1, "Fourth column type incorrect");
		}
		switch (tableType.charAt(0))
		{
			case 'T' : 
				tableTypeEnum = TableDescriptor.BASE_TABLE_TYPE;
				break;
			case 'S' :
				tableTypeEnum = TableDescriptor.SYSTEM_TABLE_TYPE;
				break;
			case 'V' :
				tableTypeEnum = TableDescriptor.VIEW_TYPE;
				break;
			default:
				if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Fourth column value invalid");
				tableTypeEnum = -1;
		}

		/* 4th column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSTABLES_SCHEMAID);
		schemaUUIDString = col.getString();
		schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);
		
		schema = dd.getSchemaDescriptor(schemaUUID, null);

		/* 5th column is LOCKGRANULARITY (char(1)) */
		col = row.getColumn(SYSTABLES_LOCKGRANULARITY);
		lockGranularity = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(lockGranularity.length() == 1, "Fifth column type incorrect");
		}

		// RESOLVE - Deal with lock granularity
		tabDesc = ddg.newTableDescriptor(tableName, schema, tableTypeEnum, lockGranularity.charAt(0));
		tabDesc.setUUID(tableUUID);
		return tabDesc;
	}

	/**
	 *	Get the table name out of this SYSTABLES row
	 *
	 * @param row a SYSTABLES row
	 *
	 * @return	string, the table name
	 *
	 * @exception   StandardException thrown on failure
	 */
	protected String getTableName(ExecRow	row)
					throws StandardException
	{
		DataValueDescriptor	col;

		col = row.getColumn(SYSTABLES_TABLENAME);
		return col.getString();
	}


	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
	{
		SystemColumn[]			columnList = new SystemColumn[SYSTABLES_COLUMN_COUNT];

		// describe columns

		columnList[0] = new SystemColumnImpl(	
								convertIdCase( "TABLEID"),			// column name
								SYSTABLES_TABLEID,	// column number
								0,					// precision
								0,					// scale
								false,				// nullability
								"CHAR",				// dataType
								true,				// built-in type
								36					// maxLength
			                   );

		columnList[1] = new SystemColumnImpl(		// SQLIDENTIFIER
								convertIdCase( "TABLENAME"),		// column name
								SYSTABLES_TABLENAME, 	// column number
								false				// nullability
			                   );

		columnList[2] = new SystemColumnImpl(	
								convertIdCase( "TABLETYPE"),		// column name
								SYSTABLES_TABLETYPE,// column number
								0,					// precision
								0,					// scale
								false,				// nullability
								"CHAR",				// dataType
								true,				// built-in type
								1					// maxLength
			                   );

		columnList[3] = new SystemColumnImpl(	
								convertIdCase( "SCHEMAID"),			// column name
								SYSTABLES_SCHEMAID,	// schema number
								0,					// precision
								0,					// scale
								false,				// nullability
								"CHAR",				// dataType
								true,				// built-in type
								36					// maxLength
			                   );

		columnList[4] = new SystemColumnImpl(	
								convertIdCase( "LOCKGRANULARITY"),		// column name
								SYSTABLES_LOCKGRANULARITY,// column number
								0,					// precision
								0,					// scale
								false,				// nullability
								"CHAR",				// dataType
								true,				// built-in type
								1					// maxLength
			                   );

		return	columnList;
	}

}
