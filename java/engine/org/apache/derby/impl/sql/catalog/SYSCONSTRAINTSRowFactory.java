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

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubCheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.IndexDescriptor;

/**
 * Factory for creating a SYSCONTRAINTS row.
 *
 * @author jerry
 */

public class SYSCONSTRAINTSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSCONSTRAINTS";

	protected static final int		SYSCONSTRAINTS_COLUMN_COUNT = 7;
	protected static final int		SYSCONSTRAINTS_CONSTRAINTID = 1;
	protected static final int		SYSCONSTRAINTS_TABLEID = 2;
	protected static final int		SYSCONSTRAINTS_CONSTRAINTNAME = 3;
	protected static final int		SYSCONSTRAINTS_TYPE = 4;
	protected static final int		SYSCONSTRAINTS_SCHEMAID = 5;
	protected static final int		SYSCONSTRAINTS_STATE = ConstraintDescriptor.SYSCONSTRAINTS_STATE_FIELD;
	protected static final int		SYSCONSTRAINTS_REFERENCECOUNT = 7;

	protected static final int		SYSCONSTRAINTS_INDEX1_ID = 0;
	protected static final int		SYSCONSTRAINTS_INDEX2_ID = 1;
	protected static final int		SYSCONSTRAINTS_INDEX3_ID = 2;

    private	static	final	boolean[]	uniqueness = {
		                                               true,
													   true,
													   false
	                                                 };

	private static final int[][] indexColumnPositions =
	{
		{SYSCONSTRAINTS_CONSTRAINTID},
		{SYSCONSTRAINTS_CONSTRAINTNAME, SYSCONSTRAINTS_SCHEMAID},
		{SYSCONSTRAINTS_TABLEID}
	};

	private static final String[][] indexColumnNames =
	{
		{"CONSTRAINTID"},
		{"CONSTRAINTNAME", "SCHEMAID"},
		{"TABLEID"}
	};

	private	static	final	String[]	uuids =
	{
		 "8000002f-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000036-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000031-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONSTRAINTS_INDEX1
		,"80000033-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONSTRAINTS_INDEX2
		,"80000035-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONSTRAINTS_INDEX3
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSCONSTRAINTSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSCONSTRAINTS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, indexColumnNames, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSCONTRAINTS row
	 *
	 * @param emptyRow	Make an empty row if this parameter is true
	 * @param constraint	constraint descriptor
	 *
	 * @return	Row suitable for inserting into SYSCONTRAINTS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		int						constraintIType;
		UUID					oid;
		String					constraintSType = null;
		String					constraintID = null;
		String					tableID = null;
		String					constraintName = null;
		String					schemaID = null;
		boolean					isEnabled = true;
		int						referenceCount = 0;

		if (td != null)
		{
			ConstraintDescriptor constraint = (ConstraintDescriptor)td;
			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			oid = constraint.getUUID();
			constraintID = oid.toString();

			oid = constraint.getTableId();
			tableID = oid.toString();

			constraintName = constraint.getConstraintName();

			constraintIType = constraint.getConstraintType();
			switch (constraintIType)
			{
			    case DataDictionary.PRIMARYKEY_CONSTRAINT:
				    constraintSType = "P";
					break;

			    case DataDictionary.UNIQUE_CONSTRAINT:
					constraintSType = "U";
					break;

			    case DataDictionary.CHECK_CONSTRAINT:
					constraintSType = "C";
					break;

			    case DataDictionary.FOREIGNKEY_CONSTRAINT:
					constraintSType = "F";
					break;

			    default:
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("invalid constraint type");
					}
			}

			schemaID = constraint.getSchemaDescriptor().getUUID().toString();
			isEnabled = constraint.isEnabled();
			referenceCount = constraint.getReferenceCount();
		}

		/* Insert info into sysconstraints */

		/* RESOLVE - It would be nice to require less knowledge about sysconstraints
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSCONSTRAINTS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSCONSTRAINTS_CONSTRAINTID, dvf.getCharDataValue(constraintID));

		/* 2nd column is TABLEID (UUID - char(36)) */
		row.setColumn(SYSCONSTRAINTS_TABLEID, dvf.getCharDataValue(tableID));

		/* 3rd column is NAME (varchar(128)) */
		row.setColumn(SYSCONSTRAINTS_CONSTRAINTNAME, dvf.getVarcharDataValue(constraintName));

		/* 4th column is TYPE (char(1)) */
		row.setColumn(SYSCONSTRAINTS_TYPE, dvf.getCharDataValue(constraintSType));

		/* 5th column is SCHEMAID (UUID - char(36)) */
		row.setColumn(SYSCONSTRAINTS_SCHEMAID, dvf.getCharDataValue(schemaID));

		/* 6th column is STATE (char(1)) */
		row.setColumn(SYSCONSTRAINTS_STATE, dvf.getCharDataValue(isEnabled ? "E" : "D"));

		/* 7th column is REFERENCED */
		row.setColumn(SYSCONSTRAINTS_REFERENCECOUNT, dvf.getDataValue(referenceCount));

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
		    case SYSCONSTRAINTS_INDEX1_ID:
				/* 1st column is CONSTRAINTID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));
				break;

		    case SYSCONSTRAINTS_INDEX2_ID:
				/* 1st column is CONSTRAINTNAME (varchar(128)) */
				row.setColumn(1, getDataValueFactory().getVarcharDataValue((String) null));

				/* 2nd column is SCHEMAID (UUID - char(36)) */
				row.setColumn(2, getDataValueFactory().getCharDataValue((String) null));

				break;

		    case SYSCONSTRAINTS_INDEX3_ID:
				/* 1st column is TABLEID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

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
	 * Make a ConstraintDescriptor out of a SYSCONSTRAINTS row
	 *
	 * @param row a SYSCONSTRAINTS row
	 * @param parentTupleDescriptor	Subconstraint descriptor with auxiliary info.
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
		ConstraintDescriptor constraintDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSCONSTRAINTS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSCONSTRAINTS row");
		}

		DataValueDescriptor	col;
		ConglomerateDescriptor conglomDesc;
		DataDescriptorGenerator ddg;
		TableDescriptor		td = null;
		int					constraintIType = -1;
		int[]				keyColumns = null;
		UUID				constraintUUID;
		UUID				schemaUUID;
		UUID				tableUUID;
		UUID				referencedConstraintId = null; 
		SchemaDescriptor	schema;
		String				tableUUIDString;
		String				constraintName;
		String				constraintSType;
		String				constraintStateStr;
		boolean				constraintEnabled;
		int					referenceCount;
		String				constraintUUIDString;
		String				schemaUUIDString;
		SubConstraintDescriptor scd;

		if (SanityManager.DEBUG)
		{
			if (!(parentTupleDescriptor instanceof SubConstraintDescriptor))
			{
				SanityManager.THROWASSERT(
					"parentTupleDescriptor expected to be instanceof " +
					"SubConstraintDescriptor, not " +
					parentTupleDescriptor.getClass().getName());
			}
		}

		scd = (SubConstraintDescriptor) parentTupleDescriptor;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_TABLEID);
		tableUUIDString = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);

		/* Get the TableDescriptor.  
		 * It may be cached in the SCD, 
		 * otherwise we need to go to the
		 * DD.
		 */
		if (scd != null)
		{
			td = scd.getTableDescriptor();
		}
		if (td == null)
		{
			td = dd.getTableDescriptor(tableUUID);
		}

		/* 3rd column is NAME (varchar(128)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTNAME);
		constraintName = col.getString();

		/* 4th column is TYPE (char(1)) */
		col = row.getColumn(SYSCONSTRAINTS_TYPE);
		constraintSType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(constraintSType.length() == 1, 
				"Fourth column type incorrect");
		}

		boolean typeSet = false;
		switch (constraintSType.charAt(0))
		{
			case 'P' : 
				constraintIType = DataDictionary.PRIMARYKEY_CONSTRAINT;
				typeSet = true;
				// fall through

			case 'U' :
				if (! typeSet)
				{
					constraintIType = DataDictionary.UNIQUE_CONSTRAINT;
					typeSet = true;
				}
				// fall through

			case 'F' :
				if (! typeSet)
					constraintIType = DataDictionary.FOREIGNKEY_CONSTRAINT;
				if (SanityManager.DEBUG)
				{
					if (!(parentTupleDescriptor instanceof SubKeyConstraintDescriptor))
					{
						SanityManager.THROWASSERT(
						"parentTupleDescriptor expected to be instanceof " +
						"SubKeyConstraintDescriptor, not " +
						parentTupleDescriptor.getClass().getName());
					}
				}
				conglomDesc = td.getConglomerateDescriptor( 
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId());
				/* Take care the rare case of conglomDesc being null.  The
				 * reason is that our "td" is out of date.  Another thread
				 * which was adding a constraint committed between the moment
				 * we got the table descriptor (conglomerate list) and the
				 * moment we scanned and got the constraint desc list.  Since
				 * that thread just added a new row to SYSCONGLOMERATES, 
				 * SYSCONSTRAINTS, etc.  We wouldn't have wanted to lock the
				 * system tables just to prevent other threads from adding new
				 * rows.
				 */
				if (conglomDesc == null)
				{
					// we can't be getting td from cache because if we are
					// here, we must have been in dd's ddl mode (that's why
					// the ddl thread went through), we are not done yet, the
					// dd ref count is not 0, hence it couldn't have turned
					// into COMPILE_ONLY mode
					td = dd.getTableDescriptor(tableUUID);
					if (scd != null)
						scd.setTableDescriptor(td);
					// try again now
					conglomDesc = td.getConglomerateDescriptor( 
									((SubKeyConstraintDescriptor) 
										parentTupleDescriptor).getIndexId());
				}

				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(conglomDesc != null,
					"conglomDesc is expected to be non-null for backing index");
				}
				keyColumns = conglomDesc.getIndexDescriptor().baseColumnPositions();
				referencedConstraintId = ((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getKeyConstraintId();
				keyColumns = conglomDesc.getIndexDescriptor().baseColumnPositions();
				break;

			case 'C' :
				constraintIType = DataDictionary.CHECK_CONSTRAINT;
				if (SanityManager.DEBUG)
				{
					if (!(parentTupleDescriptor instanceof SubCheckConstraintDescriptor))
					{
						SanityManager.THROWASSERT("parentTupleDescriptor expected to be instanceof " +
						"SubCheckConstraintDescriptor, not " +
						parentTupleDescriptor.getClass().getName());
					}
				}
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Fourth column value invalid");
				}
		}

		/* 5th column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_SCHEMAID);
		schemaUUIDString = col.getString();
		schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);

		schema = dd.getSchemaDescriptor(schemaUUID, null);

		/* 6th column is STATE (char(1)) */
		col = row.getColumn(SYSCONSTRAINTS_STATE);
		constraintStateStr = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(constraintStateStr.length() == 1, 
				"Sixth column (state) type incorrect");
		}

		switch (constraintStateStr.charAt(0))
		{
			case 'E': 
				constraintEnabled = true;
				break;
			case 'D':
				constraintEnabled = false;
				break;
			default: 
				constraintEnabled = true;
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Invalidate state value '"
							+constraintStateStr+ "' for constraint");
				}
		}

		/* 7th column is REFERENCECOUNT, boolean */
		col = row.getColumn(SYSCONSTRAINTS_REFERENCECOUNT);
		referenceCount = col.getInt();
		
		/* now build and return the descriptor */

		switch (constraintIType)
		{
			case DataDictionary.PRIMARYKEY_CONSTRAINT : 
				constraintDesc = ddg.newPrimaryKeyConstraintDescriptor(
										td, 
										constraintName, 
										false, //deferable,
										false, //initiallyDeferred,
										keyColumns,//genReferencedColumns(dd, td), //int referencedColumns[],
										constraintUUID,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId(),
										schema,
										constraintEnabled,
										referenceCount);
				break;

			case DataDictionary.UNIQUE_CONSTRAINT :
				constraintDesc = ddg.newUniqueConstraintDescriptor(
										td, 
										constraintName, 
										false, //deferable,
										false, //initiallyDeferred,
										keyColumns,//genReferencedColumns(dd, td), //int referencedColumns[],
										constraintUUID,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId(),
										schema,
										constraintEnabled,
										referenceCount);
				break;

			case DataDictionary.FOREIGNKEY_CONSTRAINT : 
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(referenceCount == 0, 
						"REFERENCECOUNT column is nonzero for fk constraint");
				}
					
				constraintDesc = ddg.newForeignKeyConstraintDescriptor(
										td, 
										constraintName, 
										false, //deferable,
										false, //initiallyDeferred,
										keyColumns,//genReferencedColumns(dd, td), //int referencedColumns[],
										constraintUUID,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getIndexId(),
										schema,
										referencedConstraintId,
										constraintEnabled,
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getRaDeleteRule(),
										((SubKeyConstraintDescriptor) 
											parentTupleDescriptor).getRaUpdateRule()
										);
				break;

			case DataDictionary.CHECK_CONSTRAINT :
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(referenceCount == 0, 
						"REFERENCECOUNT column is nonzero for check constraint");
				}
					
				constraintDesc = ddg.newCheckConstraintDescriptor(
										td, 
										constraintName, 
										false, //deferable,
										false, //initiallyDeferred,
										constraintUUID,
										((SubCheckConstraintDescriptor) 
											parentTupleDescriptor).getConstraintText(),
										((SubCheckConstraintDescriptor) 
											parentTupleDescriptor).getReferencedColumnsDescriptor(),
										schema,
										constraintEnabled);
				break;
		}
		return constraintDesc;
	}

	/**
	 * Get the constraint ID of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The constraint id
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getConstraintId(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				constraintUUIDString;

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(constraintUUIDString);
	 }

	/**
	 * Get the constraint name of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The constraint name
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected String getConstraintName(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				constraintName;

		/* 3rd column is CONSTRAINTNAME (char(128)) */
		col = row.getColumn(SYSCONSTRAINTS_CONSTRAINTNAME);
		constraintName = col.getString();
		return constraintName;
	 }

	/**
	 * Get the schema ID of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The schema
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getSchemaId(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				schemaUUIDString;

		/* 5th column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_SCHEMAID);
		schemaUUIDString =col.getString();
		return getUUIDFactory().recreateUUID(schemaUUIDString);
	 }

	/**
	 * Get the table ID of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return UUID	The table id
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getTableId(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				tableUUIDString;

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSCONSTRAINTS_TABLEID);
		tableUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(tableUUIDString);
	 }

	/**
	 * Get the constraint type out of the row.
	 * 
	 * @param row	The row from sysconstraints
	 *
	 * @return int	The constraint type	as an int
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected int getConstraintType(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		int					constraintIType;
		String				constraintSType;

		/* 4th column is TYPE (char(1)) */
		col = row.getColumn(SYSCONSTRAINTS_TYPE);
		constraintSType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(constraintSType.length() == 1, 
				"Fourth column type incorrect");
		}

		switch (constraintSType.charAt(0))
		{
			case 'P' : 
				constraintIType = DataDictionary.PRIMARYKEY_CONSTRAINT;
				break;

			case 'U' :
				constraintIType = DataDictionary.UNIQUE_CONSTRAINT;
				break;

			case 'C' :
				constraintIType = DataDictionary.CHECK_CONSTRAINT;
				break;

			case 'F' :
				constraintIType = DataDictionary.FOREIGNKEY_CONSTRAINT;
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("Fourth column value invalid");
				}
				constraintIType = -1;
		}

		return constraintIType;
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
		SystemColumn[]			columnList = new SystemColumn[SYSCONSTRAINTS_COLUMN_COUNT];

		// describe columns

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "CONSTRAINTID"),			// column name
							SYSCONSTRAINTS_CONSTRAINTID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "TABLEID"),			// column name
							SYSCONSTRAINTS_TABLEID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );

		columnList[index++] =
					new SystemColumnImpl(		// SQL IDENTIFIER
							convertIdCase( "CONSTRAINTNAME"),	// column name
							SYSCONSTRAINTS_CONSTRAINTNAME,
							false				// nullability
							);

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "TYPE"),			// column name
							SYSCONSTRAINTS_TYPE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			               );


		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "SCHEMAID"),		// column name
							SYSCONSTRAINTS_SCHEMAID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "STATE"),		// column name
							SYSCONSTRAINTS_STATE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			               );

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "REFERENCECOUNT"),		// column name
							SYSCONSTRAINTS_REFERENCECOUNT,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"INTEGER",				// dataType
							true,				// built-in type
							1					// maxLength
			               );
		return	columnList;
	}

}
