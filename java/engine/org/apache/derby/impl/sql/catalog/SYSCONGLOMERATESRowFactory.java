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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

import org.apache.derby.catalog.IndexDescriptor;

import java.util.Properties;

/**
 * Factory for creating a SYSCONGLOMERATES row.
 *
 * @author ames
 */

public class SYSCONGLOMERATESRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSCONGLOMERATES";

	protected static final int		SYSCONGLOMERATES_COLUMN_COUNT = 8;
	protected static final int		SYSCONGLOMERATES_SCHEMAID = 1;
	protected static final int		SYSCONGLOMERATES_TABLEID = 2;
	protected static final int		SYSCONGLOMERATES_CONGLOMERATENUMBER = 3;
	protected static final int		SYSCONGLOMERATES_CONGLOMERATENAME = 4;
	protected static final int		SYSCONGLOMERATES_ISINDEX = 5;
	protected static final int		SYSCONGLOMERATES_DESCRIPTOR = 6;
	protected static final int		SYSCONGLOMERATES_ISCONSTRAINT = 7;
	protected static final int		SYSCONGLOMERATES_CONGLOMERATEID = 8;

	protected static final int		SYSCONGLOMERATES_INDEX1_ID = 0;
	protected static final int		SYSCONGLOMERATES_INDEX2_ID = 1;
	protected static final int		SYSCONGLOMERATES_INDEX3_ID = 2;

    private	static	final	boolean[]	uniqueness = {
		                                               false,
													   true,
													   false
	                                                 };

	private static final int[][] indexColumnPositions =
	{
		{SYSCONGLOMERATES_CONGLOMERATEID},
		{SYSCONGLOMERATES_CONGLOMERATENAME, SYSCONGLOMERATES_SCHEMAID},
		{SYSCONGLOMERATES_TABLEID}
	};

	private static final String[][] indexColumnNames =
	{
		{"CONGLOMERATE_ID"},
		{"CONGLOMERATE_NAME", "SCHEMAID"},
		{"TABLEID"}
	};

	private	static	final	String[]	uuids =
	{
		 "80000010-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000027-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000012-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONGLOMERATES_INDEX1
		,"80000014-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONGLOMERATES_INDEX2
		,"80000016-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONGLOMERATES_INDEX3
	};

	SYSCONGLOMERATESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSCONGLOMERATES_COLUMN_COUNT, 
				 TABLENAME_STRING, indexColumnPositions, 
				 indexColumnNames, uniqueness, uuids );
	}

  /**
	 * Make a SYSCONGLOMERATES row
	 *
	 * @param emptyRow	Make an empty row if this parameter is true
	 * @param sd		Schema Descriptor
	 * @param conglomerate	conglomerate descriptor
	 *
	 * @return	Row suitable for inserting into SYSCONGLOMERATES.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException
	{
		ExecRow    				row;
		DataValueDescriptor		col;
		String					tabID =null;
		Long					conglomNumber = null;
		String					conglomName = null;
		Boolean					supportsIndex = null;
		IndexRowGenerator		indexRowGenerator = null;
		Boolean					supportsConstraint = null;
		String					conglomUUIDString = null;
		String					schemaID = null;
		ConglomerateDescriptor  conglomerate = (ConglomerateDescriptor)td;

		/* Insert info into sysconglomerates */

		if (td != null)
		{
			/* Sometimes the SchemaDescriptor is non-null and sometimes it
			 * is null.  (We can't just rely on getting the schema id from 
			 * the ConglomerateDescriptor because it can be null when
			 * we are creating a new conglomerate.
			 */
			if (parent != null)
			{
				SchemaDescriptor sd = (SchemaDescriptor)parent;
				schemaID = sd.getUUID().toString();	
			}
			else
			{
				schemaID = conglomerate.getSchemaID().toString();	
			}
			tabID = conglomerate.getTableID().toString();
			conglomNumber = new Long( conglomerate.getConglomerateNumber() );
			conglomName = conglomerate.getConglomerateName();
			conglomUUIDString = conglomerate.getUUID().toString();

			supportsIndex = new Boolean( conglomerate.isIndex() );
			indexRowGenerator = conglomerate.getIndexDescriptor();
			supportsConstraint = new Boolean( conglomerate.isConstraint() );
		}

		/* RESOLVE - It would be nice to require less knowledge about sysconglomerates
		 * and have this be more table driven.
		 */

		/* Build the row to insert */
		row = getExecutionFactory().getValueRow(SYSCONGLOMERATES_COLUMN_COUNT);

		/* 1st column is SCHEMAID (UUID - char(36)) */
		row.setColumn(1, dvf.getCharDataValue(schemaID));

		/* 2nd column is TABLEID (UUID - char(36)) */
		row.setColumn(2, dvf.getCharDataValue(tabID));

		/* 3rd column is CONGLOMERATENUMBER (long) */
		row.setColumn(3, dvf.getDataValue(conglomNumber));

		/* 4th column is CONGLOMERATENAME (varchar(128)) 
		** If null, use the tableid so we always
		** have a unique column
		*/
		row.setColumn(4, (conglomName == null) ?
				dvf.getVarcharDataValue(tabID):
				dvf.getVarcharDataValue(conglomName));

		/* 5th  column is ISINDEX (boolean) */
		row.setColumn(5, dvf.getDataValue(supportsIndex));

		/* 6th column is DESCRIPTOR
		*  (user type org.apache.derby.catalog.IndexDescriptor)
		*/
		row.setColumn(6,
			dvf.getDataValue(
						(indexRowGenerator == null ?
							(IndexDescriptor) null :
							indexRowGenerator.getIndexDescriptor()
						)
					)
				);

		/* 7th column is ISCONSTRAINT (boolean) */
		row.setColumn(7, dvf.getDataValue(supportsConstraint));

		/* 8th column is CONGLOMERATEID (UUID - char(36)) */
		row.setColumn(8, dvf.getCharDataValue(conglomUUIDString));

		return row;
	}

	public ExecRow makeEmptyRow() throws StandardException
	{
		return makeRow(null, null);
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
		ExecIndexRow row =	getExecutionFactory().getIndexableRow(ncols + 1);

		row.setColumn(ncols + 1, rowLocation);

		switch( indexNumber )
		{
		    case SYSCONGLOMERATES_INDEX1_ID:
				
				/* 1st column is CONGLOMERATEID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));
				break;

		    case SYSCONGLOMERATES_INDEX2_ID:
				
				/* 1st column is CONGLOMERATENAME (varchar(128)) */
				row.setColumn(1, getDataValueFactory().getVarcharDataValue((String) null));
				
				/* 2nd column is SCHEMAID (char(36)) */
				row.setColumn(2, getDataValueFactory().getCharDataValue((String) null));

				break;

		    case SYSCONGLOMERATES_INDEX3_ID:
				
				/* Build the row */
				/* NOTE: this index is not unique, need extra column in template for
				 * drop method in DataDictionary.
				 */

				/* 1st column is TABLEID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

				break;

		}	// end switch

		return	row;
	}

	/**
	 * Get the Properties associated with creating the heap.
	 *
	 * @return The Properties associated with creating the heap.
	 */
	public Properties getCreateHeapProperties()
	{
		Properties properties = new Properties();
		// keep page size at 4K since its a big table
		properties.put("derby.storage.pageSize","4096");
		// default properties for system tables:
		properties.put("derby.storage.pageReservedSpace","0");
		properties.put("derby.storage.minimumRecordSize","1");
		return properties;
	}

	/**
	 * Get the Properties associated with creating the specified index.
	 *
	 * @param indexNumber	The specified index number.
	 *
	 * @return The Properties associated with creating the specified index.
	 */
	public Properties getCreateIndexProperties(int indexNumber)
	{
		Properties properties = new Properties();
		// keep page size for all indexes at 4K since its a big table
		properties.put("derby.storage.pageSize","4096");
		return properties;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 *
	 * @param row a SYSCOLUMNS row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @return	a conglomerate descriptor equivalent to a SYSCONGOMERATES row
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
		SanityManager.ASSERT(
			row.nColumns() == SYSCONGLOMERATES_COLUMN_COUNT, 
			"Wrong number of columns for a SYSCONGLOMERATES row");

		DataDescriptorGenerator	ddg = dd.getDataDescriptorGenerator();
		long conglomerateNumber;
		String	name;
		boolean isConstraint;
		boolean isIndex;
		IndexRowGenerator	indexRowGenerator;
		DataValueDescriptor col;
		ConglomerateDescriptor conglomerateDesc;
		String		conglomUUIDString;
		UUID		conglomUUID;
		String		schemaUUIDString;
		UUID		schemaUUID;
		String		tableUUIDString;
		UUID		tableUUID;

		/* 1st column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(1);
		schemaUUIDString = col.getString();
		schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(2);
		tableUUIDString = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);


		/* 3nd column is CONGLOMERATENUMBER (long) */
		col = row.getColumn(3);
		conglomerateNumber = col.getLong();

		/* 4rd column is CONGLOMERATENAME (varchar(128)) */
		col = row.getColumn(4);
		name = col.getString();

		/* 5th column is ISINDEX (boolean) */
		col = row.getColumn(5);
		isIndex = col.getBoolean();

		/* 6th column is DESCRIPTOR */
		col = row.getColumn(6);
		indexRowGenerator = new IndexRowGenerator(
			(IndexDescriptor) col.getObject());

		/* 7th column is ISCONSTRAINT (boolean) */
		col = row.getColumn(7);
		isConstraint = col.getBoolean();

		/* 8th column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(8);
		conglomUUIDString = col.getString();
		conglomUUID = getUUIDFactory().recreateUUID(conglomUUIDString);

		/* now build and return the descriptor */
		conglomerateDesc = ddg.newConglomerateDescriptor(conglomerateNumber,
														 name,
														 isIndex,
														 indexRowGenerator,
														 isConstraint,
														 conglomUUID,
														 tableUUID,
														 schemaUUID);
		return conglomerateDesc;
	}

	/**
	 * Get the conglomerate's UUID of the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return UUID	The conglomerates UUID
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getConglomerateUUID(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				conglomerateUUIDString;

		/* 8th column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(SYSCONGLOMERATES_CONGLOMERATEID);
		conglomerateUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(conglomerateUUIDString);
	 }

	/**
	 * Get the table's UUID from the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return UUID	The table's UUID
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getTableUUID(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				tableUUIDString;

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSCONGLOMERATES_TABLEID);
		tableUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(tableUUIDString);
	 }

	/**
	 * Get the schema's UUID from the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return UUID	The schema's UUID
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getSchemaUUID(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				schemaUUIDString;

		/* 1st column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSCONGLOMERATES_SCHEMAID);
		schemaUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(schemaUUIDString);
	 }

	/**
	 * Get the conglomerate's name of the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return String	The conglomerates name
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected String getConglomerateName(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;

		/* 4th column is CONGLOMERATENAME (varchar(128)) */
		col = row.getColumn(SYSCONGLOMERATES_CONGLOMERATENAME);
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
		int						index = 0;
		SystemColumn[]			columnList = new SystemColumn[SYSCONGLOMERATES_COLUMN_COUNT];

		// describe columns


		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "SCHEMAID"),			// column name
							SYSCONGLOMERATES_SCHEMAID,	// column number
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
							SYSCONGLOMERATES_TABLEID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );
		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "CONGLOMERATENUMBER"),		// column name
							SYSCONGLOMERATES_CONGLOMERATENUMBER,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"BIGINT",				// dataType
							true,				// built-in type
							TypeId.LONGINT_MAXWIDTH	// maxLength
			               );

		columnList[index++] =
					new SystemColumnImpl(		// SQL IDENTIFIER
							convertIdCase( "CONGLOMERATENAME"),				// column name
							SYSCONGLOMERATES_CONGLOMERATENAME,
							true				// nullability
							);

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "ISINDEX"),			// column name
							SYSCONGLOMERATES_ISINDEX,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"BOOLEAN",				// dataType
							true,				// built-in type
							1					// maxLength
			               );

		columnList[index++] =
					new SystemColumnImpl(
							convertIdCase( "DESCRIPTOR"),				// column name
							SYSCONGLOMERATES_DESCRIPTOR,
							0,					// precision
							0,					// scale
							true,				// nullability
							"org.apache.derby.catalog.IndexDescriptor",	// datatype
							false,				// built-in type
							TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN
												// maxLength
							);

		columnList[index++] =
					new SystemColumnImpl(
							convertIdCase( "ISCONSTRAINT"),				// column name
							SYSCONGLOMERATES_ISCONSTRAINT,
							0,					// precision
							0,					// scale
							true,				// nullability
							"BOOLEAN",	// datatype
							true,				// built-in type
							1					// maxLength
							);

		columnList[index++] =
					new SystemColumnImpl(
							convertIdCase( "CONGLOMERATEID"),	// column name
							SYSCONGLOMERATES_CONGLOMERATEID,
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// datatype
							true,				// built-in type
							36					// maxLength
							);

		return	columnList;

	}
}
