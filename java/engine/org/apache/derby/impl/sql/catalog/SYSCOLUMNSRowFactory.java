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

import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.BaseTypeIdImpl;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.UniqueTupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.catalog.types.TypeDescriptorImpl;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

import org.apache.derby.catalog.types.DefaultInfoImpl;

import org.apache.derby.iapi.types.*;

import java.io.Serializable;

import java.util.Properties;

/**
 * Factory for creating a SYSCOLUMNS row.
 *
 *
 * @version 0.1
 * @author Rick Hillegas (extracted from DataDictionaryImpl).
 */

public class SYSCOLUMNSRowFactory extends CatalogRowFactory
{
	static final String		TABLENAME_STRING = "SYSCOLUMNS";

	/**
	 * Old name for REFERENCEID, used by upgrade
	 */
	public static final String		OLD_REFERENCEID_NAME = "TABLEID";

	protected static final int		SYSCOLUMNS_COLUMN_COUNT = 9;
	/* Column #s for syscolumns (1 based) */

	//TABLEID is an obsolete name, it is better to use 
	//REFERENCEID, but to make life easier you can use either
	protected static final int		SYSCOLUMNS_TABLEID = 1;
	protected static final int		SYSCOLUMNS_REFERENCEID = 1;
	protected static final int		SYSCOLUMNS_COLUMNNAME = 2;
	protected static final int		SYSCOLUMNS_COLUMNNUMBER = 3;
	protected static final int		SYSCOLUMNS_COLUMNDATATYPE = 4;
	protected static final int		SYSCOLUMNS_COLUMNDEFAULT = 5;
	protected static final int		SYSCOLUMNS_COLUMNDEFAULTID = 6;
	protected static final int 		SYSCOLUMNS_AUTOINCREMENTVALUE = 7;
	protected static final int 		SYSCOLUMNS_AUTOINCREMENTSTART = 8;
	protected static final int		SYSCOLUMNS_AUTOINCREMENTINC = 9;

	//private static final String	SYSCOLUMNS_INDEX1_NAME = "SYSCOLUMNS_INDEX1";
	protected static final int		SYSCOLUMNS_INDEX1_ID = 0;

	//private static final String	SYSCOLUMNS_INDEX2_NAME = "SYSCOLUMNS_INDEX2";
	protected static final int		SYSCOLUMNS_INDEX2_ID = 1;

	protected	static	final	String	REFERENCEDID_STRING = "REFERENCEID";
	protected	static	final	String	COLUMNNAME_STRING = "COLUMNNAME";
	protected	static	final	String	COLUMNDEFAULTID_STRING = "COLUMNDEFAULTID";

    private	static	final	boolean[]	uniqueness = {
		                                               true,
													   false
	                                                 };

	private	static	final	String[]	uuids =
	{
		 "8000001e-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000029-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000020-00d0-fd77-3ed8-000a0a0b1900"	// SYSCOLUMNS_INDEX1 UUID
		,"6839c016-00d9-2829-dfcd-000a0a411400"	// SYSCOLUMNS_INDEX2 UUID
	};

	private static final int[][] indexColumnPositions = 
	{
		{SYSCOLUMNS_REFERENCEID, SYSCOLUMNS_COLUMNNAME},
		{SYSCOLUMNS_COLUMNDEFAULTID}
	};

	private static final String[][] indexColumnNames =
	{
		{REFERENCEDID_STRING, COLUMNNAME_STRING},
		{COLUMNDEFAULTID_STRING}
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////////////////////

	private	SystemColumn[]		columnList;

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSCOLUMNSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		this(uuidf, ef, dvf, convertIdToLower, TABLENAME_STRING);
	}

    public	SYSCOLUMNSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower, String myName )
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSCOLUMNS_COLUMN_COUNT, myName, indexColumnPositions, indexColumnNames, uniqueness, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSCOLUMNS row
	 *
	 * @param emptyRow	Make an empty row if this parameter is true
	 * @param tableID	Table that the column is in
	 * @param column	Column descriptor
	 *
	 * @return	Row suitable for inserting into SYSCOLUMNS.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException
	{
		ExecRow    				row;
		DataValueDescriptor		col;

		String					colName = null;
		String					defaultID = null;
		String					tabID = null;
		Integer					colID = null;
		TypeDescriptorImpl		typeDesc = null;
		Object					defaultSerializable = null;
		long					autoincStart = 0;
		long					autoincInc = 0;

		if (td != null)
		{
			ColumnDescriptor  column = (ColumnDescriptor)td;
		
			/* Lots of info in the column's type descriptor */
			typeDesc = column.getType().getCatalogType();

			tabID = column.getReferencingUUID().toString();
			colName = column.getColumnName();
			colID = new Integer(column.getPosition() );
			autoincStart = column.getAutoincStart();
			autoincInc   = column.getAutoincInc();
			if (column.getDefaultInfo() != null)
			{
				defaultSerializable = column.getDefaultInfo();
			}
			else
			{
				defaultSerializable = column.getDefaultValue();
			}
			if  (column.getDefaultUUID() != null)
			{
				defaultID = column.getDefaultUUID().toString();
			}
		}

		/* Insert info into syscolumns */

		/* RESOLVE - It would be nice to require less knowledge about syscolumns
		 * and have this be more table driven.
		 * RESOLVE - We'd like to store the DataTypeDescriptor in a column.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSCOLUMNS_COLUMN_COUNT);

		/* 1st column is REFERENCEID (UUID - char(36)) */
		row.setColumn(SYSCOLUMNS_REFERENCEID, dvf.getCharDataValue(tabID));

		/* 2nd column is COLUMNNAME (varchar(128)) */
		row.setColumn(SYSCOLUMNS_COLUMNNAME, dvf.getVarcharDataValue(colName));

		/* 3rd column is COLUMNNUMBER (int) */
		row.setColumn(SYSCOLUMNS_COLUMNNUMBER, dvf.getDataValue(colID));

		/* 4th column is COLUMNDATATYPE */
		row.setColumn(SYSCOLUMNS_COLUMNDATATYPE,
				dvf.getDataValue(typeDesc));

		/* 5th column is COLUMNDEFAULT */
		row.setColumn(SYSCOLUMNS_COLUMNDEFAULT,
					  dvf.getDataValue(defaultSerializable));

		/* 6th column is DEFAULTID (UUID - char(36)) */
		row.setColumn(SYSCOLUMNS_COLUMNDEFAULTID, dvf.getCharDataValue(defaultID));

		if (autoincInc != 0)
		{
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, 
						  new SQLLongint(autoincStart));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, 
						  new SQLLongint(autoincStart));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC, 
						  new SQLLongint(autoincInc));
		}
		else
		{
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, 
						  new SQLLongint());
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, 
						  new SQLLongint());
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC,
						  new SQLLongint());
		}
		return row;
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
		properties.put(Property.PAGE_SIZE_PARAMETER,"4096");
		// default properties for system tables:
		properties.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER,"0");
		properties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,"1");
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
		properties.put(Property.PAGE_SIZE_PARAMETER,"4096");
		return properties;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

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

		switch(indexNumber)
		{
			case SYSCOLUMNS_INDEX1_ID:
				/* 1st column is REFERENCEID (UUID - char(36)) */
				row.setColumn
					(1, getDataValueFactory().getCharDataValue((String) null));

				/* 2nd column is COLUMNNAME (varchar(128)) */
				row.setColumn
				    (2, 
					 getDataValueFactory().getVarcharDataValue((String) null));

				break;

		    case SYSCOLUMNS_INDEX2_ID:
				
				/* 1st column is DEFAULTID (UUID - char(36)) */
				row.setColumn
					(1, getDataValueFactory().getCharDataValue((String) null));

				break;
		}	// end switch

		return	row;
	}


	/**
	 * Make a ColumnDescriptor out of a SYSCOLUMNS row
	 *
	 * @param row 					a SYSCOLUMNS row
	 * @param parentTupleDescriptor	The UniqueTupleDescriptor for the object that is tied
	 *								to this column
	 * @param dd 					dataDictionary
	 *
	 * @return	a column descriptor equivalent to a SYSCOLUMNS row
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
		{
			SanityManager.ASSERT(row.nColumns() == SYSCOLUMNS_COLUMN_COUNT, 
								 "Wrong number of columns for a SYSCOLUMNS row");
		}

		int columnNumber;
		String columnName;
		String defaultID;
		DefaultInfoImpl		defaultInfo = null;
		ColumnDescriptor colDesc;
		BaseTypeIdImpl		typeId;
		TypeId	wrapperTypeId;
		DataValueDescriptor	defaultValue = null;
		UUID				defaultUUID = null;
		UUID				uuid = null;
		UUIDFactory			uuidFactory = getUUIDFactory();
		long autoincStart, autoincInc;

		DataDescriptorGenerator	ddg = dd.getDataDescriptorGenerator();


		/*
		** We're going to be getting the UUID for this sucka
		** so make sure it is a UniqueTupleDescriptor.
		*/
		if (parentTupleDescriptor != null)
		{
			if (SanityManager.DEBUG)
			{
				if (!(parentTupleDescriptor instanceof UniqueTupleDescriptor))
				{
					SanityManager.THROWASSERT(parentTupleDescriptor.getClass().getName() 
							+ " not instanceof UniqueTupleDescriptor");	
				}
			}
			uuid = ((UniqueTupleDescriptor)parentTupleDescriptor).getUUID();
		}
		else
		{
			/* 1st column is REFERENCEID (char(36)) */
			uuid = uuidFactory.recreateUUID(row.getColumn(SYSCOLUMNS_REFERENCEID).
													getString());
		}

		/* NOTE: We get columns 5 and 6 next in order to work around 
		 * a 1.3.0 HotSpot bug.  (#4361550)
		 */

		// 5th column is COLUMNDEFAULT (serialiazable)
		Object object = row.getColumn(SYSCOLUMNS_COLUMNDEFAULT).getObject();
		if (object instanceof DataValueDescriptor)
		{
			defaultValue = (DataValueDescriptor) object;
		}
		else if (object instanceof DefaultInfoImpl)
		{
			defaultInfo = (DefaultInfoImpl) object;
			defaultValue = defaultInfo.getDefaultValue();
		}

		/* 6th column is DEFAULTID (char(36)) */
		defaultID = row.getColumn(SYSCOLUMNS_COLUMNDEFAULTID).getString();

		if (defaultID != null)
		{
			defaultUUID = uuidFactory.recreateUUID(defaultID);
		}

		/* 2nd column is COLUMNNAME (varchar(128)) */
		columnName = row.getColumn(SYSCOLUMNS_COLUMNNAME).getString();

		/* 3rd column is COLUMNNUMBER (int) */
		columnNumber = row.getColumn(SYSCOLUMNS_COLUMNNUMBER).getInt();

		/* 4th column is COLUMNDATATYPE */

		/*
		** What is stored in the column is a TypeDescriptorImpl, which
		** points to a BaseTypeIdImpl.  These are simple types that are
		** intended to be movable to the client, so they don't have
		** the entire implementation.  We need to wrap them in DataTypeServices
		** and TypeId objects that contain the full implementations for
		** language processing.
		*/
		TypeDescriptorImpl typeDescriptor = (TypeDescriptorImpl) row.getColumn(SYSCOLUMNS_COLUMNDATATYPE).
													getObject();
		typeId = typeDescriptor.getTypeId();

		/*
		** The BaseTypeIdImpl tells what type of TypeId it is supposed to
		** be wrapped in.
		*/
		wrapperTypeId =
			(TypeId) Monitor.newInstanceFromIdentifier(typeId.wrapperTypeFormatId());
		/* Wrap the BaseTypeIdImpl in a full type id */
		wrapperTypeId.setNestedTypeId(typeId);

		/* Wrap the TypeDescriptorImpl in a full DataTypeDescriptor */
		DataTypeDescriptor dataTypeServices = new DataTypeDescriptor(typeDescriptor,
													wrapperTypeId);

		/* 7th column is AUTOINCREMENTVALUE, not cached in descriptor (long) */

		/* 8th column is AUTOINCREMENTSTART (long) */
		autoincStart = row.getColumn(SYSCOLUMNS_AUTOINCREMENTSTART).getLong();

		/* 9th column is AUTOINCREMENTINC (long) */
		autoincInc = row.getColumn(SYSCOLUMNS_AUTOINCREMENTINC).getLong();

		/* NOTE: We use the autoincColumn variable in order to work around 
		 * a 1.3.0 HotSpot bug.  (#4361550)
		 */
		boolean autoincColumn = (autoincInc != 0); 

		DataValueDescriptor col = row.getColumn(SYSCOLUMNS_AUTOINCREMENTSTART);
		autoincStart = col.getLong();

		col = row.getColumn(SYSCOLUMNS_AUTOINCREMENTINC);
		autoincInc = col.getLong();

		colDesc = new ColumnDescriptor(columnName, columnNumber,
							dataTypeServices, defaultValue, defaultInfo, uuid, 
							defaultUUID, autoincStart, autoincInc, 
							autoincColumn);
		return colDesc;
	}

	/**
	  *	Get the index number for the primary key index on this catalog.
	  *
	  *	@return	a 0-based number
	  *
	  */
	public	int	getPrimaryKeyIndexNumber()
	{
		return SYSCOLUMNS_INDEX1_ID;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
	{
		if ( columnList != null ) { return columnList; }

		columnList = new SystemColumn[SYSCOLUMNS_COLUMN_COUNT];

		// describe columns

		columnList[0] = 
					new SystemColumnImpl(
								convertIdCase( REFERENCEDID_STRING),			// column name
								SYSCOLUMNS_REFERENCEID,// column number
								0,					// precision
								0,					// scale
								false,				// nullability
								"CHAR",				// dataType
								true,				// built-in type
								36					// maxLength
			                   );

		columnList[1] = 
					new SystemColumnImpl(			// SQL IDENTIFIER
								convertIdCase( COLUMNNAME_STRING),		// column name
								SYSCOLUMNS_COLUMNNAME,	// column number
								false				// nullability
			                   );

		columnList[2] = 
					new SystemColumnImpl(
								convertIdCase( "COLUMNNUMBER"),	// column name
								SYSCOLUMNS_COLUMNNUMBER,	// column number
								0,					// precision
								0,					// scale
								false,				// nullability
								"INTEGER",				// dataType
								true,				// built-in type
								4					// maxLength
							   );

		columnList[3] = 
					new SystemColumnImpl(	
							convertIdCase( "COLUMNDATATYPE"),			// column name
							SYSCOLUMNS_COLUMNDATATYPE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"org.apache.derby.catalog.TypeDescriptor",	    // dataType
							false,				// built-in type
							TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN // maxLength
			               );

		columnList[4] = 
					new SystemColumnImpl(	
							convertIdCase( "COLUMNDEFAULT"),			// column name
							SYSCOLUMNS_COLUMNDEFAULT,	// column number
							0,					// precision
							0,					// scale
							true,				// nullability
							"java.io.Serializable",	    // dataType
							false,				// built-in type
							TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN // maxLength
			               );

		columnList[5] = 
					new SystemColumnImpl(
								convertIdCase( COLUMNDEFAULTID_STRING),			// column name
								SYSCOLUMNS_COLUMNDEFAULTID,// column number
								0,					// precision
								0,					// scale
								true,				// nullability
								"CHAR",				// dataType
								true,				// built-in type
								36					// maxLength
			                   );

		// new columns for autoincrement.
		columnList[6] = 
			        new SystemColumnImpl(
							    convertIdCase( "AUTOINCREMENTVALUE"), // column name
								SYSCOLUMNS_AUTOINCREMENTVALUE,
								0,
								0, 
								true,
								"BIGINT",
								true,
								TypeId.LONGINT_MAXWIDTH
							   );
		
		columnList[7] = 
			        new SystemColumnImpl(
							    convertIdCase( "AUTOINCREMENTSTART"), // column name
								SYSCOLUMNS_AUTOINCREMENTSTART,
								0,
								0, 
								true,
								"BIGINT",
								true,
								TypeId.LONGINT_MAXWIDTH
							   );

		columnList[8] = 
			        new SystemColumnImpl(
							    convertIdCase( "AUTOINCREMENTINC"), // column name
								SYSCOLUMNS_AUTOINCREMENTINC,
								0,
								0, 
								true,
								"BIGINT",
								true,
								TypeId.LONGINT_MAXWIDTH
							   );

		return	columnList;
	}
}
