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

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.IndexDescriptor;

import java.util.Properties;

/**
 * Factory for creating a SYSVIEWS row.
 *
 * @author jerry
 */

public class SYSVIEWSRowFactory extends CatalogRowFactory
{
	private  static final String	TABLENAME_STRING = "SYSVIEWS";

	protected static final int		SYSVIEWS_COLUMN_COUNT = 4;
	protected static final int		SYSVIEWS_TABLEID = 1;
	protected static final int		SYSVIEWS_VIEWDEFINITION = 2;
	protected static final int		SYSVIEWS_CHECKOPTION = 3;
	protected static final int		SYSVIEWS_COMPILATION_SCHEMAID = 4;

	// Column widths
	protected static final int		SYSVIEWS_TABLEID_WIDTH = 36;

	protected static final int		SYSVIEWS_INDEX1_ID = 0;

	private static final int[][] indexColumnPositions =
	{
		{SYSVIEWS_TABLEID}
	};

	private static final String[][] indexColumnNames =
	{
		{"TABLEID"}
	};

	// if you add a non-unique index allocate this array.
    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		 "8000004d-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000050-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000004f-00d0-fd77-3ed8-000a0a0b1900"	// SYSVIEWS_INDEX1
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSVIEWSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                               boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSVIEWS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, indexColumnNames, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSVIEWS row
	 *
	 * @param emptyRow		Make an empty row if this parameter is true
	 * @param vd			View descriptor
	 *
	 * @return	Row suitable for inserting into SYSVIEWS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
		throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		String					tableID = null;
		String					compSchemaId = null;
		String					viewText = null;
		String	   				checkSType = null;
		int	   					checkIType;

		if (td != null)
		{
			UUID	tableUUID;
			ViewDescriptor vd = (ViewDescriptor)td;

			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			tableUUID = vd.getUUID();
			if ( tableUUID == null )
		    {
				tableUUID = getUUIDFactory().createUUID();
				vd.setUUID(tableUUID);
			}
			tableID = tableUUID.toString();
			viewText = vd.getViewText();

			/* RESOLVE - check constraints not supported yet */
			checkIType = vd.getCheckOptionType();

			if (SanityManager.DEBUG)
			{
				if (checkIType != ViewDescriptor.NO_CHECK_OPTION)
				{
					SanityManager.THROWASSERT("checkIType expected to be " + 
						ViewDescriptor.NO_CHECK_OPTION +
						", not " + checkIType);
				}
			}
			checkSType = "N";

			UUID tmpId = vd.getCompSchemaId();
			compSchemaId = (tmpId == null) ? null : tmpId.toString();
		}

		/* Insert info into sysviews */

		/* RESOLVE - It would be nice to require less knowledge about sysviews
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSVIEWS_COLUMN_COUNT);

		/* 1st column is TABLEID (UUID - char(36)) */
		row.setColumn(SYSVIEWS_TABLEID, dvf.getCharDataValue(tableID));

		/* 2nd column is VIEWDEFINITION */
		row.setColumn(SYSVIEWS_VIEWDEFINITION,
				dvf.getLongvarcharDataValue(viewText));

		/* 3rd column is CHECKOPTION (char(1)) */
		row.setColumn(SYSVIEWS_CHECKOPTION, dvf.getCharDataValue(checkSType));

		/* 4th column is COMPILATIONSCHEMAID (UUID - char(36)) */
		row.setColumn(SYSVIEWS_COMPILATION_SCHEMAID, dvf.getCharDataValue(compSchemaId));

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
		if (SanityManager.DEBUG)
			checkIndexNumber(indexNumber);
		/* Build the row  */
		ExecIndexRow row = getExecutionFactory().getIndexableRow(2);

		/* 1st column is TABLEID (char(36)) */
		row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

		row.setColumn(2, rowLocation);

		return	row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ViewDescriptor out of a SYSVIEWS row
	 *
	 * @param row a SYSVIEWS row
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
		ViewDescriptor vd = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSVIEWS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSVIEWS row");
		}

		DataValueDescriptor	col;
		DataDescriptorGenerator ddg;
		int					checkIType;
		String				checkSType;
		String				tableID;
		String				compSchemaId;
		String				viewDefinition;
		UUID				tableUUID;
		UUID				compSchemaUUID = null;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSVIEWS_TABLEID);
		tableID = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableID);

		/* 2nd column is VIEWDEFINITION */
		col = row.getColumn(SYSVIEWS_VIEWDEFINITION);
		viewDefinition = col.getString();

		/* 3rd column is CHECKOPTION (char(1)) */
		col = row.getColumn(SYSVIEWS_CHECKOPTION);
		checkSType = col.getString();

		if (SanityManager.DEBUG)
		{
			if (!checkSType.equals("N"))
			{
				SanityManager.THROWASSERT("checkSType expected to be 'N', not " + checkSType);
			}
		}

		/* RESOLVE - no check options for now */
		checkIType = ViewDescriptor.NO_CHECK_OPTION;

		/* 4th column is COMPILATIONSCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSVIEWS_COMPILATION_SCHEMAID);
		compSchemaId = col.getString();
		if (compSchemaId != null)
		{
			compSchemaUUID = getUUIDFactory().recreateUUID(compSchemaId);
		}

		/* now build and return the descriptor */
		vd = ddg.newViewDescriptor(tableUUID, null, viewDefinition, 
				checkIType, compSchemaUUID);
		return vd;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
	{
		SystemColumn[]			columnList = new SystemColumn[SYSVIEWS_COLUMN_COUNT];

		columnList[0] = new SystemColumnImpl(	
							convertIdCase( "TABLEID"),			// name 
							SYSVIEWS_TABLEID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );
		// describe columns

		columnList[1] = 
					new SystemColumnImpl(	
							convertIdCase( "VIEWDEFINITION"),		// column name
							SYSVIEWS_VIEWDEFINITION,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"LONG VARCHAR",	    // dataType
							true,				// built-in type
							TypeId.LONGVARCHAR_MAXWIDTH // maxLength
			               );
		columnList[2] = 
					new SystemColumnImpl(	
							convertIdCase( "CHECKOPTION"),		// column name
							SYSVIEWS_CHECKOPTION,// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
		                   );

		columnList[3] = new SystemColumnImpl(	
							convertIdCase( "COMPILATIONSCHEMAID"),	// name 
							SYSVIEWS_COMPILATION_SCHEMAID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );
		return	columnList;
	}

	public int heapColumnCount()
	{
		return SYSVIEWS_COLUMN_COUNT;
	}
}
