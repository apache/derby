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

import	org.apache.derby.catalog.Dependable;
import	org.apache.derby.catalog.DependableFinder;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.reference.SQLState;

/**
 * Factory for creating a SYSDEPENDSS row.
 *
 * @author jerry
 */

public class SYSDEPENDSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSDEPENDS";

	protected static final int		SYSDEPENDS_COLUMN_COUNT = 4;
	protected static final int		SYSDEPENDS_DEPENDENTID = 1;
	protected static final int		SYSDEPENDS_DEPENDENTTYPE = 2;
	protected static final int		SYSDEPENDS_PROVIDERID = 3;
	protected static final int		SYSDEPENDS_PROVIDERTYPE = 4;

	protected static final int		SYSDEPENDS_INDEX1_ID = 0;
	protected static final int		SYSDEPENDS_INDEX2_ID = 1;

	
    private	static	final	boolean[]	uniqueness = {
		                                               false,
													   false
	                                                 };

	private static final int[][] indexColumnPositions =
	{
		{SYSDEPENDS_DEPENDENTID},
		{SYSDEPENDS_PROVIDERID}
	};

	private static final String[][] indexColumnNames =
	{
		{"DEPENDENTID"},   
		{"PROVIDERID"}
	};

	private	static	final	String[]	uuids =
	{
		 "8000003e-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000043-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000040-00d0-fd77-3ed8-000a0a0b1900"	// SYSDEPENDS_INDEX1
		,"80000042-00d0-fd77-3ed8-000a0a0b1900"	// SYSDEPENDS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSDEPENDSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSDEPENDS_COLUMN_COUNT,TABLENAME_STRING, indexColumnPositions,
				 indexColumnNames, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSDEPENDS row
	 *
	 * @param td DependencyDescriptor. If its null then we want to make an empty
	 * row. 
	 *
	 * @return	Row suitable for inserting into SYSDEPENDS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		String					dependentID = null;
		DependableFinder		dependentBloodhound = null;
		String					providerID = null;
		DependableFinder		providerBloodhound = null;

		if (td != null)
		{
			DependencyDescriptor dd = (DependencyDescriptor)td;
			dependentID	= dd.getUUID().toString();
			dependentBloodhound = dd.getDependentFinder();
			if ( dependentBloodhound == null )
			{
				throw StandardException.newException(SQLState.DEP_UNABLE_TO_STORE);
			}

			providerID	= dd.getProviderID().toString();
			providerBloodhound = dd.getProviderFinder();
			if ( providerBloodhound == null )
			{
				throw StandardException.newException(SQLState.DEP_UNABLE_TO_STORE);
			}

		}

		/* Insert info into sysdepends */

		/* RESOLVE - It would be nice to require less knowledge about sysdepends
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSDEPENDS_COLUMN_COUNT);

		/* 1st column is DEPENDENTID (UUID - char(36)) */
		row.setColumn(SYSDEPENDS_DEPENDENTID, dvf.getCharDataValue(dependentID));

		/* 2nd column is DEPENDENTFINDER */
		row.setColumn(SYSDEPENDS_DEPENDENTTYPE,
				dvf.getDataValue(dependentBloodhound));

		/* 3rd column is PROVIDERID (UUID - char(36)) */
		row.setColumn(SYSDEPENDS_PROVIDERID, dvf.getCharDataValue(providerID));

		/* 4th column is PROVIDERFINDER */
		row.setColumn(SYSDEPENDS_PROVIDERTYPE,
				dvf.getDataValue(providerBloodhound));

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
		ExecIndexRow row = getExecutionFactory().getIndexableRow(2);

		/* both indices are on UUID */
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
	 * Make a ConstraintDescriptor out of a SYSDEPENDS row
	 *
	 * @param row a SYSDEPENDSS row
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
		DependencyDescriptor dependencyDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSDEPENDS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSDEPENDS row");
		}

		DataValueDescriptor	col;
		String				dependentIDstring;
		UUID				dependentUUID;
		DependableFinder	dependentBloodhound;
		String				providerIDstring;
		UUID				providerUUID;
		DependableFinder	providerBloodhound;

		/* 1st column is DEPENDENTID (UUID - char(36)) */
		col = row.getColumn(SYSDEPENDS_DEPENDENTID);
		dependentIDstring = col.getString();
		dependentUUID = getUUIDFactory().recreateUUID(dependentIDstring);

		/* 2nd column is DEPENDENTTYPE */
		col = row.getColumn(SYSDEPENDS_DEPENDENTTYPE);
		dependentBloodhound = (DependableFinder) col.getObject();

		/* 3rd column is PROVIDERID (UUID - char(36)) */
		col = row.getColumn(SYSDEPENDS_PROVIDERID);
		providerIDstring = col.getString();
		providerUUID = getUUIDFactory().recreateUUID(providerIDstring);

		/* 4th column is PROVIDERTYPE */
		col = row.getColumn(SYSDEPENDS_PROVIDERTYPE);
		providerBloodhound = (DependableFinder) col.getObject();

		/* now build and return the descriptor */
		return new DependencyDescriptor(dependentUUID, dependentBloodhound,
										   providerUUID, providerBloodhound);
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
		SystemColumn[]			columnList = new SystemColumn[SYSDEPENDS_COLUMN_COUNT];

		// describe columns

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "DEPENDENTID"),			// column name
							SYSDEPENDS_DEPENDENTID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "DEPENDENTFINDER"),		// column name
							SYSDEPENDS_DEPENDENTTYPE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"org.apache.derby.catalog.DependableFinder",	    // dataType
							false,				// built-in type
							TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN // maxLength
			               );

		columnList[index++] =
					new SystemColumnImpl(
							convertIdCase( "PROVIDERID"),		// column name
							SYSDEPENDS_PROVIDERID,
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// datatype
							true,				// built-in type
							36					// maxLength
							);

		columnList[index++] = 
					new SystemColumnImpl(	
							convertIdCase( "PROVIDERFINDER"),			// column name
							SYSDEPENDS_PROVIDERTYPE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"org.apache.derby.catalog.DependableFinder",	    // dataType
							false,				// built-in type
							TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN // maxLength
			               );

		return	columnList;
	}
}
