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
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.UUID;


/**
 * Factory for creating a SYSALIASES row.
 *
 * Here are the directions for adding a new system supplied alias.
 * Misc:
 *  All system supplied aliases are class aliases at this point.
 *	Additional arrays will need to be added if we supply system
 *	aliases of other types.
 *	The preloadAliasIDs array is an array of hard coded UUIDs
 *	for the system supplied aliases.
 *	The preloadAliases array is the array of aliases
 *	for the system supplied aliases.  This array is in alphabetical
 *	order by package and class in Xena.  Each alias is the uppercase
 *	class name of the alias.
 *  The preloadJavaClassNames array is the array of full package.class
 *  names for the system supplied aliases.  This array is in alphabetical
 *	order by package and class in Xena.  
 *	SYSALIASES_NUM_BOOT_ROWS is the number of boot rows in sys.sysaliases
 *  in a new database.
 *
 *
 * @author jerry
 */

class SYSALIASESRowFactory extends CatalogRowFactory
{

	private static final int		SYSALIASES_COLUMN_COUNT = 9;
	private static final int		SYSALIASES_ALIASID = 1;
	private static final int		SYSALIASES_ALIAS = 2;
	private static final int		SYSALIASES_SCHEMAID = 3;
	private static final int		SYSALIASES_JAVACLASSNAME = 4;
	private static final int		SYSALIASES_ALIASTYPE = 5;
	private static final int		SYSALIASES_NAMESPACE = 6;
	private static final int		SYSALIASES_SYSTEMALIAS = 7;
	public  static final int		SYSALIASES_ALIASINFO = 8;
	private static final int		SYSALIASES_SPECIFIC_NAME = 9;

 
	protected static final int		SYSALIASES_INDEX1_ID = 0;

	protected static final int		SYSALIASES_INDEX2_ID = 1;

	protected static final int		SYSALIASES_INDEX3_ID = 2;

	// null means all unique.
    private	static	final	boolean[]	uniqueness = null;

	private static int[][] indexColumnPositions =
	{
		{SYSALIASES_SCHEMAID, SYSALIASES_ALIAS, SYSALIASES_NAMESPACE},
		{SYSALIASES_ALIASID},
		{SYSALIASES_SCHEMAID, SYSALIASES_SPECIFIC_NAME},
	};

	private static String[][] indexColumnNames =
	{
		{"SCHEMAID", "ALIAS", "NAMESPACE"},
		{"ALIASID"},
		{"SCHEMAID", "SPECIFICNAME"},
	};

	private	static	final	String[]	uuids =
	{
		 "c013800d-00d7-ddbd-08ce-000a0a411400"	// catalog UUID
		,"c013800d-00d7-ddbd-75d4-000a0a411400"	// heap UUID
		,"c013800d-00d7-ddbe-b99d-000a0a411400"	// SYSALIASES_INDEX1
		,"c013800d-00d7-ddbe-c4e1-000a0a411400"	// SYSALIASES_INDEX2
		,"c013800d-00d7-ddbe-34ae-000a0a411400"	// SYSALIASES_INDEX3
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSALIASESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSALIASES_COLUMN_COUNT, "SYSALIASES", indexColumnPositions, indexColumnNames, uniqueness, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSALIASES row
	 *
	 * @param emptyRow		Make an empty row if this parameter is true
	 * @param ad			Alias descriptor
	 * @param dvf			A DataValueFactory
	 *
	 * @return	Row suitable for inserting into SYSALIASES.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		String					schemaID = null;
		String					javaClassName = null;
		String					sAliasType = null;
		String					aliasID = null;
		String					aliasName = null;
		String					specificName = null;
		char					cAliasType = AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR;
		char					cNameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
		boolean					systemAlias = false;
		AliasInfo				aliasInfo = null;

		if (td != null) {

			AliasDescriptor 		ad = (AliasDescriptor)td;
			aliasID	= ad.getUUID().toString();
			aliasName = ad.getDescriptorName();
			schemaID	= ad.getSchemaUUID().toString();
			javaClassName	= ad.getJavaClassName();
			cAliasType = ad.getAliasType();
			cNameSpace = ad.getNameSpace();
			systemAlias = ad.getSystemAlias();
			aliasInfo = ad.getAliasInfo();
			specificName = ad.getSpecificName();

			char[] charArray = new char[1];
			charArray[0] = cAliasType;
			sAliasType = new String(charArray);

			if (SanityManager.DEBUG)
			{
				switch (cAliasType)
				{
					case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
					case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
						break;

					default:
						SanityManager.THROWASSERT(
							"Unexpected value (" + cAliasType +
							") for aliasType");
				}
			}
		}


		/* Insert info into sysaliases */

		/* RESOLVE - It would be nice to require less knowledge about sysaliases
		 * and have this be more table driven.
		 */

		/* Build the row to insert */
		ExecRow row = getExecutionFactory().getValueRow(SYSALIASES_COLUMN_COUNT);

		/* 1st column is ALIASID (UUID - char(36)) */
		row.setColumn(SYSALIASES_ALIASID, dvf.getCharDataValue(aliasID));

		/* 2nd column is ALIAS (varchar(128))) */
		row.setColumn(SYSALIASES_ALIAS, dvf.getVarcharDataValue(aliasName));
		//		System.out.println(" added row-- " + aliasName);

		/* 3rd column is SCHEMAID (UUID - char(36)) */
		row.setColumn(SYSALIASES_SCHEMAID, dvf.getCharDataValue(schemaID));

		/* 4th column is JAVACLASSNAME (longvarchar) */
		row.setColumn(SYSALIASES_JAVACLASSNAME, dvf.getLongvarcharDataValue(javaClassName));

		/* 5th column is ALIASTYPE (char(1)) */
		row.setColumn(SYSALIASES_ALIASTYPE, dvf.getCharDataValue(sAliasType));

		/* 6th column is NAMESPACE (char(1)) */
		String sNameSpace = new String(new char[] { cNameSpace });

		row.setColumn
			(SYSALIASES_NAMESPACE, dvf.getCharDataValue(sNameSpace));


		/* 7th column is SYSTEMALIAS (boolean) */
		row.setColumn
			(SYSALIASES_SYSTEMALIAS, dvf.getDataValue(systemAlias));

		/* 8th column is ALIASINFO (org.apache.derby.catalog.AliasInfo) */
		row.setColumn(SYSALIASES_ALIASINFO, 
			dvf.getDataValue(aliasInfo));

		/* 9th column is specific name */
		row.setColumn
			(SYSALIASES_SPECIFIC_NAME, dvf.getVarcharDataValue(specificName));


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
		    case SYSALIASES_INDEX1_ID:
				/* 1st column is SCHEMAID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

				/* 2nd column is ALIAS (varchar(128)) */
				row.setColumn(2, getDataValueFactory().getVarcharDataValue((String) null));

				/* 3rd column is NAMESPACE (char(1)) */
				row.setColumn(3, getDataValueFactory().getCharDataValue((String) null));

				break;

		    case SYSALIASES_INDEX2_ID:
				/* 1st column is ALIASID (UUID - char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

				break;

		    case SYSALIASES_INDEX3_ID:
				/* 1st column is SCHEMAID (char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

				/* 2nd column is SPECIFICNAME (varchar(128)) */
				row.setColumn(2, getDataValueFactory().getVarcharDataValue((String) null));

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
	 * Make a AliasDescriptor out of a SYSALIASES row
	 *
	 * @param row a SYSALIASES row
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
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSALIASES_COLUMN_COUNT, 
				"Wrong number of columns for a SYSALIASES row");
		}

		char				cAliasType;
		char				cNameSpace;
		DataValueDescriptor	col;
		String				aliasID;
		UUID				aliasUUID;
		String				aliasName;
		String				javaClassName;
		String				sAliasType;
		String				sNameSpace;
		String				typeStr;
		boolean				systemAlias = false;
		AliasInfo			aliasInfo = null;

		/* 1st column is ALIASID (UUID - char(36)) */
		col = row.getColumn(SYSALIASES_ALIASID);
		aliasID = col.getString();
		aliasUUID = getUUIDFactory().recreateUUID(aliasID);

		/* 2nd column is ALIAS (varchar(128)) */
		col = row.getColumn(SYSALIASES_ALIAS);
		aliasName = col.getString();

		/* 3rd column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSALIASES_SCHEMAID);
		UUID schemaUUID = col.isNull() ? null : getUUIDFactory().recreateUUID(col.getString());

		/* 4th column is JAVACLASSNAME (longvarchar) */
		col = row.getColumn(SYSALIASES_JAVACLASSNAME);
		javaClassName = col.getString();

		/* 5th column is ALIASTYPE (char(1)) */
		col = row.getColumn(SYSALIASES_ALIASTYPE);
		sAliasType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sAliasType.length() == 1, 
				"Fifth column (aliastype) type incorrect");
			switch (sAliasType.charAt(0))
			{
				case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR: 
					break;

				default: 
					SanityManager.THROWASSERT("Invalid type value '"
							+sAliasType+ "' for  alias");
			}
		}

		cAliasType = sAliasType.charAt(0);

		/* 6th column is NAMESPACE (char(1)) */
		col = row.getColumn(SYSALIASES_NAMESPACE);
		sNameSpace = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sNameSpace.length() == 1, 
				"Sixth column (namespace) type incorrect");
			switch (sNameSpace.charAt(0))
			{
				case AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR: 
				case AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR: 
					break;

				default: 
					SanityManager.THROWASSERT("Invalid type value '"
							+sNameSpace+ "' for  alias");
			}
		}

		cNameSpace = sNameSpace.charAt(0);


		/* 7th column is SYSTEMALIAS (boolean) */
		col = row.getColumn(SYSALIASES_SYSTEMALIAS);
		systemAlias = col.getBoolean();

		/* 8th column is ALIASINFO (org.apache.derby.catalog.AliasInfo) */
		col = row.getColumn(SYSALIASES_ALIASINFO);
		aliasInfo = (AliasInfo) col.getObject();

		/* 9th column is specific name */
		col = row.getColumn(SYSALIASES_SPECIFIC_NAME);
		String specificName = col.getString();


		/* now build and return the descriptor */
		return new AliasDescriptor(dd, aliasUUID, aliasName,
										schemaUUID, javaClassName, cAliasType,
										cNameSpace, systemAlias,
										aliasInfo, specificName);
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
	{
		SystemColumn[]			columnList = new SystemColumn[SYSALIASES_COLUMN_COUNT];

		// describe columns

		columnList[0] =
					new SystemColumnImpl(
							convertIdCase( "ALIASID"),			// column name
							1,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			               );

		columnList[1] =
					new SystemColumnImpl(
							convertIdCase( "ALIAS"),			// column name
							2,	// column number
							false				// nullability
			               );

		columnList[2] = new SystemColumnImpl(	
								convertIdCase( "SCHEMAID"),			// column name
								3,	// column number
								0,					// precision
								0,					// scale
								true,				// nullability
								"CHAR",				// dataType
								true,				// built-in type
								36					// maxLength
			                   );

		columnList[3] =
					new SystemColumnImpl(
							convertIdCase( "JAVACLASSNAME"),		// column name
							4,
							0,					// precision
							0,					// scale
							false,				// nullability
							"LONG VARCHAR",			// dataType
							true,				// built-in type
							Integer.MAX_VALUE	// maxLength
							);

		columnList[4] =
					new SystemColumnImpl(
							convertIdCase( "ALIASTYPE"),		// column name
							5,
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",			// dataType
							true,				// built-in type
							1					// maxLength
							);

		columnList[5] =
					new SystemColumnImpl(
							convertIdCase( "NAMESPACE"),		// column name
							6,
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",			// dataType
							true,				// built-in type
							1					// maxLength
							);

		columnList[6] =
					new SystemColumnImpl(
							convertIdCase( "SYSTEMALIAS"),		// column name
							7,
							0,					// precision
							0,					// scale
							false,				// nullability
							"BOOLEAN",			// dataType
							true,				// built-in type
							0					// maxLength
							);

		columnList[7] = 
					new SystemColumnImpl(	
							convertIdCase( "ALIASINFO"),			// column name
							8,	// column number
							0,					// precision
							0,					// scale
							true,				// nullability
							"org.apache.derby.catalog.AliasInfo",	    // dataType
							false,				// built-in type
							TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN // maxLength
			               );

		columnList[8] =
					new SystemColumnImpl(
							convertIdCase( "SPECIFICNAME"),
							9,	// column number
							false				// nullability
			               );

		return	columnList;
	}
}
