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
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.IndexDescriptor;

/**
 * Factory for creating a SYSFOREIGNKEYS row.
 *
 * @author jerry
 */

public class SYSFOREIGNKEYSRowFactory extends CatalogRowFactory
{
	private  static final String	TABLENAME_STRING = "SYSFOREIGNKEYS";

	protected static final int		SYSFOREIGNKEYS_COLUMN_COUNT = 5;
	protected static final int		SYSFOREIGNKEYS_CONSTRAINTID = 1;
	protected static final int		SYSFOREIGNKEYS_CONGLOMERATEID = 2;
	protected static final int		SYSFOREIGNKEYS_KEYCONSTRAINTID = 3;
	protected static final int		SYSFOREIGNKEYS_DELETERULE = 4;
	protected static final int		SYSFOREIGNKEYS_UPDATERULE = 5;

	// Column widths
	protected static final int		SYSFOREIGNKEYS_CONSTRAINTID_WIDTH = 36;

	protected static final int		SYSFOREIGNKEYS_INDEX1_ID = 0;
	protected static final int		SYSFOREIGNKEYS_INDEX2_ID = 1;

	private static final int[][] indexColumnPositions = 
	{
		{SYSFOREIGNKEYS_CONSTRAINTID},
		{SYSFOREIGNKEYS_KEYCONSTRAINTID}
	};

	private static final String[][] indexColumnNames =
	{
		{"CONSTRAINTID"},
		{"KEYCONSTRAINTID"}
	};

    private	static	final	boolean[]	uniqueness = {
		                                               true,
													   false
	                                                 };

	private	static	final	String[]	uuids =
	{
		 "8000005b-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000060-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000005d-00d0-fd77-3ed8-000a0a0b1900"	// SYSFOREIGNKEYS_INDEX1
		,"8000005f-00d0-fd77-3ed8-000a0a0b1900"	// SYSFOREIGNKEYS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public	SYSFOREIGNKEYSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSFOREIGNKEYS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, indexColumnNames, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSFOREIGNKEYS row
	 *
	 * @param emptyRow		Make an empty row if this parameter is true
	 * @param cd			Constraint descriptor
	 *
	 * @return	Row suitable for inserting into SYSFOREIGNKEYS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecIndexRow			row;
		String					constraintId = null;
		String					keyConstraintId = null;
		String					conglomId = null;
		String	                raDeleteRule="N";
		String					raUpdateRule="N";

		if (td != null)
		{
			ForeignKeyConstraintDescriptor cd = (ForeignKeyConstraintDescriptor)td;
			constraintId = cd.getUUID().toString();
			
			ReferencedKeyConstraintDescriptor refCd = cd.getReferencedConstraint();
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(refCd != null, "this fk returned a null referenced key");
			}
			keyConstraintId = refCd.getUUID().toString();
			conglomId = cd.getIndexUUIDString();

			raDeleteRule = getRefActionAsString(cd.getRaDeleteRule());
			raUpdateRule = getRefActionAsString(cd.getRaUpdateRule());
		}
			
			
		/* Build the row  */
		row = getExecutionFactory().getIndexableRow(SYSFOREIGNKEYS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSFOREIGNKEYS_CONSTRAINTID, dvf.getCharDataValue(constraintId));

		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		row.setColumn(SYSFOREIGNKEYS_CONGLOMERATEID, dvf.getCharDataValue(conglomId));

		/* 3rd column is KEYCONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSFOREIGNKEYS_KEYCONSTRAINTID, dvf.getCharDataValue(keyConstraintId));

		// currently, DELETERULE and UPDATERULE are always "R" for restrict
		/* 4th column is DELETERULE char(1) */
		row.setColumn(SYSFOREIGNKEYS_DELETERULE, dvf.getCharDataValue(raDeleteRule));

		/* 5th column is UPDATERULE char(1) */
		row.setColumn(SYSFOREIGNKEYS_UPDATERULE, dvf.getCharDataValue(raUpdateRule));

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

		row.setColumn(ncols +1, rowLocation);

		/* 1st column is CONSTRAINTID (char(36)) or KEYCONSTRAINTID (char(36)) */
		row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));

		return	row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ViewDescriptor out of a SYSFOREIGNKEYS row
	 *
	 * @param row a SYSFOREIGNKEYS row
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
				row.nColumns() == SYSFOREIGNKEYS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSKEYS row");
		}

		DataValueDescriptor		col;
		DataDescriptorGenerator ddg;
		UUID					constraintUUID;
		UUID					conglomerateUUID;
		UUID					keyConstraintUUID;
		String					constraintUUIDString;
		String					conglomerateUUIDString;
		String                  raRuleString;
		int                     raDeleteRule;
		int                     raUpdateRule;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSFOREIGNKEYS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(SYSFOREIGNKEYS_CONGLOMERATEID);
		conglomerateUUIDString = col.getString();
		conglomerateUUID = getUUIDFactory().recreateUUID(conglomerateUUIDString);

		/* 3rd column is KEYCONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSFOREIGNKEYS_KEYCONSTRAINTID);
		constraintUUIDString = col.getString();
		keyConstraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);


		/* 4th column is DELETERULE char(1) */
		col= row.getColumn(SYSFOREIGNKEYS_DELETERULE);
		raRuleString = col.getString();
		raDeleteRule  = getRefActionAsInt(raRuleString);
		
		/* 5th column is UPDATERULE char(1) */
		col = row.getColumn(SYSFOREIGNKEYS_UPDATERULE);
		raRuleString = col.getString();
		raUpdateRule  = getRefActionAsInt(raRuleString);


		/* now build and return the descriptor */
		return new SubKeyConstraintDescriptor(
										constraintUUID,
										conglomerateUUID,
										keyConstraintUUID,
										raDeleteRule,
										raUpdateRule);
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
		SystemColumn[]			columnList = new SystemColumn[SYSFOREIGNKEYS_COLUMN_COUNT];

		// describe columns

		columnList[index++] = new SystemColumnImpl(	
							convertIdCase( "CONSTRAINTID"),			// name 
							SYSFOREIGNKEYS_CONSTRAINTID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );
		columnList[index++] = new SystemColumnImpl(	
							convertIdCase( "CONGLOMERATEID"),			// name 
							SYSFOREIGNKEYS_CONGLOMERATEID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );
		columnList[index++] = new SystemColumnImpl(	
							convertIdCase( "KEYCONSTRAINTID"),			// name 
							SYSFOREIGNKEYS_KEYCONSTRAINTID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );

		columnList[index++] = new SystemColumnImpl(	
							convertIdCase( "DELETERULE"),			// name 
							SYSFOREIGNKEYS_DELETERULE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			                );

		columnList[index++] = new SystemColumnImpl(	
							convertIdCase( "UPDATERULE"),			// name 
							SYSFOREIGNKEYS_UPDATERULE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			                );
		return	columnList;
	}


	int getRefActionAsInt(String raRuleString)
	{
		int raRule ;
		switch (raRuleString.charAt(0)){
		case 'C': 
			raRule = StatementType.RA_CASCADE;
			break;
		case 'S':
			raRule = StatementType.RA_RESTRICT;
			break;
		case 'R':
			raRule = StatementType.RA_NOACTION;
			break;
		case 'U':
			raRule = StatementType.RA_SETNULL;
			break;
		case 'D':
			raRule = StatementType.RA_SETDEFAULT;
			break;
		default: 
			raRule =StatementType.RA_NOACTION; ;
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Invalid  value '"
										  +raRuleString+ "' for a referetial Action");
			}
		}
		return raRule ;
	}


	String getRefActionAsString(int raRule)
	{
		String raRuleString ;
		switch (raRule){
		case StatementType.RA_CASCADE:
			raRuleString = "C";
			break;
		case StatementType.RA_RESTRICT:
			raRuleString = "S";
				break;
		case StatementType.RA_NOACTION:
			raRuleString = "R";
			break;
		case StatementType.RA_SETNULL:
			raRuleString = "U";
			break;
		case StatementType.RA_SETDEFAULT:
			raRuleString = "D";
			raRule = StatementType.RA_SETDEFAULT;
			break;
		default: 
			raRuleString ="N" ; // NO ACTION (default value)
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Invalid  value '"
							+raRule+ "' for a referetial Action");
			}

		}
		return raRuleString ;
	}


}
