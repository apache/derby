/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSTRIGGERSRowFactory

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.catalog.ReferencedColumns;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.SQLTimestamp;
import java.sql.Timestamp;

/**
 * Factory for creating a SYSTRIGGERS row.
 *
 *
 * @version 0.1
 * @author Jamie
 */

public class SYSTRIGGERSRowFactory extends CatalogRowFactory
{
	static	final	String	TABLENAME_STRING = "SYSTRIGGERS";

	/* Column #s for sysinfo (1 based) */
	public	static	final	int		SYSTRIGGERS_TRIGGERID = 1;
	public	static	final	int		SYSTRIGGERS_TRIGGERNAME = 2;
	public	static	final	int		SYSTRIGGERS_SCHEMAID = 3;
	public	static	final	int		SYSTRIGGERS_CREATIONTIMESTAMP = 4;
	public	static	final	int		SYSTRIGGERS_EVENT = 5;
	public	static	final	int		SYSTRIGGERS_FIRINGTIME = 6;
	public	static	final	int		SYSTRIGGERS_TYPE = 7;
	public	static	final	int		SYSTRIGGERS_STATE = TriggerDescriptor.SYSTRIGGERS_STATE_FIELD;
	public	static	final	int		SYSTRIGGERS_TABLEID = 9;
	public	static	final	int		SYSTRIGGERS_WHENSTMTID = 10;
	public	static	final	int		SYSTRIGGERS_ACTIONSTMTID = 11;
	public	static	final	int		SYSTRIGGERS_REFERENCEDCOLUMNS = 12;
	public	static	final	int		SYSTRIGGERS_TRIGGERDEFINITION = 13;
	public	static	final	int		SYSTRIGGERS_REFERENCINGOLD = 14;
	public	static	final	int		SYSTRIGGERS_REFERENCINGNEW = 15;
	public	static	final	int		SYSTRIGGERS_OLDREFERENCINGNAME = 16;
	public	static	final	int		SYSTRIGGERS_NEWREFERENCINGNAME = 17;

	public	static	final	int		SYSTRIGGERS_COLUMN_COUNT = SYSTRIGGERS_NEWREFERENCINGNAME;

	public  static final int		SYSTRIGGERS_INDEX1_ID = 0;
	public  static final int		SYSTRIGGERS_INDEX2_ID = 1;
	public  static final int		SYSTRIGGERS_INDEX3_ID = 2;

	private static final int[][] indexColumnPositions =
	{
		{SYSTRIGGERS_TRIGGERID},
		{SYSTRIGGERS_TRIGGERNAME, SYSTRIGGERS_SCHEMAID},
		{SYSTRIGGERS_TABLEID, SYSTRIGGERS_CREATIONTIMESTAMP}
	};

	private static final String[][] indexColumnNames =
	{
		{"TRIGGERID"},
		{"TRIGGERNAME", "SCHEMAID"},
		{"CREATIONTIMESTAMP"}
	};

	private	static	final	boolean[]	uniqueness = {
													   true,
													   true,
													   false,
	                                                 };

	private	static	final	String[]	uuids =
	{
		 "c013800d-00d7-c025-4809-000a0a411200"	// catalog UUID
		,"c013800d-00d7-c025-480a-000a0a411200"	// heap UUID
		,"c013800d-00d7-c025-480b-000a0a411200"	// SYSTRIGGERS_INDEX1 
		,"c013800d-00d7-c025-480c-000a0a411200"	// SYSTRIGGERS_INDEX2
		,"c013800d-00d7-c025-480d-000a0a411200"	// SYSTRIGGERS_INDEX3
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////
	public	SYSTRIGGERSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                  boolean convertIdToLower)
	{
		super(uuidf,ef,dvf,convertIdToLower);
		initInfo(SYSTRIGGERS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, indexColumnNames,  uniqueness, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////
	/**
	 * Make a SYSTRIGGERS row. 
	 * 
	 * @param emptyRow			Make an empty row if this parameter is true
	 * @param triggerDescriptor	In-memory tuple to be converted to a disk row.
	 *
	 * @return	Row suitable for inserting into SYSTRIGGERS.
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
		UUID					uuid = null;	
		UUID					suuid = null;			// schema	
		UUID					tuuid = null;			// referenced table	
		UUID					actionSPSID = null;		// action sps uuid string
		UUID					whenSPSID = null;		// when clause sps uuid string
		Timestamp				createTime = null;
		String					event = null;
		String					time = null;
		String					type = null;
		String					enabled = null;
		String					triggerDefinition = null;
		String					oldReferencingName = null;
		String					newReferencingName = null;
		ReferencedColumns rcd = null;
		boolean					referencingOld = false;
		boolean					referencingNew = false;

		if (td != null)
		{
			TriggerDescriptor triggerDescriptor = (TriggerDescriptor)td;
			name = triggerDescriptor.getName();
			uuid = triggerDescriptor.getUUID();
			suuid = triggerDescriptor.getSchemaDescriptor().getUUID();
			createTime = triggerDescriptor.getCreationTimestamp();
			// for now we are assuming that a trigger can only listen to a single event
			event = triggerDescriptor.listensForEvent(TriggerDescriptor.TRIGGER_EVENT_UPDATE) ? "U" :
					triggerDescriptor.listensForEvent(TriggerDescriptor.TRIGGER_EVENT_DELETE) ? "D" : "I";
			time = triggerDescriptor.isBeforeTrigger() ? "B" : "A";
			type = triggerDescriptor.isRowTrigger() ? "R" : "S";
			enabled = triggerDescriptor.isEnabled() ? "E" : "D";
			tuuid = triggerDescriptor.getTableDescriptor().getUUID();
			int[] refCols = triggerDescriptor.getReferencedCols();
			rcd = (refCols != null) ? new
				ReferencedColumnsDescriptorImpl(refCols) : null;

			actionSPSID =  triggerDescriptor.getActionId();
			whenSPSID =  triggerDescriptor.getWhenClauseId();
			triggerDefinition = triggerDescriptor.getTriggerDefinition();
			referencingOld = triggerDescriptor.getReferencingOld();
			referencingNew = triggerDescriptor.getReferencingNew();
			oldReferencingName = triggerDescriptor.getOldReferencingName();
			newReferencingName = triggerDescriptor.getNewReferencingName();
		}

		/* Build the row to insert */
		row = getExecutionFactory().getValueRow(SYSTRIGGERS_COLUMN_COUNT);

		/* 1st column is TRIGGERID */
		row.setColumn(1, dvf.getCharDataValue((uuid == null) ? null : uuid.toString()));

		/* 2nd column is TRIGGERNAME */
		row.setColumn(2, dvf.getVarcharDataValue(name));

		/* 3rd column is SCHEMAID */
		row.setColumn(3, dvf.getCharDataValue((suuid == null) ? null : suuid.toString()));

		/* 4th column is CREATIONTIMESTAMP */
		row.setColumn(4, dvf.getDataValue(createTime));

		/* 5th column is EVENT */
		row.setColumn(5, dvf.getCharDataValue(event));

		/* 6th column is FIRINGTIME */
		row.setColumn(6, dvf.getCharDataValue(time));

		/* 7th column is TYPE */
		row.setColumn(7, dvf.getCharDataValue(type));

		/* 8th column is STATE */
		row.setColumn(8, dvf.getCharDataValue(enabled));

		/* 9th column is TABLEID */
		row.setColumn(9, dvf.getCharDataValue((tuuid == null) ? null : tuuid.toString()));

		/* 10th column is WHENSTMTID */
		row.setColumn(10, dvf.getCharDataValue((whenSPSID == null) ? null : whenSPSID.toString()));

		/* 11th column is ACTIONSTMTID */
		row.setColumn(11, dvf.getCharDataValue((actionSPSID == null) ? null : actionSPSID.toString()));

		/* 12th column is REFERENCEDCOLUMNS 
		 *  (user type org.apache.derby.catalog.ReferencedColumns)
		 */
		row.setColumn(12, dvf.getDataValue(rcd));

		/* 13th column is TRIGGERDEFINITION */
		row.setColumn(13, dvf.getLongvarcharDataValue(triggerDefinition));

		/* 14th column is REFERENCINGOLD */
		row.setColumn(14, dvf.getDataValue(referencingOld));

		/* 15th column is REFERENCINGNEW */
		row.setColumn(15, dvf.getDataValue(referencingNew));

		/* 16th column is OLDREFERENCINGNAME */
		row.setColumn(16, dvf.getVarcharDataValue(oldReferencingName));

		/* 17th column is NEWREFERENCINGNAME */
		row.setColumn(17, dvf.getVarcharDataValue(newReferencingName));

		return row;
	}

	/**
	 * Builds an empty index row.
	 *
	 * @param	indexNumber	Index to build empty row for.
	 * @param  rowLocation	Row location for last column of index row
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
		    case SYSTRIGGERS_INDEX1_ID:
				/* 1st column is TRIGGERID (UUID - char(36)) */
				row.setColumn(1, getDataValueFactory().getCharDataValue((String) null));
				break;

		    case SYSTRIGGERS_INDEX2_ID:
				/* 1st column is TRIGGERNAME (varchar(128)) */
				row.setColumn(1, getDataValueFactory().getVarcharDataValue((String) null));

				/* 2nd column is SCHEMAID (char(32)) */
				row.setColumn(2, getDataValueFactory().getCharDataValue((String) null));
				break;

		    case SYSTRIGGERS_INDEX3_ID:
				/* 1nd column is TABLEID (char(32)) */
				row.setColumn(2, getDataValueFactory().getCharDataValue((String) null));

				/* 2nd column is COMPILATIONTIMESTAMP (timestamp) */
				row.setColumn(2, new SQLTimestamp());
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
	 * Make an  Tuple Descriptor out of a SYSTRIGGERS row
	 *
	 * @param row 					a SYSTRIGGERS row
	 * @param parentTupleDescriptor	unused
	 * @param dd 					dataDictionary
	 *
	 * @return	a  descriptor equivalent to a SYSTRIGGERS row
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor
	(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd 
	) throws StandardException
	{
		DataValueDescriptor		col;
		String					name;
		char					theChar;
		String					uuidStr;
		String					triggerDefinition;
		String					oldReferencingName;
		String					newReferencingName;
		UUID					uuid;	
		UUID					suuid;					// schema	
		UUID					tuuid;					// referenced table	
		UUID					actionSPSID = null;		// action sps uuid string
		UUID					whenSPSID = null;		// when clause sps uuid string
		Timestamp				createTime;
		int						eventMask = 0;
		boolean					isBefore;
		boolean					isRow;
		boolean					isEnabled;
		boolean					referencingOld;
		boolean					referencingNew;
		ReferencedColumns rcd;
		TriggerDescriptor		descriptor;
		DataDescriptorGenerator	ddg = dd.getDataDescriptorGenerator();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row.nColumns() == SYSTRIGGERS_COLUMN_COUNT, 
								 "Wrong number of columns for a SYSTRIGGERS row");
		}

		// 1st column is TRIGGERID (UUID - char(36))
		col = row.getColumn(1);
		uuidStr = col.getString();
		uuid = getUUIDFactory().recreateUUID(uuidStr);

		// 2nd column is TRIGGERNAME (varchar(128))
		col = row.getColumn(2);
		name = col.getString();

		// 3rd column is SCHEMAID (UUID - char(36))
		col = row.getColumn(3);
		uuidStr = col.getString();
		suuid = getUUIDFactory().recreateUUID(uuidStr);

		// 4th column is CREATIONTIMESTAMP (TIMESTAMP)
		col = row.getColumn(4);
		createTime = (Timestamp) col.getObject();

		// 5th column is EVENT (char(1))
		col = row.getColumn(5);
		theChar = col.getString().charAt(0);
		switch (theChar)
		{
			case 'U': 
						eventMask = TriggerDescriptor.TRIGGER_EVENT_UPDATE;
						break;

			case 'I': 
						eventMask = TriggerDescriptor.TRIGGER_EVENT_INSERT;
						break;

			case 'D': 
						eventMask = TriggerDescriptor.TRIGGER_EVENT_DELETE;
						break;

			default:
					if (SanityManager.DEBUG)	
					{
						SanityManager.THROWASSERT("bad event mask: "+theChar);
					}
		}
		
		// 6th column is FIRINGTIME (char(1))
		isBefore = getCharBoolean(row.getColumn(6), 'B', 'A');

		// 7th column is TYPE (char(1))
		isRow = getCharBoolean(row.getColumn(7), 'R', 'S');

		// 8th column is STATE (char(1))
		isEnabled = getCharBoolean(row.getColumn(8), 'E', 'D');

		// 9th column is TABLEID (UUID - char(36))
		col = row.getColumn(9);
		uuidStr = col.getString();
		tuuid = getUUIDFactory().recreateUUID(uuidStr);

		// 10th column is WHENSTMTID (UUID - char(36))
		col = row.getColumn(10);
		uuidStr = col.getString();
		if (uuidStr != null)
			whenSPSID = getUUIDFactory().recreateUUID(uuidStr);

		// 11th column is ACTIONSTMTID (UUID - char(36))
		col = row.getColumn(11);
		uuidStr = col.getString();
		if (uuidStr != null)
			actionSPSID = getUUIDFactory().recreateUUID(uuidStr);

		// 12th column is REFERENCEDCOLUMNS user type org.apache.derby.catalog.ReferencedColumns
		col = row.getColumn(12);
		rcd = (ReferencedColumns) col.getObject();

		// 13th column is TRIGGERDEFINITION (longvarhar)
		col = row.getColumn(13);
		triggerDefinition = col.getString();

		// 14th column is REFERENCINGOLD (boolean)
		col = row.getColumn(14);
		referencingOld = col.getBoolean();

		// 15th column is REFERENCINGNEW (boolean)
		col = row.getColumn(15);
		referencingNew = col.getBoolean();

		// 16th column is REFERENCINGNAME (varchar(128))
		col = row.getColumn(16);
		oldReferencingName = col.getString();

		// 17th column is REFERENCINGNAME (varchar(128))
		col = row.getColumn(17);
		newReferencingName = col.getString();

		descriptor = new TriggerDescriptor(
									dd,
									dd.getSchemaDescriptor(suuid, null),
									uuid, 
									name, 
									eventMask,
									isBefore, 
									isRow,
									isEnabled,
									dd.getTableDescriptor(tuuid),
									whenSPSID,
									actionSPSID,
									createTime,
									(rcd == null) ? (int[])null : rcd.getReferencedColumnPositions(),
									triggerDefinition,
									referencingOld,
									referencingNew,
									oldReferencingName,
									newReferencingName
									);

		return descriptor;
	}

			
	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 * The last column, the serialized statement, is not added
	 * to the column list.  This is done deliberately to make it
	 * a 'hidden' column -- one that is not visible to customers,
	 * but is visible to the system.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[] buildColumnList()
	{
		SystemColumn[]	columnList = new SystemColumn[SYSTRIGGERS_COLUMN_COUNT];

		// describe columns
		columnList[SYSTRIGGERS_TRIGGERID-1] = new SystemColumnImpl(	
							convertIdCase( "TRIGGERID"),			// name 
							SYSTRIGGERS_TRIGGERID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );

		columnList[SYSTRIGGERS_TRIGGERNAME-1] = 
					new SystemColumnImpl(		// SQL IDENTIFIER
							convertIdCase( "TRIGGERNAME"),			// column name
							SYSTRIGGERS_TRIGGERNAME,	// column number
							false				// nullability
							);

		columnList[SYSTRIGGERS_SCHEMAID-1] = new SystemColumnImpl(	
							convertIdCase( "SCHEMAID"),				// name 
							SYSTRIGGERS_SCHEMAID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );

		columnList[SYSTRIGGERS_CREATIONTIMESTAMP-1] = 
					new SystemColumnImpl(		
							convertIdCase( "CREATIONTIMESTAMP"),		// name 
							SYSTRIGGERS_CREATIONTIMESTAMP,	// column number
							0,							// precision
							0,							// scale
							false,						// nullability
							"TIMESTAMP",				// dataType
							true,						// built-in type
							TypeId.TIMESTAMP_MAXWIDTH	// maxLength
			                );

		columnList[SYSTRIGGERS_EVENT-1] = 
					new SystemColumnImpl(		
							convertIdCase( "EVENT"),			// name 
							SYSTRIGGERS_EVENT,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			                );

		columnList[SYSTRIGGERS_FIRINGTIME-1] = 
					new SystemColumnImpl(		
							convertIdCase( "FIRINGTIME"),		// name 
							SYSTRIGGERS_FIRINGTIME, // column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			                );

		columnList[SYSTRIGGERS_TYPE-1] = 
					new SystemColumnImpl(		
							convertIdCase( "TYPE"),				// name 
							SYSTRIGGERS_TYPE,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			                );

		columnList[SYSTRIGGERS_STATE-1] = 
					new SystemColumnImpl(		
							convertIdCase( "STATE"),			// name 
							SYSTRIGGERS_STATE,// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							1					// maxLength
			                );

		columnList[SYSTRIGGERS_TABLEID-1] = 
					new SystemColumnImpl(	
							convertIdCase( "TABLEID"),			// name 
							SYSTRIGGERS_TABLEID,	// column number
							0,					// precision
							0,					// scale
							false,				// nullability
							"CHAR",				// dataType
							true,				// built-in type
							36					// maxLength
			                );

		columnList[SYSTRIGGERS_WHENSTMTID-1] = 
					new SystemColumnImpl(	
							convertIdCase( "WHENSTMTID"),				// name 
							SYSTRIGGERS_WHENSTMTID,		// column number
							0,							// precision
							0,							// scale
							true,						// nullability
							"CHAR",						// dataType
							true,						// built-in type
							36							// maxLength
			                );

		columnList[SYSTRIGGERS_ACTIONSTMTID-1] = 
					new SystemColumnImpl(	
							convertIdCase( "ACTIONSTMTID"),				// name 
							SYSTRIGGERS_ACTIONSTMTID,	// column number
							0,							// precision
							0,							// scale
							true,						// nullability
							"CHAR",						// dataType
							true,						// built-in type
							36							// maxLength
			                );

		columnList[SYSTRIGGERS_REFERENCEDCOLUMNS-1] = 
					new SystemColumnImpl(		
							convertIdCase( "REFERENCEDCOLUMNS"),			// name 
							SYSTRIGGERS_REFERENCEDCOLUMNS,	// column number
							0,								// precision
							0,								// scale
							true,							// nullability
							"org.apache.derby.catalog.ReferencedColumns",	//datatype
							false,							// built-in type
							DataTypeDescriptor.MAXIMUM_WIDTH_UNKNOWN // maxLength
			                );

		columnList[SYSTRIGGERS_TRIGGERDEFINITION-1] = 
					new SystemColumnImpl(	
							convertIdCase( "TRIGGERDEFINITION"),				// name 
							SYSTRIGGERS_TRIGGERDEFINITION,	// column number
							0,							// precision
							0,							// scale
							true,						// nullability
							"LONG VARCHAR",			// dataType
							true,				// built-in type
							Integer.MAX_VALUE	// maxLength
			                );

		columnList[SYSTRIGGERS_REFERENCINGOLD-1] = 
					new SystemColumnImpl(		
							convertIdCase( "REFERENCINGOLD"),			// name 
							SYSTRIGGERS_REFERENCINGOLD,// column number
							0,					// precision
							0,					// scale
							true,				// nullability
							"BOOLEAN",			// dataType
							true,				// built-in type
							1					// maxLength
			                );

		columnList[SYSTRIGGERS_REFERENCINGNEW-1] = 
					new SystemColumnImpl(		
							convertIdCase( "REFERENCINGNEW"),			// name 
							SYSTRIGGERS_REFERENCINGNEW,// column number
							0,					// precision
							0,					// scale
							true,				// nullability
							"BOOLEAN",			// dataType
							true,				// built-in type
							1					// maxLength
			                );

		columnList[SYSTRIGGERS_OLDREFERENCINGNAME-1] = 
					new SystemColumnImpl(		// SQL IDENTIFIER
							convertIdCase( "OLDREFERENCINGNAME"),			// column name
							SYSTRIGGERS_OLDREFERENCINGNAME,	// column number
							true				// nullability
							);

		columnList[SYSTRIGGERS_NEWREFERENCINGNAME-1] = 
					new SystemColumnImpl(		// SQL IDENTIFIER
							convertIdCase( "NEWREFERENCINGNAME"),			// column name
							SYSTRIGGERS_NEWREFERENCINGNAME,	// column number
							true				// nullability
							);

		return	columnList;
	}

	public int heapColumnCount()
	{
		return SYSTRIGGERS_COLUMN_COUNT;
	}

	// a little helper
	private boolean getCharBoolean(DataValueDescriptor col, char trueValue, char falseValue) throws StandardException
	{
		char theChar = col.getString().charAt(0);
		if (theChar == trueValue)
		{
			return true;
		}
		else if (theChar == falseValue)
		{
			return false;
		}
		else
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("bad char value "+theChar);
		
			return true;
		}
	}
}
