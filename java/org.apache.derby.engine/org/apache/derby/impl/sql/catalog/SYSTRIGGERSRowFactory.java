/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSTRIGGERSRowFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.UserType;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.catalog.ReferencedColumns;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.SQLTimestamp;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Factory for creating a SYSTRIGGERS row.
 *
 *
 * @version 0.1
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
    public  static  final   int     SYSTRIGGERS_WHENCLAUSETEXT = 18;

    public  static  final   int     SYSTRIGGERS_COLUMN_COUNT = SYSTRIGGERS_WHENCLAUSETEXT;

	public  static final int		SYSTRIGGERS_INDEX1_ID = 0;
	public  static final int		SYSTRIGGERS_INDEX2_ID = 1;
	public  static final int		SYSTRIGGERS_INDEX3_ID = 2;

	private static final int[][] indexColumnPositions =
	{
		{SYSTRIGGERS_TRIGGERID},
		{SYSTRIGGERS_TRIGGERNAME, SYSTRIGGERS_SCHEMAID},
		{SYSTRIGGERS_TABLEID, SYSTRIGGERS_CREATIONTIMESTAMP}
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

    /** TimeZone object representing Coordinated Universal Time. */
    private final static TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final DataDictionary dataDictionary;

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////
    SYSTRIGGERSRowFactory(
            DataDictionary dd,
            UUIDFactory uuidf,
            ExecutionFactory ef,
            DataValueFactory dvf)
        throws StandardException
	{
		super(uuidf,ef,dvf);
        this.dataDictionary = dd;
		initInfo(SYSTRIGGERS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions,  uniqueness, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////
	/**
	 * Make a SYSTRIGGERS row. 
	 * 
	 * @return	Row suitable for inserting into SYSTRIGGERS.
	 *
	 * @exception   StandardException thrown on failure
	 */
    @Override
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
		throws StandardException
	{
        return makeRow(td, getHeapColumnCount());
    }

    @Override
    public ExecRow makeEmptyRowForCurrentVersion() throws StandardException {
        return makeRow(null, SYSTRIGGERS_COLUMN_COUNT);
    }

    /**
     * Helper method that contains common logic for {@code makeRow()} and
     * {@code makeEmptyRowForCurrentVersion()}. Creates a row for the
     * SYSTRIGGERS conglomerate.
     *
     * @param td the {@code TriggerDescriptor} to create a row from (can be
     *   {@code null} if the returned row should be empty)
     * @param columnCount the number of columns in the returned row (used for
     *   trimming off columns in soft upgrade mode to match the format in
     *   the old dictionary version)
     * @return a row for the SYSTRIGGERS conglomerate
     * @throws StandardException if an error happens when creating the row
     */
    private ExecRow makeRow(TupleDescriptor td, int columnCount)
            throws StandardException {
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
        String                  whenClauseText = null;

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
			int[] refColsInTriggerAction = triggerDescriptor.getReferencedColsInTriggerAction();
			rcd = (refCols != null || refColsInTriggerAction != null) ? new
				ReferencedColumnsDescriptorImpl(refCols, refColsInTriggerAction) : null;

			actionSPSID =  triggerDescriptor.getActionId();
			whenSPSID =  triggerDescriptor.getWhenClauseId();
			triggerDefinition = triggerDescriptor.getTriggerDefinition();
			referencingOld = triggerDescriptor.getReferencingOld();
			referencingNew = triggerDescriptor.getReferencingNew();
			oldReferencingName = triggerDescriptor.getOldReferencingName();
			newReferencingName = triggerDescriptor.getNewReferencingName();
            whenClauseText = triggerDescriptor.getWhenClauseText();
		}

		/* Build the row to insert */
        ExecRow row = getExecutionFactory().getValueRow(columnCount);

		/* 1st column is TRIGGERID */
		row.setColumn(1, new SQLChar((uuid == null) ? null : uuid.toString()));

		/* 2nd column is TRIGGERNAME */
		row.setColumn(2, new SQLVarchar(name));

		/* 3rd column is SCHEMAID */
		row.setColumn(3, new SQLChar((suuid == null) ? null : suuid.toString()));

		/* 4th column is CREATIONTIMESTAMP */
        SQLTimestamp creationTimestamp = (createTime == null)
            ? new SQLTimestamp(null)
            : new SQLTimestamp(createTime, getCalendarForCreationTimestamp());
        row.setColumn(4, creationTimestamp);

		/* 5th column is EVENT */
		row.setColumn(5, new SQLChar(event));

		/* 6th column is FIRINGTIME */
		row.setColumn(6, new SQLChar(time));

		/* 7th column is TYPE */
		row.setColumn(7, new SQLChar(type));

		/* 8th column is STATE */
		row.setColumn(8, new SQLChar(enabled));

		/* 9th column is TABLEID */
		row.setColumn(9, new SQLChar((tuuid == null) ? null : tuuid.toString()));

		/* 10th column is WHENSTMTID */
		row.setColumn(10, new SQLChar((whenSPSID == null) ? null : whenSPSID.toString()));

		/* 11th column is ACTIONSTMTID */
		row.setColumn(11, new SQLChar((actionSPSID == null) ? null : actionSPSID.toString()));

		/* 12th column is REFERENCEDCOLUMNS 
		 *  (user type org.apache.derby.catalog.ReferencedColumns)
		 */
		row.setColumn(12, new UserType(rcd));

		/* 13th column is TRIGGERDEFINITION */
		row.setColumn(13, dvf.getLongvarcharDataValue(triggerDefinition));

		/* 14th column is REFERENCINGOLD */
		row.setColumn(14, new SQLBoolean(referencingOld));

		/* 15th column is REFERENCINGNEW */
		row.setColumn(15, new SQLBoolean(referencingNew));

		/* 16th column is OLDREFERENCINGNAME */
		row.setColumn(16, new SQLVarchar(oldReferencingName));

		/* 17th column is NEWREFERENCINGNAME */
		row.setColumn(17, new SQLVarchar(newReferencingName));

        /* 18th column is WHENCLAUSETEXT */
        if (row.nColumns() >= 18) {
            // This column is present only if the data dictionary version is
            // 10.11 or higher.
            row.setColumn(18, dvf.getLongvarcharDataValue(whenClauseText));
        }

		return row;
	}

    /**
     * Get a calendar instance with the correct time zone for storing and
     * retrieving creation timestamps. Creation timestamps are stored in UTC
     * to avoid ambiguities around the change from daylight saving time to
     * standard time, or other time zone changes. If the data dictionary
     * version is less than 10.11, however, the timestamps are stored in the
     * local time zone.
     *
     * @return a calendar instance that can be used for storing and retrieving
     *   trigger creation timestamps
     * @throws StandardException if an error occurs
     */
    private Calendar getCalendarForCreationTimestamp()
            throws StandardException {
        if (dataDictionary.checkVersion(
                DataDictionary.DD_VERSION_DERBY_10_11, null)) {
            return Calendar.getInstance(UTC);
        } else {
            return Calendar.getInstance();
        }
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
            // The expected number of columns depends on the version of the
            // data dictionary. The WHENCLAUSETEXT column was added in version
            // 10.11 (DERBY-534).
            int expectedCols =
                dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_11, null)
                    ? SYSTRIGGERS_COLUMN_COUNT
                    : (SYSTRIGGERS_COLUMN_COUNT - 1);

            SanityManager.ASSERT(row.nColumns() == expectedCols,
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
        createTime = col.getTimestamp(getCalendarForCreationTimestamp());

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
		
        // 13th column is TRIGGERDEFINITION (longvarchar)
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

        // 18th column is WHENCLAUSETEXT (longvarchar)
        String whenClauseText = null;
        if (row.nColumns() >= 18) {
            // This column is present only if the data dictionary version is
            // 10.11 or higher.
            col = row.getColumn(18);
            whenClauseText = col.getString();
        }

        descriptor = ddg.newTriggerDescriptor(
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
									(rcd == null) ? (int[])null : rcd.getTriggerActionReferencedColumnPositions(),
									triggerDefinition,
									referencingOld,
									referencingNew,
									oldReferencingName,
                                    newReferencingName,
                                    whenClauseText
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
        throws StandardException
	{
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("TRIGGERID", false),
            SystemColumnImpl.getIdentifierColumn("TRIGGERNAME", false),
            SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
            SystemColumnImpl.getColumn("CREATIONTIMESTAMP", Types.TIMESTAMP, false),
            SystemColumnImpl.getIndicatorColumn("EVENT"),
            SystemColumnImpl.getIndicatorColumn("FIRINGTIME"),
            SystemColumnImpl.getIndicatorColumn("TYPE"),
            SystemColumnImpl.getIndicatorColumn("STATE"),
            SystemColumnImpl.getUUIDColumn("TABLEID", false),
            SystemColumnImpl.getUUIDColumn("WHENSTMTID", true),
            SystemColumnImpl.getUUIDColumn("ACTIONSTMTID", true),
            SystemColumnImpl.getJavaColumn("REFERENCEDCOLUMNS",
                    "org.apache.derby.catalog.ReferencedColumns", true),
            SystemColumnImpl.getColumn("TRIGGERDEFINITION",
                    Types.LONGVARCHAR, true, Integer.MAX_VALUE),
            SystemColumnImpl.getColumn("REFERENCINGOLD", Types.BOOLEAN, true),
            SystemColumnImpl.getColumn("REFERENCINGNEW", Types.BOOLEAN, true),
            SystemColumnImpl.getIdentifierColumn("OLDREFERENCINGNAME", true),
            SystemColumnImpl.getIdentifierColumn("NEWREFERENCINGNAME", true),
            SystemColumnImpl.getColumn("WHENCLAUSETEXT",
                    Types.LONGVARCHAR, true, Integer.MAX_VALUE),
           };
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

    @Override
    public int getHeapColumnCount() throws StandardException {
        // The WHEN clause (DERBY-534) is only supported if the dictionary
        // version is 10.11 or higher. Older versions of the dictionary don't
        // include the WHENCLAUSETEXT column, so adjust the column count
        // accordingly.
        boolean supportsWhenClause = dataDictionary
                .checkVersion(DataDictionary.DD_VERSION_DERBY_10_11, null);
        int heapCols = super.getHeapColumnCount();
        return supportsWhenClause ? heapCols : (heapCols - 1);
    }
}
