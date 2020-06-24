/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSSTATEMENTSRowFactory

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

import org.apache.derby.shared.common.reference.Property;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.types.*;

import org.apache.derby.shared.common.sanity.SanityManager;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;

/**
 * Factory for creating a SYSSTATEMENTS row.
 *
 *
 * @version 0.1
 */

public class SYSSTATEMENTSRowFactory extends CatalogRowFactory
{
	static	final	String	TABLENAME_STRING = "SYSSTATEMENTS";

	/* Column #s for sysinfo (1 based) */
	public	static	final	int		SYSSTATEMENTS_STMTID = 1;
	public	static	final	int		SYSSTATEMENTS_STMTNAME = 2;
	public	static	final	int		SYSSTATEMENTS_SCHEMAID = 3;
	public	static	final	int		SYSSTATEMENTS_TYPE = 4;
	public	static	final	int		SYSSTATEMENTS_VALID = 5;
	public	static	final	int		SYSSTATEMENTS_TEXT = 6;
	public	static	final	int		SYSSTATEMENTS_LASTCOMPILED = 7;
	public	static	final	int		SYSSTATEMENTS_COMPILATION_SCHEMAID = 8;
	public	static	final	int		SYSSTATEMENTS_USINGTEXT = 9;
	public	static	final	int		SYSSTATEMENTS_CONSTANTSTATE = 10;
	public	static	final	int		SYSSTATEMENTS_INITIALLY_COMPILABLE = 11;

	public	static	final	int		SYSSTATEMENTS_COLUMN_COUNT = SYSSTATEMENTS_INITIALLY_COMPILABLE;

	public	static	final	int		SYSSTATEMENTS_HIDDEN_COLUMN_COUNT = 2;

	protected static final int		SYSSTATEMENTS_INDEX1_ID = 0;
	protected static final int		SYSSTATEMENTS_INDEX2_ID = 1;


	private static final int[][] indexColumnPositions = 
	{
		{SYSSTATEMENTS_STMTID},
		{SYSSTATEMENTS_STMTNAME, SYSSTATEMENTS_SCHEMAID}
	};

	private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		 "80000000-00d1-15f7-ab70-000a0a0b1500"	// catalog UUID
		,"80000000-00d1-15fc-60b9-000a0a0b1500"	// heap UUID
		,"80000000-00d1-15fc-eda1-000a0a0b1500"	// SYSSTATEMENTS_INDEX1
		,"80000000-00d1-15fe-bdf8-000a0a0b1500"	// SYSSTATEMENTS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

//IC see: https://issues.apache.org/jira/browse/DERBY-3147
    SYSSTATEMENTSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSSTATEMENTS_COLUMN_COUNT, TABLENAME_STRING, 
//IC see: https://issues.apache.org/jira/browse/DERBY-1739
				 indexColumnPositions, uniqueness, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SYSSTATEMENTS row. 
	 * <p>
	 * <B>WARNING</B>: When empty row is true, this method takes
	 * a snapshot of the SPSD and creates a row.  It is imperative
	 * that that row remain consistent with the descriptor (the
	 * valid and StorablePreparedStatement fields must be in sync).
	 * If this row is to be written out and valid is true, then
	 * this call and the insert should be synchronized on the
	 * SPSD. This method has <B>NO</B> synchronization.
	 * 
	 * @param compileMe			passed into SPSDescriptorImpl.getPreparedStatement().
	 *							if true, we (re)compile the stmt
	 * @param spsDescriptor		In-memory tuple to be converted to a disk row.
	 *
	 * @return	Row suitable for inserting into SYSSTATEMENTS.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeSYSSTATEMENTSrow(
		boolean				compileMe,
		SPSDescriptor		spsDescriptor
	) throws StandardException
	{
		DataTypeDescriptor		dtd;
		ExecRow    				row;
		DataValueDescriptor		col;
		String					name = null;
		UUID					uuid = null;
		String					uuidStr = null;	
		String					suuidStr = null;	// schema	
		String					compUuidStr = null;	// compilation schema	
		String					text = null;
		String					usingText = null;
		ExecPreparedStatement	preparedStatement = null;
		String					typeStr = null;
		boolean					valid = true;
		Timestamp				time = null;
		boolean					initiallyCompilable = true;

		if (spsDescriptor != null)
		{
			name = spsDescriptor.getName();
			uuid = spsDescriptor.getUUID();
			suuidStr = spsDescriptor.getSchemaDescriptor().getUUID().toString();
			uuidStr = uuid.toString();
			text = spsDescriptor.getText();			
			valid = spsDescriptor.isValid();
			time = spsDescriptor.getCompileTime();
			typeStr = spsDescriptor.getTypeAsString();
			initiallyCompilable = spsDescriptor.initiallyCompilable();
			preparedStatement = spsDescriptor.getPreparedStatement(compileMe);
			compUuidStr = (spsDescriptor.getCompSchemaId() != null)?
					spsDescriptor.getCompSchemaId().toString():null;
			usingText = spsDescriptor.getUsingText();
		}

		/* Build the row to insert */
		row = getExecutionFactory().getValueRow(SYSSTATEMENTS_COLUMN_COUNT);

		/* 1st column is STMTID */
		row.setColumn(1, new SQLChar(uuidStr));

		/* 2nd column is STMTNAME */
		row.setColumn(2, new SQLVarchar(name));

		/* 3rd column is SCHEMAID */
		row.setColumn(3, new SQLChar(suuidStr));

		/* 4th column is TYPE */
		row.setColumn(4, new SQLChar(typeStr));

		/* 5th column is VALID */
		row.setColumn(5, new SQLBoolean(valid));
//IC see: https://issues.apache.org/jira/browse/DERBY-4062

		/* 6th column is TEXT */
		row.setColumn(6, dvf.getLongvarcharDataValue(text));

		/* 7th column is LASTCOMPILED */
		row.setColumn(7, new SQLTimestamp(time));

		/* 8th column is COMPILATIONSCHEMAID */
		row.setColumn(8, new SQLChar(compUuidStr));

		/* 9th column is USINGTEXT */
		row.setColumn(9, dvf.getLongvarcharDataValue(usingText));

		/* 
		** 10th column is CONSTANTSTATE
		**
		** CONSTANTSTATE is really a formatable StorablePreparedStatement.
		*/
		row.setColumn(10, new UserType(preparedStatement));
//IC see: https://issues.apache.org/jira/browse/DERBY-4062

		/* 11th column is INITIALLY_COMPILABLE */
		row.setColumn(11, new SQLBoolean(initiallyCompilable));

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make an  Tuple Descriptor out of a SYSSTATEMENTS row
	 *
	 * @param row 					a SYSSTATEMENTS row
	 * @param parentTupleDescriptor	unused
	 * @param dd 					dataDictionary
	 *
	 * @return	a  descriptor equivalent to a SYSSTATEMENTS row
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		DataValueDescriptor			col;
		SPSDescriptor				descriptor;
		String						name;
		String						text;
		String						usingText;
		UUID						uuid;
		UUID						compUuid = null;
		String						uuidStr;
		UUID						suuid;		// schema
		String						suuidStr;	// schema
		String						typeStr;
		char						type;
		boolean						valid;
		Timestamp					time = null;
		ExecPreparedStatement		preparedStatement = null;
		boolean						initiallyCompilable;
		DataDescriptorGenerator		ddg = dd.getDataDescriptorGenerator();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row.nColumns() == SYSSTATEMENTS_COLUMN_COUNT, 
								 "Wrong number of columns for a SYSSTATEMENTS row");
		}

		// 1st column is STMTID (UUID - char(36))
		col = row.getColumn(1);
		uuidStr = col.getString();
		uuid = getUUIDFactory().recreateUUID(uuidStr);

		// 2nd column is STMTNAME (varchar(128))
		col = row.getColumn(2);
		name = col.getString();

		// 3rd column is SCHEMAID (UUID - char(36))
		col = row.getColumn(3);
		suuidStr = col.getString();
		suuid = getUUIDFactory().recreateUUID(suuidStr);

		// 4th column is TYPE (char(1))
		col = row.getColumn(4);
		type = col.getString().charAt(0);

		if (SanityManager.DEBUG)
		{
			if (!SPSDescriptor.validType(type))
			{
				SanityManager.THROWASSERT("Bad type value ("+type+") for  statement "+name);
			}
		}

		// In soft upgrade mode the plan may not be understand by this engine
		// so force a recompile.
//IC see: https://issues.apache.org/jira/browse/DERBY-4845
		if (dd.isReadOnlyUpgrade()) {
			valid = false;
		} else {
			// 5th column is VALID (boolean)
			col = row.getColumn(5);
			valid = col.getBoolean();
		}

		// 6th column is TEXT (LONG VARCHAR)
		col = row.getColumn(6);
		text = col.getString();

		/* 7th column is LASTCOMPILED (TIMESTAMP) */
		col = row.getColumn(7);
		time = col.getTimestamp(new java.util.GregorianCalendar());

		// 8th column is COMPILATIONSCHEMAID (UUID - char(36))
		col = row.getColumn(8);
		uuidStr = col.getString();
		if (uuidStr != null)
			compUuid = getUUIDFactory().recreateUUID(uuidStr);

		// 9th column is TEXT (LONG VARCHAR)
		col = row.getColumn(9);
		usingText = col.getString();

		// 10th column is CONSTANTSTATE (COM...ExecPreparedStatement)

		// Only load the compiled plan if the statement is valid
		if (valid) {
			col = row.getColumn(10);
			preparedStatement = (ExecPreparedStatement) col.getObject();
		}

		// 11th column is INITIALLY_COMPILABLE (boolean)
		col = row.getColumn(11);
		if ( col.isNull() ) { initiallyCompilable = true; }
		else { initiallyCompilable = col.getBoolean(); }

		descriptor = new SPSDescriptor(dd, name, 
									uuid, 
									suuid,
									compUuid,
									type, 
									valid,
									text,
									usingText,
									time,
									preparedStatement,
									initiallyCompilable
									);

		return descriptor;
	}

	public ExecRow makeEmptyRow()
		throws StandardException
 	{
 		return makeSYSSTATEMENTSrow(false,
 							   		(SPSDescriptor) null);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        throws StandardException
	{
            return new SystemColumn[] {
                SystemColumnImpl.getUUIDColumn("STMTID", false),
                SystemColumnImpl.getIdentifierColumn("STMTNAME", false),
                SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
                SystemColumnImpl.getIndicatorColumn("TYPE"),
                SystemColumnImpl.getColumn("VALID", Types.BOOLEAN, false),
                SystemColumnImpl.getColumn("TEXT", Types.LONGVARCHAR, false,
                        TypeId.LONGVARCHAR_MAXWIDTH),
                SystemColumnImpl.getColumn("LASTCOMPILED", Types.TIMESTAMP, true),
                SystemColumnImpl.getUUIDColumn("COMPILATIONSCHEMAID", true),
                SystemColumnImpl.getColumn("USINGTEXT", Types.LONGVARCHAR, true,
                        TypeId.LONGVARCHAR_MAXWIDTH),         
            };
		/*
		** This column is deliberately left out.  It
	 	** is effectively 'hidden' from users.  The code
	 	** to create it is left here to demonstrate what
		** it really looks like.
		*/
		//columnList[9] = 
		//			new SystemColumnImpl(		
		//					convertIdCase( "CONSTANTSTATE"),			// name 
		//					SYSSTATEMENTS_CONSTANTSTATE,// column number
		//					0,							// precision
		//					0,							// scale
		//					false,						// nullability
		//					ExecPreparedStatement.CLASS_NAME,	//datatype
		//					false,						// built-in type
		//					DataTypeDescriptor.MAXIMUM_WIDTH_UNKNOWN	// maxLength
		//	                );

		/*
		** This column is also deliberately left out.  It
	 	** is effectively 'hidden' from users.  The code
	 	** to create it is left here to demonstrate what
		** it really looks like.
		*/
		//columnList[10] = 
		//			new SystemColumnImpl(		
		//					convertIdCase( "INITIALLY_COMPILABLE"),			// name 
		//					SYSSTATEMENTS_INITIALLY_COMPILABLE,// column number
		//					0,					// precision
		//					0,					// scale
		//					true,				// nullability
		//					"BOOLEAN",			// dataType
		//					true,				// built-in type
		//					1					// maxLength
		//	                );

	}

	/**
	 * Get the Properties associated with creating the heap.
	 *
	 * @return The Properties associated with creating the heap.
	 */
	public Properties getCreateHeapProperties()
	{
		Properties properties = new Properties();

		// keep page size at 2K since most stmts are that size
		// anyway
		properties.put(Property.PAGE_SIZE_PARAMETER,"2048");

		// default properties for system tables:
		properties.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER,"0");
		properties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,"1");
		return properties;
	}

}
