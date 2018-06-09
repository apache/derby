/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSCOLUMNSRowFactory

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

import java.sql.Types;
import java.util.Properties;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.types.*;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.UniqueTupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.impl.sql.compile.ColumnDefinitionNode;

/**
 * Factory for creating a SYSCOLUMNS row.
 *
 *
 * @version 0.1
 */

public class SYSCOLUMNSRowFactory extends CatalogRowFactory
{
	static final String		TABLENAME_STRING = "SYSCOLUMNS";

	protected static final int		SYSCOLUMNS_COLUMN_COUNT = 10;
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
	protected static final int		SYSCOLUMNS_AUTOINCREMENTINCCYCLE = 10;

	//private static final String	SYSCOLUMNS_INDEX1_NAME = "SYSCOLUMNS_INDEX1";
	protected static final int		SYSCOLUMNS_INDEX1_ID = 0;

	//private static final String	SYSCOLUMNS_INDEX2_NAME = "SYSCOLUMNS_INDEX2";
	protected static final int		SYSCOLUMNS_INDEX2_ID = 1;

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

	/////////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////
	
	// SYSCOLUMNSRowFactory class needs to access the DD version to perform soft / hard upgrades if any.
	private final DataDictionary dataDictionary;


    SYSCOLUMNSRowFactory(DataDictionary dd,UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		this(dd, uuidf, ef, dvf, TABLENAME_STRING);
	}

    SYSCOLUMNSRowFactory(DataDictionary dd,UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf,
                                 String myName )
	{
		super(uuidf,ef,dvf);
		this.dataDictionary = dd;
		initInfo(SYSCOLUMNS_COLUMN_COUNT, myName, indexColumnPositions, uniqueness, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSCOLUMNS row
	 *
	 * @return	Row suitable for inserting into SYSCOLUMNS.
	 *
	 * @exception   StandardException thrown on failure
	 */
  	@Override
  	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException{
	return makeRow(td, getHeapColumnCount());
	}

	@Override
	public ExecRow makeEmptyRowForCurrentVersion() throws StandardException {
        return makeRow(null, SYSCOLUMNS_COLUMN_COUNT);
    }

	private ExecRow makeRow(TupleDescriptor td, int columnCount)
					throws StandardException
	{
		ExecRow    				row;

		String					colName = null;
		String					defaultID = null;
		String					tabID = null;
		Integer					colID = null;
		TypeDescriptor 		    typeDesc = null;
		Object					defaultSerializable = null;
		long					autoincStart = 0;
		long					autoincInc = 0;
		long					autoincValue = 0;
		boolean					autoincCycle = false;
		//The SYSCOLUMNS table's autoinc related columns change with different
		//values depending on what happened to the autoinc column, ie is the 
		//user adding an autoincrement column, or is user changing the existing 
		//autoincrement column to change it's increment value or to change it's
		//start value? Following variable is used to keep track of what happened 
		//to the autoincrement column.
		long autoinc_create_or_modify_Start_Increment = -1;

		if (td != null)
		{
			ColumnDescriptor  column = (ColumnDescriptor)td;
		
			/* Lots of info in the column's type descriptor */
			typeDesc = column.getType().getCatalogType();

			tabID = column.getReferencingUUID().toString();
			colName = column.getColumnName();
			colID = column.getPosition();
			autoincStart = column.getAutoincStart();
			autoincInc   = column.getAutoincInc();
			autoincValue   = column.getAutoincValue();
			autoinc_create_or_modify_Start_Increment = column.getAutoinc_create_or_modify_Start_Increment();
			autoincCycle = column.getAutoincCycle();

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
		row = getExecutionFactory().getValueRow(columnCount);

		/* 1st column is REFERENCEID (UUID - char(36)) */
		row.setColumn(SYSCOLUMNS_REFERENCEID, new SQLChar(tabID));

		/* 2nd column is COLUMNNAME (varchar(128)) */
		row.setColumn(SYSCOLUMNS_COLUMNNAME, new SQLVarchar(colName));

		/* 3rd column is COLUMNNUMBER (int) */
		row.setColumn(SYSCOLUMNS_COLUMNNUMBER, new SQLInteger(colID));

		/* 4th column is COLUMNDATATYPE */
		row.setColumn(SYSCOLUMNS_COLUMNDATATYPE,
				new UserType(typeDesc));

		/* 5th column is COLUMNDEFAULT */
		row.setColumn(SYSCOLUMNS_COLUMNDEFAULT,
					  new UserType(defaultSerializable));

		/* 6th column is DEFAULTID (UUID - char(36)) */
		row.setColumn(SYSCOLUMNS_COLUMNDEFAULTID, new SQLChar(defaultID));

		if (
            (autoinc_create_or_modify_Start_Increment ==
             ColumnDefinitionNode.CREATE_AUTOINCREMENT) ||
            (autoinc_create_or_modify_Start_Increment ==
             ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE) ||
            (autoinc_create_or_modify_Start_Increment ==
             ColumnDefinitionNode.MODIFY_AUTOINCREMENT_ALWAYS_VS_DEFAULT)
            )
		{
            //user is adding an autoinc column
            // or is changing the increment value of autoinc column
            // or is changing an autoinc column between ALWAYS and DEFAULT.
			// This code also gets run when ALTER TABLE DROP COLUMN
			// is used to drop a column other than the autoinc
			// column, and the autoinc column gets removed from
			// SYSCOLUMNS and immediately re-added with a different
			// column position (to account for the dropped column).
			// In this case, the autoincValue may have a
			// different value than the autoincStart.
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, 
						  new SQLLongint(autoincValue));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, 
						  new SQLLongint(autoincStart));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC, 
						  new SQLLongint(autoincInc));
			if (row.nColumns() >= 10) {
            // This column is present only if the data dictionary version is
            // 10.14 or higher.
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTINCCYCLE,
					new SQLBoolean(autoincCycle));
        	}
			
		} else if (autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE)
		{//user asked for restart with a new value, so don't change increment by and original start
			//with values in the SYSCOLUMNS table. Just record the RESTART WITH value as the
			//next value to be generated in the SYSCOLUMNS table

			ColumnDescriptor  column = (ColumnDescriptor)td;
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, new SQLLongint(autoincStart));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, new SQLLongint(autoincStart));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC, new SQLLongint(
					column.getTableDescriptor().getColumnDescriptor(colName).getAutoincInc()));
			if (row.nColumns() >= 10) {
            // This column is present only if the data dictionary version is
            // 10.14 or higher.
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTINCCYCLE,
					new SQLBoolean(autoincCycle));
        	}
		}
		else if(autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_CYCLE_VALUE){
			ColumnDescriptor  column = (ColumnDescriptor)td;
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, new SQLLongint(
					column.getTableDescriptor().getColumnDescriptor(colName).getAutoincValue()));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, new SQLLongint(
					column.getTableDescriptor().getColumnDescriptor(colName).getAutoincStart()));
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC, new SQLLongint(
					column.getTableDescriptor().getColumnDescriptor(colName).getAutoincInc()));
			if (row.nColumns() >= 10) {
            // This column is present only if the data dictionary version is
            // 10.14 or higher.
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTINCCYCLE,
					new SQLBoolean(autoincCycle));
        	}
		}
		else
		{
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTVALUE, 
						  new SQLLongint());
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTSTART, 
						  new SQLLongint());
			row.setColumn(SYSCOLUMNS_AUTOINCREMENTINC,
						  new SQLLongint());
			if (row.nColumns() >= 10) {
            // This column is present only if the data dictionary version is
            // 10.14 or higher.
            row.setColumn(SYSCOLUMNS_AUTOINCREMENTINCCYCLE,
					new SQLBoolean(autoincCycle));
        	}
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
			
			int expectedCols =
                dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_14, null)
                    ? SYSCOLUMNS_COLUMN_COUNT
                    : (SYSCOLUMNS_COLUMN_COUNT - 1);


			SanityManager.ASSERT(row.nColumns() == expectedCols, 
								 "Wrong number of columns for a SYSCOLUMNS row");
		}

		int columnNumber;
		String columnName;
		String defaultID;
		DefaultInfoImpl		defaultInfo = null;
		ColumnDescriptor colDesc;
		DataValueDescriptor	defaultValue = null;
		UUID				defaultUUID = null;
		UUID				uuid = null;
		UUIDFactory			uuidFactory = getUUIDFactory();
		long autoincStart, autoincInc, autoincValue;
		boolean autoincCycle = false;

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
		TypeDescriptor catalogType = (TypeDescriptor) row.getColumn(SYSCOLUMNS_COLUMNDATATYPE).
													getObject();
		DataTypeDescriptor dataTypeServices = 
			DataTypeDescriptor.getType(catalogType);

		/* 7th column is AUTOINCREMENTVALUE (long) */
		autoincValue = row.getColumn(SYSCOLUMNS_AUTOINCREMENTVALUE).getLong();

		/* 8th column is AUTOINCREMENTSTART (long) */
		autoincStart = row.getColumn(SYSCOLUMNS_AUTOINCREMENTSTART).getLong();

		/* 9th column is AUTOINCREMENTINC (long) */
		autoincInc = row.getColumn(SYSCOLUMNS_AUTOINCREMENTINC).getLong();

		if (row.nColumns() >= 10){
			DataValueDescriptor col = row.getColumn(SYSCOLUMNS_AUTOINCREMENTINCCYCLE);
			autoincCycle = col.getBoolean();
		}


		DataValueDescriptor col = row.getColumn(SYSCOLUMNS_AUTOINCREMENTSTART);
		autoincStart = col.getLong();

		col = row.getColumn(SYSCOLUMNS_AUTOINCREMENTINC);
		autoincInc = col.getLong();

		// Hard upgraded tables <=10.13 come with a false autoincCyle before they are first
		// explicitly set with cycle or no cycle command.
		colDesc = new ColumnDescriptor(columnName, columnNumber,
							dataTypeServices, defaultValue, defaultInfo, uuid, 
							defaultUUID, autoincStart, autoincInc,
                            autoincValue, autoincCycle);
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
        throws StandardException
	{
        
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("REFERENCEID", false),
            SystemColumnImpl.getIdentifierColumn("COLUMNNAME", false),
            SystemColumnImpl.getColumn("COLUMNNUMBER", Types.INTEGER, false),
            SystemColumnImpl.getJavaColumn("COLUMNDATATYPE",
                "org.apache.derby.catalog.TypeDescriptor", false),
            SystemColumnImpl.getJavaColumn("COLUMNDEFAULT",
                "java.io.Serializable", true),
            SystemColumnImpl.getUUIDColumn("COLUMNDEFAULTID", true),
            
            SystemColumnImpl.getColumn("AUTOINCREMENTVALUE", Types.BIGINT, true),
            SystemColumnImpl.getColumn("AUTOINCREMENTSTART", Types.BIGINT, true),
            SystemColumnImpl.getColumn("AUTOINCREMENTINC", Types.BIGINT, true),
            SystemColumnImpl.getColumn("AUTOINCREMENTCYCLE", Types.BOOLEAN, true)

       };
	}

	@Override
	public int getHeapColumnCount() throws StandardException {
        // The CYCLE clause (DERBY-6904) is only supported if the dictionary
        // version is 10.14 or higher.
		boolean supportsCycleClause = dataDictionary
                .checkVersion(DataDictionary.DD_VERSION_DERBY_10_14, null);
        int heapCols = super.getHeapColumnCount();
        return supportsCycleClause ? heapCols : (heapCols - 1);
    }
}
