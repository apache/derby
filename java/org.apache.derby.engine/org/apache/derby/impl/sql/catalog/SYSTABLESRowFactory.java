/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSTABLESRowFactory

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

import org.apache.derby.iapi.sql.dictionary.SystemColumn;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLVarchar;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Factory for creating a SYSTABLES row.
 *
 *
 * @version 0.1
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-1674
class SYSTABLESRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSTABLES";

	protected static final int		SYSTABLES_COLUMN_COUNT = 5;
	/* Column #s for systables (1 based) */
	protected static final int		SYSTABLES_TABLEID = 1;
	protected static final int		SYSTABLES_TABLENAME = 2;
	protected static final int		SYSTABLES_TABLETYPE = 3;
	protected static final int		SYSTABLES_SCHEMAID = 4;
	protected static final int		SYSTABLES_LOCKGRANULARITY = 5;

	protected static final int		SYSTABLES_INDEX1_ID = 0;
	protected static final int		SYSTABLES_INDEX1_TABLENAME = 1;
	protected static final int		SYSTABLES_INDEX1_SCHEMAID = 2;

	protected static final int		SYSTABLES_INDEX2_ID = 1;
	protected static final int		SYSTABLES_INDEX2_TABLEID = 1;
	
	// all indexes are unique.

	private	static	final	String[]	uuids =
	{
		 "80000018-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000028-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000001a-00d0-fd77-3ed8-000a0a0b1900"	// SYSTABLES_INDEX1
		,"8000001c-00d0-fd77-3ed8-000a0a0b1900"	// SYSTABLES_INDEX2
	};

	private static final int[][] indexColumnPositions = 
	{ 
		{ SYSTABLES_TABLENAME, SYSTABLES_SCHEMAID},
		{ SYSTABLES_TABLEID }
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

//IC see: https://issues.apache.org/jira/browse/DERBY-3147
    SYSTABLESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
//IC see: https://issues.apache.org/jira/browse/DERBY-1739
		initInfo(SYSTABLES_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, (boolean[]) null, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SYSTABLES row
	 *
	 * @return	Row suitable for inserting into SYSTABLES.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td,
						   TupleDescriptor	parent)
					throws StandardException
	{
		UUID						oid;
		String	   				tabSType = null;
		int	   					tabIType;
		ExecRow        			row;
		String					lockGranularity = null;
		String					tableID = null;
		String					schemaID = null;
		String					tableName = null;


		if (td != null)
		{
			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			TableDescriptor descriptor = (TableDescriptor)td;
			SchemaDescriptor schema = (SchemaDescriptor)parent;

			oid = descriptor.getUUID();
			if ( oid == null )
		    {
				oid = getUUIDFactory().createUUID();
				descriptor.setUUID(oid);
			}
			tableID = oid.toString();
			
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(schema != null, 
							"Schema should not be null unless empty row is true");
				if (schema.getUUID() == null)
				{
					SanityManager.THROWASSERT("schema " + schema + " has a null OID");
				}
			}
		
			schemaID = schema.getUUID().toString();

			tableName = descriptor.getName();

			/* RESOLVE - Table Type should really be a char in the descriptor
			 * T, S, V, S instead of 0, 1, 2, 3
			 */
			tabIType = descriptor.getTableType();
			switch (tabIType)
			{
			    case TableDescriptor.BASE_TABLE_TYPE:
					tabSType = "T";
					break;
			    case TableDescriptor.SYSTEM_TABLE_TYPE:
					tabSType = "S";
					break;
			    case TableDescriptor.VIEW_TYPE:
					tabSType = "V";
					break;		

//IC see: https://issues.apache.org/jira/browse/DERBY-335
			    case TableDescriptor.SYNONYM_TYPE:
					tabSType = "A";
					break;		

			    default:
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT("invalid table type");
			}
			char[] lockGChar = new char[1];
			lockGChar[0] = descriptor.getLockGranularity();
			lockGranularity = new String(lockGChar);
		}

		/* Insert info into systables */

		/* RESOLVE - It would be nice to require less knowledge about systables
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSTABLES_COLUMN_COUNT);

		/* 1st column is TABLEID (UUID - char(36)) */
		row.setColumn(SYSTABLES_TABLEID, new SQLChar(tableID));

		/* 2nd column is NAME (varchar(30)) */
		row.setColumn(SYSTABLES_TABLENAME, new SQLVarchar(tableName));

		/* 3rd column is TABLETYPE (char(1)) */
		row.setColumn(SYSTABLES_TABLETYPE, new SQLChar(tabSType));

		/* 4th column is SCHEMAID (UUID - char(36)) */
		row.setColumn(SYSTABLES_SCHEMAID, new SQLChar(schemaID));

		/* 5th column is LOCKGRANULARITY (char(1)) */
		row.setColumn(SYSTABLES_LOCKGRANULARITY, new SQLChar(lockGranularity));

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1674
	ExecIndexRow	buildEmptyIndexRow( int indexNumber,
											RowLocation rowLocation)
			throws StandardException
	{
		int ncols = getIndexColumnCount(indexNumber);
		ExecIndexRow row = getExecutionFactory().getIndexableRow(ncols + 1);

		row.setColumn(ncols + 1, rowLocation);

		switch( indexNumber )
		{
		    case SYSTABLES_INDEX1_ID:
				/* 1st column is TABLENAME (varchar(128)) */
				row.setColumn(1, new SQLVarchar());

				/* 2nd column is SCHEMAID (UUID - char(36)) */
				row.setColumn(2, new SQLChar());

				break;

		    case SYSTABLES_INDEX2_ID:
				/* 1st column is TABLEID (UUID - char(36)) */
				row.setColumn(1,new SQLChar());
				break;
		}	// end switch

		return	row;
	}

	/**
	 * Make a TableDescriptor out of a SYSTABLES row
	 *
	 * @param row a SYSTABLES row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 * @param isolationLevel use this explicit isolation level. Only
	 *                       ISOLATION_REPEATABLE_READ (normal usage)
	 *                       or ISOLATION_READ_UNCOMMITTED (corner
	 *                       cases) supported for now.
	 * @exception   StandardException thrown on failure
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-3678
	TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd,
		int                     isolationLevel)
					throws StandardException
	{
		return buildDescriptorBody(row,
								   parentTupleDescriptor,
								   dd,
								   isolationLevel);
	}


	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a TableDescriptor out of a SYSTABLES row
	 *
	 * @param row a SYSTABLES row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @return	a table descriptor equivalent to a SYSTABLES row
	 *
	 * @exception   StandardException thrown on failure
	 */

	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3678
		return buildDescriptorBody(
			row,
			parentTupleDescriptor,
			dd,
			TransactionController.ISOLATION_REPEATABLE_READ);
	}


	public TupleDescriptor buildDescriptorBody(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd,
		int                     isolationLevel)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(row.nColumns() == SYSTABLES_COLUMN_COUNT, "Wrong number of columns for a SYSTABLES row");

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		String	tableUUIDString; 
		String	schemaUUIDString; 
		int		tableTypeEnum;
		String	lockGranularity;
		String	tableName, tableType;
		DataValueDescriptor	col;
		UUID		tableUUID;
		UUID		schemaUUID;
		SchemaDescriptor	schema;
		TableDescriptor		tabDesc;

		/* 1st column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSTABLES_TABLEID);
		tableUUIDString = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);


		/* 2nd column is TABLENAME (varchar(128)) */
		col = row.getColumn(SYSTABLES_TABLENAME);
		tableName = col.getString();

		/* 3rd column is TABLETYPE (char(1)) */
		col = row.getColumn(SYSTABLES_TABLETYPE);
		tableType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tableType.length() == 1, "Fourth column type incorrect");
		}
		switch (tableType.charAt(0))
		{
			case 'T' : 
				tableTypeEnum = TableDescriptor.BASE_TABLE_TYPE;
				break;
			case 'S' :
				tableTypeEnum = TableDescriptor.SYSTEM_TABLE_TYPE;
				break;
			case 'V' :
				tableTypeEnum = TableDescriptor.VIEW_TYPE;
				break;
//IC see: https://issues.apache.org/jira/browse/DERBY-335
			case 'A' :
				tableTypeEnum = TableDescriptor.SYNONYM_TYPE;
				break;
			default:
				if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Fourth column value invalid");
				tableTypeEnum = -1;
		}

		/* 4th column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSTABLES_SCHEMAID);
		schemaUUIDString = col.getString();
		schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);
		
		schema = dd.getSchemaDescriptor(schemaUUID, isolationLevel, null);
//IC see: https://issues.apache.org/jira/browse/DERBY-3678

		/* 5th column is LOCKGRANULARITY (char(1)) */
		col = row.getColumn(SYSTABLES_LOCKGRANULARITY);
		lockGranularity = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(lockGranularity.length() == 1, "Fifth column type incorrect");
		}

		// RESOLVE - Deal with lock granularity
		tabDesc = ddg.newTableDescriptor(tableName, schema, tableTypeEnum, lockGranularity.charAt(0));
		tabDesc.setUUID(tableUUID);
		return tabDesc;
	}

	/**
	 *	Get the table name out of this SYSTABLES row
	 *
	 * @param row a SYSTABLES row
	 *
	 * @return	string, the table name
	 *
	 * @exception   StandardException thrown on failure
	 */
	protected String getTableName(ExecRow	row)
					throws StandardException
	{
		DataValueDescriptor	col;

		col = row.getColumn(SYSTABLES_TABLENAME);
		return col.getString();
	}


	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        throws StandardException
	{
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("TABLEID", false),
            SystemColumnImpl.getIdentifierColumn("TABLENAME", false),
            SystemColumnImpl.getIndicatorColumn("TABLETYPE"),
            SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
            SystemColumnImpl.getIndicatorColumn("LOCKGRANULARITY"),
        };
	}

}
