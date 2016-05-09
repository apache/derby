/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSCONGLOMERATESRowFactory

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

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

import org.apache.derby.catalog.IndexDescriptor;

import java.sql.Types;
import java.util.Properties;

/**
 * Factory for creating a SYSCONGLOMERATES row.
 *
 */

public class SYSCONGLOMERATESRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSCONGLOMERATES";

	protected static final int		SYSCONGLOMERATES_COLUMN_COUNT = 8;
	protected static final int		SYSCONGLOMERATES_SCHEMAID = 1;
	protected static final int		SYSCONGLOMERATES_TABLEID = 2;
	protected static final int		SYSCONGLOMERATES_CONGLOMERATENUMBER = 3;
	protected static final int		SYSCONGLOMERATES_CONGLOMERATENAME = 4;
	protected static final int		SYSCONGLOMERATES_ISINDEX = 5;
	protected static final int		SYSCONGLOMERATES_DESCRIPTOR = 6;
	protected static final int		SYSCONGLOMERATES_ISCONSTRAINT = 7;
	protected static final int		SYSCONGLOMERATES_CONGLOMERATEID = 8;

	protected static final int		SYSCONGLOMERATES_INDEX1_ID = 0;
	protected static final int		SYSCONGLOMERATES_INDEX2_ID = 1;
	protected static final int		SYSCONGLOMERATES_INDEX3_ID = 2;

    private	static	final	boolean[]	uniqueness = {
		                                               false,
													   true,
													   false
	                                                 };

	private static final int[][] indexColumnPositions =
	{
		{SYSCONGLOMERATES_CONGLOMERATEID},
		{SYSCONGLOMERATES_CONGLOMERATENAME, SYSCONGLOMERATES_SCHEMAID},
		{SYSCONGLOMERATES_TABLEID}
	};

	private	static	final	String[]	uuids =
	{
		 "80000010-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000027-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000012-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONGLOMERATES_INDEX1
		,"80000014-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONGLOMERATES_INDEX2
		,"80000016-00d0-fd77-3ed8-000a0a0b1900"	// SYSCONGLOMERATES_INDEX3
	};

	SYSCONGLOMERATESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSCONGLOMERATES_COLUMN_COUNT, 
				 TABLENAME_STRING, indexColumnPositions, 
				 uniqueness, uuids );
	}

  /**
	 * Make a SYSCONGLOMERATES row
	 *
	 * @return	Row suitable for inserting into SYSCONGLOMERATES.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException
	{
		ExecRow    				row;
		DataValueDescriptor		col;
		String					tabID =null;
		Long					conglomNumber = null;
		String					conglomName = null;
		Boolean					supportsIndex = null;
		IndexRowGenerator		indexRowGenerator = null;
		Boolean					supportsConstraint = null;
		String					conglomUUIDString = null;
		String					schemaID = null;
		ConglomerateDescriptor  conglomerate = (ConglomerateDescriptor)td;

		/* Insert info into sysconglomerates */

		if (td != null)
		{
			/* Sometimes the SchemaDescriptor is non-null and sometimes it
			 * is null.  (We can't just rely on getting the schema id from 
			 * the ConglomerateDescriptor because it can be null when
			 * we are creating a new conglomerate.
			 */
			if (parent != null)
			{
				SchemaDescriptor sd = (SchemaDescriptor)parent;
				schemaID = sd.getUUID().toString();	
			}
			else
			{
				schemaID = conglomerate.getSchemaID().toString();	
			}
			tabID = conglomerate.getTableID().toString();
			conglomNumber = conglomerate.getConglomerateNumber();
			conglomName = conglomerate.getConglomerateName();
			conglomUUIDString = conglomerate.getUUID().toString();

			supportsIndex = conglomerate.isIndex();
			indexRowGenerator = conglomerate.getIndexDescriptor();
			supportsConstraint = conglomerate.isConstraint();
		}

		/* RESOLVE - It would be nice to require less knowledge about sysconglomerates
		 * and have this be more table driven.
		 */

		/* Build the row to insert */
		row = getExecutionFactory().getValueRow(SYSCONGLOMERATES_COLUMN_COUNT);

		/* 1st column is SCHEMAID (UUID - char(36)) */
		row.setColumn(1, new SQLChar(schemaID));

		/* 2nd column is TABLEID (UUID - char(36)) */
		row.setColumn(2, new SQLChar(tabID));

		/* 3rd column is CONGLOMERATENUMBER (long) */
		row.setColumn(3, new SQLLongint(conglomNumber));

		/* 4th column is CONGLOMERATENAME (varchar(128)) 
		** If null, use the tableid so we always
		** have a unique column
		*/
		row.setColumn(4, (conglomName == null) ?
                new SQLVarchar(tabID): new SQLVarchar(conglomName));

		/* 5th  column is ISINDEX (boolean) */
		row.setColumn(5, new SQLBoolean(supportsIndex));

		/* 6th column is DESCRIPTOR
		*  (user type org.apache.derby.catalog.IndexDescriptor)
		*/
		row.setColumn(6,
			new UserType(
						(indexRowGenerator == null ?
							(IndexDescriptor) null :
							indexRowGenerator.getIndexDescriptor()
						)
					)
				);

		/* 7th column is ISCONSTRAINT (boolean) */
		row.setColumn(7, new SQLBoolean(supportsConstraint));

		/* 8th column is CONGLOMERATEID (UUID - char(36)) */
		row.setColumn(8, new SQLChar(conglomUUIDString));

		return row;
	}

	public ExecRow makeEmptyRow() throws StandardException
	{
		return makeRow(null, null);
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
		properties.put("derby.storage.pageSize","4096");
		// default properties for system tables:
		properties.put("derby.storage.pageReservedSpace","0");
		properties.put("derby.storage.minimumRecordSize","1");
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
		properties.put("derby.storage.pageSize","4096");
		return properties;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 *
	 * @param row a SYSCOLUMNS row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @return	a conglomerate descriptor equivalent to a SYSCONGOMERATES row
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
		SanityManager.ASSERT(
			row.nColumns() == SYSCONGLOMERATES_COLUMN_COUNT, 
			"Wrong number of columns for a SYSCONGLOMERATES row");

		DataDescriptorGenerator	ddg = dd.getDataDescriptorGenerator();
		long conglomerateNumber;
		String	name;
		boolean isConstraint;
		boolean isIndex;
		IndexRowGenerator	indexRowGenerator;
		DataValueDescriptor col;
		ConglomerateDescriptor conglomerateDesc;
		String		conglomUUIDString;
		UUID		conglomUUID;
		String		schemaUUIDString;
		UUID		schemaUUID;
		String		tableUUIDString;
		UUID		tableUUID;

		/* 1st column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(1);
		schemaUUIDString = col.getString();
		schemaUUID = getUUIDFactory().recreateUUID(schemaUUIDString);

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(2);
		tableUUIDString = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);


		/* 3nd column is CONGLOMERATENUMBER (long) */
		col = row.getColumn(3);
		conglomerateNumber = col.getLong();

		/* 4rd column is CONGLOMERATENAME (varchar(128)) */
		col = row.getColumn(4);
		name = col.getString();

		/* 5th column is ISINDEX (boolean) */
		col = row.getColumn(5);
		isIndex = col.getBoolean();

		/* 6th column is DESCRIPTOR */
		col = row.getColumn(6);
		indexRowGenerator = new IndexRowGenerator(
			(IndexDescriptor) col.getObject());

		/* 7th column is ISCONSTRAINT (boolean) */
		col = row.getColumn(7);
		isConstraint = col.getBoolean();

		/* 8th column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(8);
		conglomUUIDString = col.getString();
		conglomUUID = getUUIDFactory().recreateUUID(conglomUUIDString);

		/* now build and return the descriptor */
		conglomerateDesc = ddg.newConglomerateDescriptor(conglomerateNumber,
														 name,
														 isIndex,
														 indexRowGenerator,
														 isConstraint,
														 conglomUUID,
														 tableUUID,
														 schemaUUID);
		return conglomerateDesc;
	}

	/**
	 * Get the conglomerate's UUID of the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return UUID	The conglomerates UUID
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getConglomerateUUID(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				conglomerateUUIDString;

		/* 8th column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(SYSCONGLOMERATES_CONGLOMERATEID);
		conglomerateUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(conglomerateUUIDString);
	 }

	/**
	 * Get the table's UUID from the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return UUID	The table's UUID
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getTableUUID(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				tableUUIDString;

		/* 2nd column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSCONGLOMERATES_TABLEID);
		tableUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(tableUUIDString);
	 }

	/**
	 * Get the schema's UUID from the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return UUID	The schema's UUID
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected UUID getSchemaUUID(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;
		String				schemaUUIDString;

		/* 1st column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSCONGLOMERATES_SCHEMAID);
		schemaUUIDString = col.getString();
		return getUUIDFactory().recreateUUID(schemaUUIDString);
	 }

	/**
	 * Get the conglomerate's name of the row.
	 * 
	 * @param row	The row from sysconglomerates
	 *
	 * @return String	The conglomerates name
	 *
	 * @exception   StandardException thrown on failure
	 */
	 protected String getConglomerateName(ExecRow row)
		 throws StandardException
	 {
		DataValueDescriptor	col;

		/* 4th column is CONGLOMERATENAME (varchar(128)) */
		col = row.getColumn(SYSCONGLOMERATES_CONGLOMERATENAME);
		return col.getString();
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
               SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
               SystemColumnImpl.getUUIDColumn("TABLEID", false),
               SystemColumnImpl.getColumn("CONGLOMERATENUMBER", Types.BIGINT, false),
               SystemColumnImpl.getIdentifierColumn("CONGLOMERATENAME", true),
               SystemColumnImpl.getColumn("ISINDEX", Types.BOOLEAN, false),
               SystemColumnImpl.getJavaColumn("DESCRIPTOR",
                       "org.apache.derby.catalog.IndexDescriptor", true),
               SystemColumnImpl.getColumn("ISCONSTRAINT", Types.BOOLEAN, true),
               SystemColumnImpl.getUUIDColumn("CONGLOMERATEID", false)
           };
	}
}
