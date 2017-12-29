/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSFILESRowFactory

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

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;

import java.sql.Types;
import java.util.Properties;

/**
 * Factory for creating a SYSFILES row.
 *
 *
 * @version 0.1
 */

class SYSFILESRowFactory extends CatalogRowFactory
{
	private static final String	TABLENAME_STRING = "SYSFILES";

    private static final int		SYSFILES_COLUMN_COUNT = 4;

	/* Column #s (1 based) */
    private static final int		ID_COL_NUM = 1;
    private static final String   ID_COL_NAME = "FILEID";

    private static final int		SCHEMA_ID_COL_NUM = 2;
    private static final String   SCHEMA_ID_COL_NAME = "SCHEMAID";

    private static final int		NAME_COL_NUM = 3;
    private static final String   NAME_COL_NAME = "FILENAME";

    private static final int		GENERATION_ID_COL_NUM = 4;
    private static final String   GENERATION_ID_COL_NAME = "GENERATIONID";

    static final int		SYSFILES_INDEX1_ID = 0;
    static final int		SYSFILES_INDEX2_ID = 1;

	private static final int[][] indexColumnPositions =
	{
		{NAME_COL_NUM, SCHEMA_ID_COL_NUM},
		{ID_COL_NUM}
	};

    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		"80000000-00d3-e222-873f-000a0a0b1900",	// catalog UUID
		"80000000-00d3-e222-9920-000a0a0b1900",	// heap UUID
		"80000000-00d3-e222-a373-000a0a0b1900",	// SYSSQLFILES_INDEX1
		"80000000-00d3-e222-be7b-000a0a0b1900"	// SYSSQLFILES_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    SYSFILESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) 
	{
		super(uuidf,ef,dvf);
		initInfo(SYSFILES_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SYSFILES row
	 *
	 * @return	Row suitable for inserting into SYSFILES
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException
	{
		String					id_S = null;
		String					schemaId_S = null;
		String                  SQLname = null;
		long                    generationId = 0;
		
		ExecRow        			row;

		if (td != null)	
		{
			FileInfoDescriptor descriptor = (FileInfoDescriptor)td;
			id_S = descriptor.getUUID().toString();
			schemaId_S = descriptor.getSchemaDescriptor().getUUID().toString();
			SQLname = descriptor.getName();
			generationId = descriptor.getGenerationId();
		}
	
		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSFILES_COLUMN_COUNT);

		/* 1st column is ID (UUID - char(36)) */
		row.setColumn(ID_COL_NUM, new SQLChar(id_S));

		/* 2nd column is SCHEMAID (UUID - char(36)) */
		row.setColumn(SCHEMA_ID_COL_NUM, new SQLChar(schemaId_S));

		/* 3rd column is NAME (varchar(30)) */
		row.setColumn(NAME_COL_NUM, new SQLVarchar(SQLname));

		/* 4th column is GENERATIONID (long) */
		row.setColumn(GENERATION_ID_COL_NUM, new SQLLongint(generationId));

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a descriptor out of a SYSFILES row
	 *
	 * @param row a row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @return	a descriptor equivalent to a row
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
			if (row.nColumns() != SYSFILES_COLUMN_COUNT)
			{
				SanityManager.THROWASSERT("Wrong number of columns for a SYSFILES row: "+
							 row.nColumns());
			}
		}

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		String	id_S;
		UUID    id;
		String	schemaId_S;
		UUID    schemaId;
		String	name;
		long     generationId;
		DataValueDescriptor	col;

		SchemaDescriptor	schemaDescriptor;
		FileInfoDescriptor	result;

		/* 1st column is ID (UUID - char(36)) */
		col = row.getColumn(ID_COL_NUM);
		id_S = col.getString();
		id = getUUIDFactory().recreateUUID(id_S);

		/* 2nd column is SchemaId */
		col = row.getColumn(SCHEMA_ID_COL_NUM);
		schemaId_S = col.getString();
		schemaId = getUUIDFactory().recreateUUID(schemaId_S);
		
		schemaDescriptor = dd.getSchemaDescriptor(schemaId, null);
		if (SanityManager.DEBUG)
		{
			if (schemaDescriptor == null)
			{
				SanityManager.THROWASSERT("Missing schema for FileInfo: "+id_S);
			}
		}

		/* 3nd column is NAME (varchar(128)) */
		col = row.getColumn(NAME_COL_NUM);
		name = col.getString();

		/* 4th column is generationId (long) */
		col = row.getColumn(GENERATION_ID_COL_NUM);
		generationId = col.getLong();

	    result = ddg.newFileInfoDescriptor(id,schemaDescriptor,name,
										   generationId);
		return result;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
    public SystemColumn[]   buildColumnList()
        throws StandardException
    {
        return new SystemColumn[] {
           SystemColumnImpl.getUUIDColumn(ID_COL_NAME, false),
           SystemColumnImpl.getUUIDColumn(SCHEMA_ID_COL_NAME, false),
           SystemColumnImpl.getIdentifierColumn(NAME_COL_NAME, false),
           SystemColumnImpl.getColumn(GENERATION_ID_COL_NAME, Types.BIGINT, false)
                
        };
    }
}
