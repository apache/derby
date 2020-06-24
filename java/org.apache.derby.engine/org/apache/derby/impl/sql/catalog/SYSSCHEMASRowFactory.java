/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSSCHEMASRowFactory

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

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Factory for creating a SYSSCHEMAS row.
 *
 *
 * @version 0.1
 */

public class SYSSCHEMASRowFactory extends CatalogRowFactory
{
	private	static	final	String	TABLENAME_STRING = "SYSSCHEMAS";

	public	static	final	int		SYSSCHEMAS_COLUMN_COUNT = 3;
	/* Column #s for sysinfo (1 based) */
	public	static	final	int		SYSSCHEMAS_SCHEMAID = 1;
	public	static	final	int		SYSSCHEMAS_SCHEMANAME = 2;
	public	static	final	int		SYSSCHEMAS_SCHEMAAID = 3;

	protected static final int		SYSSCHEMAS_INDEX1_ID = 0;
	protected static final int		SYSSCHEMAS_INDEX2_ID = 1;


	private static final int[][] indexColumnPositions =
	{
		{SYSSCHEMAS_SCHEMANAME},
		{SYSSCHEMAS_SCHEMAID}
	};
	
    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		 "80000022-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"8000002a-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000024-00d0-fd77-3ed8-000a0a0b1900"	// SYSSCHEMAS_INDEX1
		,"80000026-00d0-fd77-3ed8-000a0a0b1900"	// SYSSCHEMAS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

//IC see: https://issues.apache.org/jira/browse/DERBY-3147
    SYSSCHEMASRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSSCHEMAS_COLUMN_COUNT, TABLENAME_STRING, 
//IC see: https://issues.apache.org/jira/browse/DERBY-1739
				 indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSSCHEMAS row
	 *
	 * @return	Row suitable for inserting into SYSSCHEMAS.
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
		UUID						oid = null;
		String					uuid = null;	
		String					aid = null;

		if (td != null)
		{
			SchemaDescriptor	schemaDescriptor = (SchemaDescriptor)td;

			name = schemaDescriptor.getSchemaName();
			oid = schemaDescriptor.getUUID();
			if ( oid == null )
		    {
				oid = getUUIDFactory().createUUID();
				schemaDescriptor.setUUID(oid);
			}
			uuid = oid.toString();

			aid = schemaDescriptor.getAuthorizationId();
		}

		/* Build the row to insert */
		row = getExecutionFactory().getValueRow(SYSSCHEMAS_COLUMN_COUNT);

		/* 1st column is SCHEMAID */
		row.setColumn(1, new SQLChar(uuid));

		/* 2nd column is SCHEMANAME */
		row.setColumn(2, new SQLVarchar(name));

		/* 3rd column is SCHEMAAID */
		row.setColumn(3, new SQLVarchar(aid));

		return row;
	}


	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make an  Tuple Descriptor out of a SYSSCHEMAS row
	 *
	 * @param row 					a SYSSCHEMAS row
	 * @param parentTupleDescriptor	unused
	 * @param dd 					dataDictionary
	 *
	 * @return	a  descriptor equivalent to a SYSSCHEMAS row
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
		SchemaDescriptor			descriptor;
		String						name;
		UUID							id;
		String						aid;
		String						uuid;
		DataDescriptorGenerator		ddg = dd.getDataDescriptorGenerator();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row.nColumns() == SYSSCHEMAS_COLUMN_COUNT, 
								 "Wrong number of columns for a SYSSCHEMAS row");
		}

		// first column is schemaid (UUID - char(36))
		col = row.getColumn(1);
		uuid = col.getString();
		id = getUUIDFactory().recreateUUID(uuid);

		// second column is schemaname (varchar(128))
		col = row.getColumn(2);
		name = col.getString();

		// third column is auid (varchar(128))
		col = row.getColumn(3);
		aid = col.getString();

		descriptor = ddg.newSchemaDescriptor(name, aid, id);

		return descriptor;
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
                SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
                SystemColumnImpl.getIdentifierColumn("SCHEMANAME", false),
                SystemColumnImpl.getIdentifierColumn("AUTHORIZATIONID", false),
            };
	}
}
