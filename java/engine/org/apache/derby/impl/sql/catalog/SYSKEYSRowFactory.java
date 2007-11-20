/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSKEYSRowFactory

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.SQLChar;

/**
 * Factory for creating a SYSKEYS row.
 *
 */

public class SYSKEYSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSKEYS";

	protected static final int		SYSKEYS_COLUMN_COUNT = 2;
	protected static final int		SYSKEYS_CONSTRAINTID = 1;
	protected static final int		SYSKEYS_CONGLOMERATEID = 2;

	protected static final int		SYSKEYS_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = null;

	private static final int[][] indexColumnPositions =
	{
		{SYSKEYS_CONSTRAINTID}
	};

	private	static	final	String[]	uuids =
	{
		 "80000039-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"8000003c-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000003b-00d0-fd77-3ed8-000a0a0b1900"	// SYSKEYS_INDEX1
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    SYSKEYSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSKEYS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSKEYS row
	 *
	 * @return	Row suitable for inserting into SYSKEYS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		UUID						oid;
		String					constraintID = null;
		String					conglomerateID = null;

		if (td != null)
		{
			KeyConstraintDescriptor	constraint = (KeyConstraintDescriptor)td;

			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			oid = constraint.getUUID();
			constraintID = oid.toString();

			conglomerateID = constraint.getIndexUUIDString();
		}

		/* Insert info into syskeys */

		/* RESOLVE - It would be nice to require less knowledge about syskeys
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSKEYS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSKEYS_CONSTRAINTID, new SQLChar(constraintID));
		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		row.setColumn(SYSKEYS_CONGLOMERATEID, new SQLChar(conglomerateID));

		return row;
	}



	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SubConstraintDescriptor out of a SYSKEYS row
	 *
	 * @param row a SYSKEYS row
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
		SubKeyConstraintDescriptor keyDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSKEYS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSKEYS row");
		}

		DataValueDescriptor		col;
		DataDescriptorGenerator ddg;
		UUID					constraintUUID;
		UUID					conglomerateUUID;
		String				constraintUUIDString;
		String				conglomerateUUIDString;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSKEYS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(SYSKEYS_CONGLOMERATEID);
		conglomerateUUIDString = col.getString();
		conglomerateUUID = getUUIDFactory().recreateUUID(conglomerateUUIDString);

		/* now build and return the descriptor */

		keyDesc =  new SubKeyConstraintDescriptor(
										constraintUUID,
										conglomerateUUID);
		return keyDesc;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
	{
            return new SystemColumn[] {
                SystemColumnImpl.getUUIDColumn("CONSTRAINTID", false),
                SystemColumnImpl.getUUIDColumn("CONGLOMERATEID", false),
            };
        }

}
