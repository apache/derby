/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSDEPENDSRowFactory

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

import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.UserType;

/**
 * Factory for creating a SYSDEPENDSS row.
 *
 */

public class SYSDEPENDSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSDEPENDS";

	protected static final int		SYSDEPENDS_COLUMN_COUNT = 4;
	protected static final int		SYSDEPENDS_DEPENDENTID = 1;
	protected static final int		SYSDEPENDS_DEPENDENTTYPE = 2;
	protected static final int		SYSDEPENDS_PROVIDERID = 3;
	protected static final int		SYSDEPENDS_PROVIDERTYPE = 4;

	protected static final int		SYSDEPENDS_INDEX1_ID = 0;
	protected static final int		SYSDEPENDS_INDEX2_ID = 1;

	
    private	static	final	boolean[]	uniqueness = {
		                                               false,
													   false
	                                                 };

	private static final int[][] indexColumnPositions =
	{
		{SYSDEPENDS_DEPENDENTID},
		{SYSDEPENDS_PROVIDERID}
	};

	private	static	final	String[]	uuids =
	{
		 "8000003e-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000043-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000040-00d0-fd77-3ed8-000a0a0b1900"	// SYSDEPENDS_INDEX1
		,"80000042-00d0-fd77-3ed8-000a0a0b1900"	// SYSDEPENDS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

//IC see: https://issues.apache.org/jira/browse/DERBY-3147
    SYSDEPENDSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSDEPENDS_COLUMN_COUNT,TABLENAME_STRING, indexColumnPositions,
//IC see: https://issues.apache.org/jira/browse/DERBY-1739
				 uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSDEPENDS row
	 *
	 * @param td DependencyDescriptor. If its null then we want to make an empty
	 * row. 
	 *
	 * @return	Row suitable for inserting into SYSDEPENDS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		String					dependentID = null;
		DependableFinder		dependentBloodhound = null;
		String					providerID = null;
		DependableFinder		providerBloodhound = null;

		if (td != null)
		{
			DependencyDescriptor dd = (DependencyDescriptor)td;
			dependentID	= dd.getUUID().toString();
			dependentBloodhound = dd.getDependentFinder();
			if ( dependentBloodhound == null )
			{
				throw StandardException.newException(SQLState.DEP_UNABLE_TO_STORE);
			}

			providerID	= dd.getProviderID().toString();
			providerBloodhound = dd.getProviderFinder();
			if ( providerBloodhound == null )
			{
				throw StandardException.newException(SQLState.DEP_UNABLE_TO_STORE);
			}

		}

		/* Insert info into sysdepends */

		/* RESOLVE - It would be nice to require less knowledge about sysdepends
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSDEPENDS_COLUMN_COUNT);

		/* 1st column is DEPENDENTID (UUID - char(36)) */
		row.setColumn(SYSDEPENDS_DEPENDENTID, new SQLChar(dependentID));

		/* 2nd column is DEPENDENTFINDER */
		row.setColumn(SYSDEPENDS_DEPENDENTTYPE,
				new UserType(dependentBloodhound));
//IC see: https://issues.apache.org/jira/browse/DERBY-4062

		/* 3rd column is PROVIDERID (UUID - char(36)) */
		row.setColumn(SYSDEPENDS_PROVIDERID, new SQLChar(providerID));

		/* 4th column is PROVIDERFINDER */
		row.setColumn(SYSDEPENDS_PROVIDERTYPE,
				new UserType(providerBloodhound));

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ConstraintDescriptor out of a SYSDEPENDS row
	 *
	 * @param row a SYSDEPENDSS row
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
		DependencyDescriptor dependencyDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSDEPENDS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSDEPENDS row");
		}

		DataValueDescriptor	col;
		String				dependentIDstring;
		UUID				dependentUUID;
		DependableFinder	dependentBloodhound;
		String				providerIDstring;
		UUID				providerUUID;
		DependableFinder	providerBloodhound;

		/* 1st column is DEPENDENTID (UUID - char(36)) */
		col = row.getColumn(SYSDEPENDS_DEPENDENTID);
		dependentIDstring = col.getString();
		dependentUUID = getUUIDFactory().recreateUUID(dependentIDstring);

		/* 2nd column is DEPENDENTTYPE */
		col = row.getColumn(SYSDEPENDS_DEPENDENTTYPE);
		dependentBloodhound = (DependableFinder) col.getObject();

		/* 3rd column is PROVIDERID (UUID - char(36)) */
		col = row.getColumn(SYSDEPENDS_PROVIDERID);
		providerIDstring = col.getString();
		providerUUID = getUUIDFactory().recreateUUID(providerIDstring);

		/* 4th column is PROVIDERTYPE */
		col = row.getColumn(SYSDEPENDS_PROVIDERTYPE);
		providerBloodhound = (DependableFinder) col.getObject();

		/* now build and return the descriptor */
		return new DependencyDescriptor(dependentUUID, dependentBloodhound,
										   providerUUID, providerBloodhound);
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
                SystemColumnImpl.getUUIDColumn("DEPENDENTID", false),
                SystemColumnImpl.getJavaColumn("DEPENDENTFINDER",
                        "org.apache.derby.catalog.DependableFinder", false),
                SystemColumnImpl.getUUIDColumn("PROVIDERID", false),
                SystemColumnImpl.getJavaColumn("PROVIDERFINDER",
                        "org.apache.derby.catalog.DependableFinder", false),
           };
	}
}
