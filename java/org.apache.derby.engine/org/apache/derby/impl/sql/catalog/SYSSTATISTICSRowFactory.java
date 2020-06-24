/*

   Derby - Class org.apache.derby.impl.sql.catalog.SYSSTATISTICSRowFactory

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

import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Statistics;
import org.apache.derby.iapi.types.*;

import java.sql.Timestamp;
import java.sql.Types;

/**
 * Factory for creating a SYSSTATISTICS row.
 *
 * @version 0.1
 *
 */

public class SYSSTATISTICSRowFactory extends CatalogRowFactory
{
	static final String TABLENAME_STRING = "SYSSTATISTICS";

	/* column #s for sysstatistics (1 based) */
	
	/* unique UUID of this entry in statistics. 
	*/
	protected static final int 	SYSSTATISTICS_ID = 1;

	/* reference id from sysconglomerates... */
	protected static final int  SYSSTATISTICS_REFERENCEID = 2;

	/* table id--table for which this statistic is created */
	protected static final int SYSSTATISTICS_TABLEID = 3;
	
	/* time when this statistic was created/updated */
	protected static final int SYSSTATISTICS_TIMESTAMP = 4;

	/* type of statistics-- we only have index (I) statistics right now but
	 * later on we might have table or column statistics.
	 */
	protected static final int SYSSTATISTICS_TYPE = 5;	

  	/* whether the statistics are valid or not; currently this is not used, but
	 * in the future the optimizer might be smart enough to recognize that a
	 * statistic has gone stale and then mark it as invalid (as opposed to
	 * dropping it which is a more drastic measure?)
	 */
  	protected static final int SYSSTATISTICS_VALID = 6;

	/* the  number of columns in this statistics */
	protected static final int SYSSTATISTICS_COLCOUNT = 7;

	/* and finally the statistics */
	protected static final int SYSSTATISTICS_STAT = 8;

	protected static final int SYSSTATISTICS_COLUMN_COUNT = 8;

	/* first index on tableUUID, conglomerate UUID */
	protected static final int SYSSTATISTICS_INDEX1_ID = 0;

	private static final boolean[] uniqueness = {false};

	private static final int[][] indexColumnPositions =
	{
		{SYSSTATISTICS_TABLEID, SYSSTATISTICS_REFERENCEID}
	};

	private static final String[] uuids =
	{
		"f81e0010-00e3-6612-5a96-009e3a3b5e00", // catalog UUID
		"08264012-00e3-6612-5a96-009e3a3b5e00",  // heap UUID.
		"c013800d-00e3-ffbe-37c6-009e3a3b5e00", // _INDEX1 UUID
	};
	/*
	 * STATE
	 */

	/*
	 *	CONSTRUCTORS
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-3147
    SYSSTATISTICSRowFactory(UUIDFactory uuidf, 
									ExecutionFactory ef, 
									DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		
		initInfo(SYSSTATISTICS_COLUMN_COUNT, TABLENAME_STRING, 
//IC see: https://issues.apache.org/jira/browse/DERBY-1739
				 indexColumnPositions, uniqueness, uuids);
	}


  /**
	 * Make a SYSSTATISTICS row
	 *
	 *
	 * @return	Row suitable for inserting into SYSSTATISTICS.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException					
	{
		String myID = null, referenceID = null, tableID = null;
		String statName = null, colMap = null, statType = null;
		Timestamp updateTime = null;
		int columnCount = 0;
		Statistics statisticsObject = null;
		boolean validStat = false;
		ExecRow row = getExecutionFactory().getValueRow(SYSSTATISTICS_COLUMN_COUNT);
		
		if (td != null)
		{
			StatisticsDescriptor statDesc = (StatisticsDescriptor)td;
			myID = statDesc.getUUID().toString();
			tableID = statDesc.getTableUUID().toString();
			referenceID = statDesc.getReferenceID().toString();
			updateTime = statDesc.getUpdateTimestamp();
			statType = statDesc.getStatType();
  			validStat = statDesc.isValid();
			statisticsObject = statDesc.getStatistic();
			columnCount = statDesc.getColumnCount();
		}

		row.setColumn(1, new SQLChar(myID));
		row.setColumn(2, new SQLChar(referenceID));
		row.setColumn(3, new SQLChar(tableID));
		row.setColumn(4, new SQLTimestamp(updateTime));
		row.setColumn(5, new SQLChar(statType));
//IC see: https://issues.apache.org/jira/browse/DERBY-4062
  		row.setColumn(6, new SQLBoolean(validStat));
		row.setColumn(7, new SQLInteger(columnCount));
		row.setColumn(8, new UserType(statisticsObject));
		return row;
	}
	
	public TupleDescriptor buildDescriptor(
		 ExecRow 			row,
		 TupleDescriptor    parentDesc,
		 DataDictionary 	dd)
		throws StandardException
		 
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSSTATISTICS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSSTATISTICS row");
		}

		DataValueDescriptor col;
		String scratch;
		UUIDFactory uuidFactory = getUUIDFactory();
		UUID statUUID, statReferenceUUID, statTableUUID;
		String statName;
		
		/* 1st column is UUID */
		col = row.getColumn(SYSSTATISTICS_ID);
		scratch = col.getString();
		statUUID = uuidFactory.recreateUUID(scratch);

		/* 2nd column is reference UUID */
		col = row.getColumn(SYSSTATISTICS_REFERENCEID);
		scratch = col.getString();
		statReferenceUUID = uuidFactory.recreateUUID(scratch);

		/* 3rd column is table UUID */
		col = row.getColumn(SYSSTATISTICS_TABLEID);
		scratch = col.getString();
		statTableUUID = uuidFactory.recreateUUID(scratch);

		/* 4th column is timestamp */
		col = row.getColumn(SYSSTATISTICS_TIMESTAMP);
		Timestamp updateTime = (Timestamp) col.getObject();

		/* 5th column is stat type -- string */
		col = row.getColumn(SYSSTATISTICS_TYPE);
		String statType = col.getString();

		/* 6th column is stat valid -- boolean */
		col = row.getColumn(SYSSTATISTICS_VALID);
		boolean valid = col.getBoolean();

		/* 7th column is column count */
		col = row.getColumn(SYSSTATISTICS_COLCOUNT);
		int columnCount = col.getInt();

		/* 8th column is statistics itself */
		col = row.getColumn(SYSSTATISTICS_STAT);
		Statistics stat = (Statistics)col.getObject();

		return new StatisticsDescriptor(dd, statUUID, statReferenceUUID,
										   statTableUUID, // statName, colMap,
										   statType, stat, columnCount);
	}			

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[] buildColumnList()
//IC see: https://issues.apache.org/jira/browse/DERBY-4484
        throws StandardException
	{
            return new SystemColumn[] {
                SystemColumnImpl.getUUIDColumn("STATID", false),
                SystemColumnImpl.getUUIDColumn("REFERENCEID", false),
                SystemColumnImpl.getUUIDColumn("TABLEID", false),
                SystemColumnImpl.getColumn("CREATIONTIMESTAMP", Types.TIMESTAMP, false),
                SystemColumnImpl.getIndicatorColumn("TYPE"),
                SystemColumnImpl.getColumn("VALID", Types.BOOLEAN, false),
                SystemColumnImpl.getColumn("COLCOUNT", Types.INTEGER, false),
                SystemColumnImpl.getJavaColumn("STATISTICS",
                        "org.apache.derby.catalog.Statistics", false)
            };
        }
}	
