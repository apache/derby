/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.Statistics;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import java.sql.Timestamp;

/**
 * Implementation of StatisticsDescriptor.
 *
 */
public class StatisticsDescriptor extends TupleDescriptor
{
	private UUID statID; 		// my UUID 
	private UUID statRefID;  	// UUID of object for which I'm a statistic 
	private UUID statTableID;  	// UUID of table for which I'm a stat 
	private Timestamp statUpdateTime; 	// when was I last modified 

	/* I for Index, T for table and such; even though right now all 
	   our statistics are 'I' but who knows what we'll need later.
	*/
	private String statType;  							
	private boolean statValid = true;	// am I valid? 
	private Statistics statStat; // the real enchilada.
	private int statColumnCount; // for how many columns??
	
	public StatisticsDescriptor(DataDictionary dd,
							 UUID newUUID,
							 UUID objectUUID,
							 UUID tableUUID,
							 String type,
							 Statistics stat,
							 int colCount)
	{
		super (dd);
		this.statID = newUUID;
		this.statRefID = objectUUID;
		this.statTableID = tableUUID;
		this.statUpdateTime = new Timestamp(System.currentTimeMillis());	
		this.statType = "I";	// for now only index.
		this.statStat = stat;
		this.statColumnCount = colCount;
	}

	public UUID getUUID()
	{
		return statID;
	}
	
	/*----- getter functions for rowfactory ------*/
	public UUID getTableUUID() { return statTableID;}
	public UUID getReferenceID() { return statRefID; }
	public Timestamp getUpdateTimestamp() { return statUpdateTime; }
	public String getStatType() { return statType; }
	public boolean isValid() { return statValid; }
	public Statistics getStatistic() { return statStat; }
	public int getColumnCount() { return statColumnCount; }

	public String toString()
	{
		return "statistics: table=" + getTableUUID().toString() + 
			",conglomerate=" + getReferenceID() +
			",colCount=" + getColumnCount() +
			",stat=" + getStatistic();
	}		
}	




