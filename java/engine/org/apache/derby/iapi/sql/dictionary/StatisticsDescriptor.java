/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;
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




