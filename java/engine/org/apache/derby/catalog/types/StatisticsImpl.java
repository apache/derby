/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog.types
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog.types;

import org.apache.derby.catalog.Statistics;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableLongHolder;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

public class StatisticsImpl	implements Statistics, Formatable
{
	/* total count of rows for which this statistic was created-- this
	   is not the same as the total number of rows in the conglomerate
	   currently, but a snapshot; i.e the  number of rows when this
	   statistic was created/updated.
	*/

	private long numRows;
	
	/* total count of unique values for the keys 
	 */
	private long numUnique;

	/**
	 * Constructor for StatisticsImpl.
	 * 
	 * @param numRows	number of rows in the conglomerate for which
	 * this statistic is being created.
	 * @param numUnique number of unique values in the key for which
	 * this statistic is being created.
	 */
	public StatisticsImpl(long numRows, long numUnique)
	{
		this.numRows = numRows;
		this.numUnique = numUnique;
	}

	/** Zero argument constructor for Formatable Interface */
	public StatisticsImpl()
	{}

	/** @see Statistics#selectivity */
	public double selectivity(Object[] predicates)
	{
		if (numRows == 0.0)
			return 0.1;

		/* xxxSTATresolve: for small values of numRows, should we do something
		 * special? 
		 */
		return (double)(1/(double)numUnique);
	}

	/*------------------ Externalizable Interface ------------------*/
	
	/**
	 * @see java.io.Externalizable#readExternal
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		FormatableHashtable fh = (FormatableHashtable)in.readObject();
		numRows = fh.getLong("numRows");
		numUnique = fh.getLong("numUnique");
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal(ObjectOutput out)
		 throws IOException
	{
		FormatableHashtable fh = new FormatableHashtable();
		fh.putLong("numRows", numRows);
		fh.putLong("numUnique", numUnique);
		out.writeObject(fh);
	}
		
	/*------------------- Formatable Interface ------------------*/
	/**
	 * @return the format id which corresponds to this class.
	 */
	public int getTypeFormatId()
	{
		return StoredFormatIds.STATISTICS_IMPL_V01_ID;
	}

	
	/** @see java.lang.Object#toString */
	public String toString()
	{
		return "numunique= " + numUnique + " numrows= " + numRows;
	}
	
}
