/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Aggregator for MAX()/MIN().  Defers most of its work
 * to OrderableAggregator.
 *
 * @see OrderableAggregator
 *
 * @author jamie
 */
public final class MaxMinAggregator 
	extends OrderableAggregator
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;

	private boolean isMax; // true for max, false for min

	/**
	 */
	public void setup(String aggregateName)
	{
		super.setup(aggregateName);
		isMax = aggregateName.equals("MAX");
	}
	/**
	 * Accumulate
 	 *
	 * @param addend	value to be added in
	 *
	 * @exception StandardException on error
	 *
	 */
	protected void accumulate(DataValueDescriptor addend) 
		throws StandardException
	{
		if ( (value == null) ||
			      (isMax && (value.compare(addend) < 0)) ||
				  (!isMax && (value.compare(addend) > 0))
				  )
		{
			/* NOTE: We need to call getClone() since value gets
			 * reused underneath us
			 */
			value = addend.getClone();
		}
	}

	/**
	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator()
	{
		MaxMinAggregator ma = new MaxMinAggregator();
		ma.isMax = isMax;
		return ma;
	}

	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeBoolean(isMax);
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 * @exception ClassNotFoundException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException {
		super.readExternal(in);
		isMax = in.readBoolean();
	}
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_MAX_MIN_V01_ID; }
}
