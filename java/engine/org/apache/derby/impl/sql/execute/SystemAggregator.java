/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Abstract aggregator that is extended by all internal
 * (system) aggregators.
 *
 * @author jamie
 */
abstract class SystemAggregator implements ExecAggregator
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

    private boolean eliminatedNulls;


	public boolean didEliminateNulls() {
		return eliminatedNulls;
	}

	public void accumulate(DataValueDescriptor addend, Object ga) 
		throws StandardException
	{
		if ((addend == null) || addend.isNull()) {
			eliminatedNulls = true;
			return;
		}

		this.accumulate(addend);
	}

	protected abstract void accumulate(DataValueDescriptor addend)
		throws StandardException;
	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////

	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeBoolean(eliminatedNulls);
	}

	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		eliminatedNulls = in.readBoolean();
	}
}
