/*

   Derby - Class org.apache.derby.impl.sql.execute.SystemAggregator

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.shared.common.sanity.SanityManager;
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
 */
abstract class SystemAggregator implements ExecAggregator
{

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
        public String toString()
        {
            try
            {
            return super.toString() + "[" + getResult().getString() + "]";
            }
            catch (Exception e)
            {
                return e.getMessage();
            }
        }
}
