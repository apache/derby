/*

   Derby - Class org.apache.derby.impl.store.raw.data.CompressSpacePageOperation10_2

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * This class overrides the CompressSpacePageOperation class to write
 * CompressSpaceOperation Log Records that do not support negative values
 * for new_highest_page. No other changes are added to the superclass behavior.
 * This class ensures backward compatibility for Soft upgrades.
 */
public final class CompressSpacePageOperation10_2 extends CompressSpacePageOperation {
    
	/**************************************************************************
	* Constructors for This class:
	**************************************************************************
	*/
    CompressSpacePageOperation10_2(
		AllocPage   allocPage, 
		int         highest_page, 
		int         num_truncated)
			throws StandardException
	{
		super(allocPage, highest_page, num_truncated);
	}

	// no-arg constructor, required by Formatable 
	public CompressSpacePageOperation10_2() { super(); }

	/**************************************************************************
	* Public Methods of Formatable interface.
	**************************************************************************
	*/

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeInt(out, newHighestPage);
		CompressedNumber.writeInt(out, num_pages_truncated);
	}

	/**
		@exception IOException error reading from log stream
		@exception ClassNotFoundException cannot read object from input
	*/
	public void readExternal(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		newHighestPage      = CompressedNumber.readInt(in);
		num_pages_truncated = CompressedNumber.readInt(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_COMPRESS10_2_SPACE;
	}

}
