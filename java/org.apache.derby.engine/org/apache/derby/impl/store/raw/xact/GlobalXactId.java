/*

   Derby - Class org.apache.derby.impl.store.raw.xact.GlobalXactId

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.store.raw.GlobalTransactionId;
import org.apache.derby.iapi.store.access.GlobalXact;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

public class GlobalXactId extends GlobalXact implements GlobalTransactionId
{
    /**************************************************************************
     * Private Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6184
    GlobalXactId(
						int     format_id,
						byte[]  global_id,
						byte[]  branch_id)
    {
		this.format_id = format_id;
        this.global_id = (byte[]) global_id.clone();
        this.branch_id = (byte[]) branch_id.clone();
	}

    /**************************************************************************
     * Public Methods of Formatable interface:
     **************************************************************************
     */

	// no-arg constructor, required by Formatable 
	public GlobalXactId()
    { 
    }

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException 
	{
        out.writeInt(format_id);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(global_id.length <= 64);
            SanityManager.ASSERT(global_id != null);
            SanityManager.ASSERT(branch_id != null);
        }

        // write length of array followed by the array
        out.write(global_id.length);
        if (global_id.length > 0)
            out.write(global_id);

        // write length of array followed by the array
        out.write(branch_id.length);
        if (branch_id.length > 0)
            out.write(branch_id);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
		@exception ClassNotFoundException log stream corrupted
	*/
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
        format_id = in.readInt();

        // read global_id in from disk
        int array_len = in.read();

        if (SanityManager.DEBUG) 
        {
            SanityManager.ASSERT(array_len >= 0);
        }

        global_id = new byte[array_len];
        if (array_len > 0)
            in.read(global_id);

        // read branch_id in from disk
        array_len = in.read();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(array_len >= 0);
        }

        branch_id = new byte[array_len];
        if (array_len > 0)
            in.read(branch_id);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.RAW_STORE_GLOBAL_XACT_ID_NEW;
	}

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Obtain the format id part of the GlobalTransactionId.
     * @return format identifier, 0 means the OSI CCR format
     */
    int getFormat_Id()
    {
        return(format_id);
    }

    /**
     * Obtain the global transaction identifier as an array of bytes.
     * @return a byte array containing the global transaction identifier
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6184
    byte[] getGlobalTransactionId()
    {
        return(global_id);
    }

    /**
     * Obtain the transaction branch qualifier as an array of bytes.
     * @return a byte array containing the branch qualifier of the transaction
     */
    byte[] getBranchQualifier()
    {
        return(branch_id);
    }
}
