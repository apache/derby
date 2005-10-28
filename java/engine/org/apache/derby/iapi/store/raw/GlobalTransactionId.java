/*

   Derby - Class org.apache.derby.iapi.store.raw.GlobalTransactionId

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.io.Formatable;

/**
	A transaction identifier that is unique among all raw stores and all
	transactions

	The equals() method for TransactionId implements by value equality.

	MT - immutable
*/
public interface GlobalTransactionId extends Formatable 
{
    /**
     * Obtain the format id part of the GlobalTransactionId.
     * <p>
     *
	 * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormat_Id();

    /**
     * Obtain the global transaction identifier part of GlobalTransactionId 
     * as an array of bytes.
     * <p>
     *
	 * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId();

    /**
     * Obtain the transaction branch qualifier part of the GlobalTransactionId
     * in a byte array.
     * <p>
     *
	 * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier();

	/* need to write a value based HashCode() method. */
}
