/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
