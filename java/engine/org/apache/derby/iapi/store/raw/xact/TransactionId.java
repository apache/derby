/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw.xact
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw.xact;

import org.apache.derby.iapi.services.io.Formatable;

/**
	A transaction identifier that is only unique within a raw store, do not
	ever pass this out of raw store.  During reboot, all transaction Ids that
	have ever generated a log record will not be reused.  

	However, if you put away the transaction Id of a read only transaction,
	then the is no guarentee that the transactionId won't be reused when the
	system reboots.  It is much safer to store away the ExternalTrasanctionId
	rather than the transactionId.

	The equals() method for TransactionId implements by value equality.

	MT - immutable

*/
public interface TransactionId extends Formatable {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/** 
		Return the maximum number of bytes the transactionId will take
		to store using writeExternal.
	*/
	int getMaxStoredSize();

	/* need to write a value based HashCode() method. */
}
