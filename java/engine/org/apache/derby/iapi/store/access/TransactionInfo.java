/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

public interface TransactionInfo
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	String getGlobalTransactionIdString();
	String getTransactionIdString();
	String getUsernameString();
	String getTransactionTypeString();
	String getTransactionStatusString();
	String getFirstLogInstantString();
	String getStatementTextString();

}
