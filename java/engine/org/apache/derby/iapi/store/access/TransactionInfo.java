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
	String getGlobalTransactionIdString();
	String getTransactionIdString();
	String getUsernameString();
	String getTransactionTypeString();
	String getTransactionStatusString();
	String getFirstLogInstantString();
	String getStatementTextString();

}
