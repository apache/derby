/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute; 

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.io.FormatableBitSet;

public interface KeyToBaseRowConstantAction
extends ConstantAction
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	public ExecRow getEmptyHeapRow(LanguageConnectionContext lcc)
		 throws StandardException;
	public ExecRow getEmptyKeyRow()
		 throws StandardException;
	public long getKeyConglomId()
		 throws StandardException;
	public long getBaseTableConglomId()
		 throws StandardException;
	public UUID getTableId() 
		 throws StandardException;
	public FormatableBitSet getBaseRowReadList()
		 throws StandardException;
}
