/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;

/**
 * A DropStatementNode represents a DDL statement that drops something.
 * It contains the name of the object to be dropped.
 *
 * @author Jerry Brenner
 */

public abstract class DropStatementNode extends DDLStatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	//public static final int RESTRICT = StatementType.RESTRICT;
	//public static final int CASCADE = StatementType.CASCADE;
	//public static final int DEFAULT = StatementType.DEFAULT;

}
