/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql;

/**
 * Different types of statements
 *
 * @author jamie
 */
public interface StatementType
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	public static final int UNKNOWN	= 0;
	public static final int INSERT	= 1;
	public static final int BULK_INSERT_REPLACE = 2;
	public static final int UPDATE	= 3;
	public static final int DELETE	= 4;
	public static final int ENABLED = 5;
	public static final int DISABLED = 6;

	public static final int DROP_CASCADE = 0;
	public static final int DROP_RESTRICT = 1;
	public static final int DROP_DEFAULT = 2;

	public static final int RENAME_TABLE = 1;
	public static final int RENAME_COLUMN = 2;
	public static final int RENAME_INDEX = 3;

	public static final int RA_CASCADE = 0;
	public static final int RA_RESTRICT = 1;
	public static final int RA_NOACTION = 2;  //default value
	public static final int RA_SETNULL = 3;
	public static final int RA_SETDEFAULT = 4;
	
	public static final int SET_SCHEMA_USER = 1;
	public static final int SET_SCHEMA_DYNAMIC = 2;
	
}





