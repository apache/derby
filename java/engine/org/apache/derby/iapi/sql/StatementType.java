/*

   Derby - Class org.apache.derby.iapi.sql.StatementType

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

package org.apache.derby.iapi.sql;

/**
 * Different types of statements
 *
 * @author jamie
 */
public interface StatementType
{
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





