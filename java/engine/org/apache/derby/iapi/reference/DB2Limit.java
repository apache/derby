/*

   Derby - Class org.apache.derby.iapi.reference.DB2Limit

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.reference;

public interface DB2Limit
{
	/**
     * Various fixed numbers related to DB2 limits.
     */

	public static final int DB2_MAX_TRIGGER_RECURSION = 16; /* Maximum nesting level for triggers */

	/** Maximum number of indexes on a table */
	public static final int DB2_MAX_INDEXES_ON_TABLE = 32767;
	/* Maximum number of columns in a table */
	public static final int DB2_MAX_COLUMNS_IN_TABLE = 1012;

	/* Maximum number of columns in a view */
	public static final int DB2_MAX_COLUMNS_IN_VIEW = 5000;

	/* Maximum number of parameters in a stored procedure */
	public static final int DB2_MAX_PARAMS_IN_STORED_PROCEDURE = 90;

	/* Maximum number of elements in a select list */
	public static final int DB2_MAX_ELEMENTS_IN_SELECT_LIST = 1012;
	/* Maximum number of columns in a group by list */
	public static final int DB2_MAX_ELEMENTS_IN_GROUP_BY = 32677;
	/* Maximum number of columns in an order by list */
	public static final int DB2_MAX_ELEMENTS_IN_ORDER_BY = 1012;

	// Max length for an exception parameter string over CCC server.
	public static final int DB2_CCC_MAX_EXCEPTION_PARAM_LENGTH = 70;

	// Warning. Changing this value will affect upgrade and the creation of the
	// SQLCAMESSAGE procedure. See org.apache.derby.impl.sql.catalog.
	public static final int DB2_JCC_MAX_EXCEPTION_PARAM_LENGTH = 2400;

	/* Some identifiers like Constraint name, Cursor name, Function name, Index name, Trigger name
	* are limited to 18 character in DB2*/
	public static final int DB2_MAX_IDENTIFIER_LENGTH18 = 18;
	/* Some identifiers like Column name, Schema name are limited to 30 characters in DB2*/
	public static final int DB2_MAX_IDENTIFIER_LENGTH30 = 30;
	/* Some identifiers like Savepoint names, Table names, view names etc are limited to 128 characters in DB2*/
	public static final int DB2_MAX_IDENTIFIER_LENGTH128 = 128;
	public static final int	DB2_CHAR_MAXWIDTH = 254;
	public static final int	DB2_VARCHAR_MAXWIDTH = 32672;
	public static final int DB2_LOB_MAXWIDTH = 2147483647;
	public static final int	DB2_LONGVARCHAR_MAXWIDTH = 32700;
    public static final int DB2_CONCAT_VARCHAR_LENGTH = 4000;
	public static final int DB2_MAX_FLOATINGPOINT_LITERAL_LENGTH = 30; // note, this value 30 is also contained in err msg 42820
	public static final int DB2_MAX_CHARACTER_LITERAL_LENGTH = 32672;
	public static final int DB2_MAX_HEX_LITERAL_LENGTH = 16336;

	public static final int MIN_COL_LENGTH_FOR_CURRENT_USER = 8;
	public static final int MIN_COL_LENGTH_FOR_CURRENT_SCHEMA = 128;     
	public static final int MAX_USERID_LENGTH = 30;

    /**
     * DB2 TABLESPACE page size limits
     */
    public static final int DB2_MIN_PAGE_SIZE = 4096;   //  4k
    public static final int DB2_MAX_PAGE_SIZE = 32768;  // 32k

    /**
     * DECIMAL type limits
     */

	public static final int MAX_DECIMAL_PRECISION_SCALE = 31;
	public static final int DEFAULT_DECIMAL_PRECISION   = 5;
	public static final int DEFAULT_DECIMAL_SCALE       = 0;

    /**
     * REAL/DOUBLE range limits
     */

    static final float DB2_SMALLEST_REAL = -3.402E+38f;
    static final float DB2_LARGEST_REAL  = +3.402E+38f;
    static final float DB2_SMALLEST_POSITIVE_REAL = +1.175E-37f;
    static final float DB2_LARGEST_NEGATIVE_REAL  = -1.175E-37f;

    static final double DB2_SMALLEST_DOUBLE = -1.79769E+308d;
    static final double DB2_LARGEST_DOUBLE  = +1.79769E+308d;
    static final double DB2_SMALLEST_POSITIVE_DOUBLE = +2.225E-307d;
    static final double DB2_LARGEST_NEGATIVE_DOUBLE  = -2.225E-307d;

    
}
