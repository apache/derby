/*

   Derby - Class org.apache.derby.shared.common.reference.Limits

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.shared.common.reference;

public interface Limits
{
	/**
        * Various fixed Limits. DB2 related limits are prefixed with "DB2_".
        */

	public static final int DB2_MAX_TRIGGER_RECURSION = 16; /* Maximum nesting level for triggers */

	/** Maximum number of indexes on a table */
	public static final int DB2_MAX_INDEXES_ON_TABLE = 32767;
	/* Maximum number of columns in a table */
	public static final int DB2_MAX_COLUMNS_IN_TABLE = 1012;

	/* Maximum number of columns in a view */
	public static final int DB2_MAX_COLUMNS_IN_VIEW = 5000;

	/* Maximum number of elements in a select list */
	public static final int DB2_MAX_ELEMENTS_IN_SELECT_LIST = 1012;
	/* Maximum number of columns in a group by list */
	public static final int DB2_MAX_ELEMENTS_IN_GROUP_BY = 32677;
	/* Maximum number of columns in an order by list */
	public static final int DB2_MAX_ELEMENTS_IN_ORDER_BY = 1012;


        /* Identifiers (Constraint, Cursor, Function/Procedure, Index,
         * Trigger, Column, Schema, Savepoint, Table and View names)
         * are limited to 128 */ 
        public static final int MAX_IDENTIFIER_LENGTH = 128;

	public static final int	DB2_CHAR_MAXWIDTH = 254;
	public static final int	DB2_VARCHAR_MAXWIDTH = 32672;
	public static final int DB2_LOB_MAXWIDTH = 2147483647;
	public static final int	DB2_LONGVARCHAR_MAXWIDTH = 32700;
    public static final int DB2_CONCAT_VARCHAR_LENGTH = 4000;
	public static final int DB2_MAX_FLOATINGPOINT_LITERAL_LENGTH = 30; // note, this value 30 is also contained in err msg 42820
	public static final int DB2_MAX_CHARACTER_LITERAL_LENGTH = 32672;
	public static final int DB2_MAX_HEX_LITERAL_LENGTH = 16336;

	public static final int DB2_MIN_COL_LENGTH_FOR_CURRENT_USER = 8;
	public static final int DB2_MIN_COL_LENGTH_FOR_CURRENT_SCHEMA = 128;     

    /**
     * DB2 TABLESPACE page size limits
     */
    public static final int DB2_MIN_PAGE_SIZE = 4096;   //  4k
    public static final int DB2_MAX_PAGE_SIZE = 32768;  // 32k

    /**
     * DECIMAL type limits
     */

	public static final int DB2_MAX_DECIMAL_PRECISION_SCALE = 31;
	public static final int DB2_DEFAULT_DECIMAL_PRECISION   = 5;
	public static final int DB2_DEFAULT_DECIMAL_SCALE       = 0;

    /**
     * REAL/DOUBLE range limits pre DERBY-3398. After that fix, they are
     * only used in soft-upgrade scenarios with older databases.
     */

    static final float DB2_SMALLEST_REAL = -3.402E+38f;
    static final float DB2_LARGEST_REAL  = +3.402E+38f;
    static final float DB2_SMALLEST_POSITIVE_REAL = +1.175E-37f;
    static final float DB2_LARGEST_NEGATIVE_REAL  = -1.175E-37f;

    static final double DB2_SMALLEST_DOUBLE = -1.79769E+308d;
    static final double DB2_LARGEST_DOUBLE  = +1.79769E+308d;
    static final double DB2_SMALLEST_POSITIVE_DOUBLE = +2.225E-307d;
    static final double DB2_LARGEST_NEGATIVE_DOUBLE  = -2.225E-307d;

    // Limits on the length of the return values for the procedures in
    // LOBStoredProcedure.

    /**
     * The maximum length of the data returned from the BLOB stored procedures.
     * <p>
     * This value is currently dictated by the maximum length of
     * VARCHAR/VARBINARY, because these are the return types of the stored
     * procedures.
     */
    int MAX_BLOB_RETURN_LEN = Limits.DB2_VARCHAR_MAXWIDTH;

    /**
     * The maximum length of the data returned from the CLOB stored procedures.
     * <p>
     * This value is currently dictated by the maximum length of
     * VARCHAR/VARBINARY, because these are the return types of the stored
     * procedures, and the modified UTF8 encoding used for CLOB data. This
     * threshold value could be higher (equal to {@code MAX_BLOB_RETURN_LEN}),
     * but then the procedure fetching data from the CLOB must be rewritten to
     * have more logic.
     * <p>
     * For now we use the defensive assumption that all characters are
     * represented by three bytes.
     */
    int MAX_CLOB_RETURN_LEN = MAX_BLOB_RETURN_LEN / 3;
    
}
