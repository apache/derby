/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.reference
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.reference;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
/**
        This class contains public statics that map directly
        to the new public statics in the jdbc 3.0 classes.
        By providing an intermediary class, we can use the
        same statics without having to import the jdbc 3.0 classes
        into other classes.


        <P>
        This class should not be shipped with the product.

        <P>
        This class has no methods, all it contains are constants
        are public, static and final since they are declared in an interface.
*/

public interface JDBC30Translation {

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;
        /*
        ** public statics from 3.0 version of java.sql.DatabaseMetaData
        */
        public static final int SQL_STATE_XOPEN = DatabaseMetaData.sqlStateXOpen;
        public static final int SQL_STATE_SQL99 = DatabaseMetaData.sqlStateSQL99;

        /*
        ** public statics from 3.0 version of java.sql.ParameterMetaData
        */
        public static final int PARAMETER_NO_NULLS = ParameterMetaData.parameterNoNulls;
        public static final int PARAMETER_NULLABLE = ParameterMetaData.parameterNullable;
        public static final int PARAMETER_NULLABLE_UNKNOWN = ParameterMetaData.parameterNullableUnknown;
        public static final int PARAMETER_MODE_UNKNOWN = ParameterMetaData.parameterModeUnknown;
        public static final int PARAMETER_MODE_IN = ParameterMetaData.parameterModeIn;
        public static final int PARAMETER_MODE_IN_OUT = ParameterMetaData.parameterModeInOut;
        public static final int PARAMETER_MODE_OUT = ParameterMetaData.parameterModeOut;

        /*
        ** public statics from 3.0 version of java.sql.ResultSet
        */
        public static final int HOLD_CURSORS_OVER_COMMIT = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        public static final int CLOSE_CURSORS_AT_COMMIT = ResultSet.CLOSE_CURSORS_AT_COMMIT;

        /*
        ** public statics from 3.0 version of java.sql.Statement
        */
        public static final int CLOSE_CURRENT_RESULT = Statement.CLOSE_CURRENT_RESULT;
        public static final int KEEP_CURRENT_RESULT = Statement.KEEP_CURRENT_RESULT;
        public static final int CLOSE_ALL_RESULTS = Statement.CLOSE_ALL_RESULTS;
        public static final int SUCCESS_NO_INFO = Statement.SUCCESS_NO_INFO;
        public static final int EXECUTE_FAILED = Statement.EXECUTE_FAILED;
        public static final int RETURN_GENERATED_KEYS = Statement.RETURN_GENERATED_KEYS;
        public static final int NO_GENERATED_KEYS = Statement.NO_GENERATED_KEYS;

        /*
        ** public statics from 3.0 version of java.sql.Types
        */
        public static final int DATALINK = Types.DATALINK;
        public static final int BOOLEAN = Types.BOOLEAN;

        /*
        ** New types in JDBC 3.0
        */
        public static final int SQL_TYPES_BOOLEAN = Types.BOOLEAN;
}
