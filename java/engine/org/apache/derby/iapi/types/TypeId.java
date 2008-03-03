/*

   Derby - Class org.apache.derby.iapi.types.TypeId

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

package org.apache.derby.iapi.types;

import java.sql.Types;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.BaseTypeIdImpl;
import org.apache.derby.catalog.types.DecimalTypeIdImpl;
import org.apache.derby.catalog.types.UserDefinedTypeIdImpl;
import org.apache.derby.iapi.reference.JDBC40Translation;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * TypeId describes the static information about a SQL type
 * independent of any specific attributes of the type such
 * as length. So the TypeId for CHARACTER describes the
 * fundamental information about CHARACTER. A specific
 * type (e.g. CHARACTER(10)) is described by a TypeDescriptor for
 * a catlog type and a DataTypeDescriptor for a runtime type.
 * (note a DataTypeDescriptor adds runtime attributes to the
 * TypeDescriptor it has).
 * <P>
 * A TypeId is immutable.
   <P>
 * The equals(Object) method can be used to determine if two typeIds are for the same type,
 * which defines type id equality.


   @see DataTypeDescriptor
 */

public final class TypeId
{
        /**
         * Various fixed numbers related to datatypes.
         */
        public static final int LONGINT_PRECISION                       = 19;
        public static final int LONGINT_SCALE                           = 0;
        public static final int LONGINT_MAXWIDTH                        = 8;

        public static final int INT_PRECISION                   = 10;
        public static final int INT_SCALE                               = 0;
        public static final int INT_MAXWIDTH                    = 4;

        public static final int SMALLINT_PRECISION                      = 5;
        public static final int SMALLINT_SCALE                          = 0;
        public static final int SMALLINT_MAXWIDTH                       = 2;

        public static final int TINYINT_PRECISION                       = 3;
        public static final int TINYINT_SCALE                           = 0;
        public static final int TINYINT_MAXWIDTH                        = 1;

	// precision in number of bits 
        public static final int DOUBLE_PRECISION                        = 52;
	// the ResultSetMetaData needs to have the precision for numeric data
	// in decimal digits, rather than number of bits, so need a separate constant.
        public static final int DOUBLE_PRECISION_IN_DIGITS              = 15;
        public static final int DOUBLE_SCALE                            = 0;
        public static final int DOUBLE_MAXWIDTH                         = 8;

	// precision in number of bits 
        public static final int REAL_PRECISION                  = 23;
	// the ResultSetMetaData needs to have the precision for numeric data
	// in decimal digits, rather than number of bits, so need a separate constant.
        public static final int REAL_PRECISION_IN_DIGITS        = 7;
        public static final int REAL_SCALE                              = 0;
        public static final int REAL_MAXWIDTH                   = 4;

        public static final int DECIMAL_PRECISION                       = Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;
        public static final int DECIMAL_SCALE                           = Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;
        public static final int DECIMAL_MAXWIDTH                        = Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;

        public static final int BOOLEAN_MAXWIDTH                        = 1;

        public static final int CHAR_MAXWIDTH           = Limits.DB2_CHAR_MAXWIDTH;
        public static final int VARCHAR_MAXWIDTH        = Limits.DB2_VARCHAR_MAXWIDTH;
        public static final int LONGVARCHAR_MAXWIDTH = Limits.DB2_LONGVARCHAR_MAXWIDTH;
        public static final int BIT_MAXWIDTH            = Limits.DB2_CHAR_MAXWIDTH;
        public static final int VARBIT_MAXWIDTH         = Limits.DB2_VARCHAR_MAXWIDTH;
        public static final int LONGVARBIT_MAXWIDTH = Limits.DB2_LONGVARCHAR_MAXWIDTH;

        // not supposed to be limited! 4096G should be ok(?), if Derby can handle...
        public static final int BLOB_MAXWIDTH = Integer.MAX_VALUE; // to change long
        public static final int CLOB_MAXWIDTH = Integer.MAX_VALUE; // to change long
        public static final int XML_MAXWIDTH = Integer.MAX_VALUE;

        // Max width for datetime values is the length of the
        // string returned from a call to "toString()" on the
        // java.sql.Date, java.sql.Time, and java.sql.Timestamp
        // classes (the result of toString() on those classes
        // is defined by the JDBC API).  This value is also
        // used as the "precision" for those types.
        public static final int DATE_MAXWIDTH           = 10;	// yyyy-mm-dd
        public static final int TIME_MAXWIDTH           = 8;	// hh:mm:ss
        public static final int TIMESTAMP_MAXWIDTH      = 26;	// yyyy-mm-dd hh:mm:ss.ffffff

        // Scale DOES exist for time values.  For a TIMESTAMP value,
        // it's 6 ('ffffff'); for a TIME value, it's 0 (because there
        // are no fractional seconds).  Note that date values do
        // not have a scale.
        public static final int TIME_SCALE           = 0;
        public static final int TIMESTAMP_SCALE      = 6;

        /* These define all the type names for SQL92 and JDBC 
         * NOTE: boolean is SQL3
         */
          //public static final String      BIT_NAME = "BIT";
          //public static final String      VARBIT_NAME = "BIT VARYING";
          //public static final String      LONGVARBIT_NAME = "LONG BIT VARYING";

        public static final String      BIT_NAME = "CHAR () FOR BIT DATA";
        public static final String      VARBIT_NAME = "VARCHAR () FOR BIT DATA";
        public static final String      LONGVARBIT_NAME = "LONG VARCHAR FOR BIT DATA";
        public static final String      TINYINT_NAME = "TINYINT";
        public static final String      SMALLINT_NAME = "SMALLINT";
        public static final String      INTEGER_NAME = "INTEGER";
        public static final String      LONGINT_NAME = "BIGINT";
        public static final String      FLOAT_NAME = "FLOAT";
        public static final String      REAL_NAME = "REAL";
        public static final String      DOUBLE_NAME = "DOUBLE";
        public static final String      NUMERIC_NAME = "NUMERIC";
        public static final String      DECIMAL_NAME = "DECIMAL";
        public static final String      CHAR_NAME = "CHAR";
        public static final String      VARCHAR_NAME = "VARCHAR";
        public static final String      LONGVARCHAR_NAME = "LONG VARCHAR";
        public static final String      DATE_NAME = "DATE";
        public static final String      TIME_NAME = "TIME";
        public static final String      TIMESTAMP_NAME = "TIMESTAMP";
        public static final String      BINARY_NAME = "BINARY";
        public static final String      VARBINARY_NAME = "VARBINARY";
        public static final String      LONGVARBINARY_NAME = "LONGVARBINARY";
        public static final String      BOOLEAN_NAME = "BOOLEAN";
        public static final String      REF_NAME = "REF";
        public static final String      NATIONAL_CHAR_NAME = "NATIONAL CHAR";
        public static final String      NATIONAL_VARCHAR_NAME = "NATIONAL CHAR VARYING";
        public static final String      NATIONAL_LONGVARCHAR_NAME = "LONG NVARCHAR";
        public static final String      BLOB_NAME = "BLOB";
        public static final String      CLOB_NAME = "CLOB";
        public static final String      NCLOB_NAME = "NCLOB";

        // Following use of "XML" is per SQL/XML (2003) spec,
        // section "10.2 Type name determination".
        public static final String      XML_NAME = "XML";
        
        // ARRAY and STRUCT are JDBC 2.0 data types that are not
        // supported by Derby.
        public static final String      ARRAY_NAME = "ARRAY";
        public static final String      STRUCT_NAME = "STRUCT";

        // DATALINK is a JDBC 3.0 data type. Not supported by Derby.
        public static final String      DATALINK_NAME = "DATALINK";

        // ROWID and SQLXML are new types in JDBC 4.0. Not supported
        // by Derby.
        public static final String      ROWID_NAME = "ROWID";
        public static final String      SQLXML_NAME = "SQLXML";

        /**
         * The following constants define the type precedence hierarchy.
         */
        public static final int USER_PRECEDENCE  = 1000;

        public static final int XML_PRECEDENCE       = 180;
        public static final int BLOB_PRECEDENCE = 170;
        public static final int LONGVARBIT_PRECEDENCE = 160;
        public static final int VARBIT_PRECEDENCE        = 150;
        public static final int BIT_PRECEDENCE           = 140;
        public static final int BOOLEAN_PRECEDENCE       = 130;
        public static final int TIME_PRECEDENCE  = 120;
        public static final int TIMESTAMP_PRECEDENCE = 110;
        public static final int DATE_PRECEDENCE  = 100;
        public static final int DOUBLE_PRECEDENCE        = 90;
        public static final int REAL_PRECEDENCE  = 80;
        public static final int DECIMAL_PRECEDENCE       = 70;
        public static final int NUMERIC_PRECEDENCE       = 69;
        public static final int LONGINT_PRECEDENCE       = 60;
        public static final int INT_PRECEDENCE           = 50;
        public static final int SMALLINT_PRECEDENCE = 40;
        public static final int TINYINT_PRECEDENCE       = 30;
        public static final int REF_PRECEDENCE           = 25;
        public static final int CLOB_PRECEDENCE = 14;
        public static final int LONGVARCHAR_PRECEDENCE = 12;
        public static final int VARCHAR_PRECEDENCE  = 10;
        public static final int CHAR_PRECEDENCE  = 0;

        /*
        ** Static runtime fields for typeIds
        ** These are put here because the system needs them init time.
        */
        public static final TypeId BOOLEAN_ID = create(
            StoredFormatIds.BOOLEAN_TYPE_ID, StoredFormatIds.BOOLEAN_TYPE_ID_IMPL);
        
        public static final TypeId SMALLINT_ID = create(
            StoredFormatIds.SMALLINT_TYPE_ID, StoredFormatIds.SMALLINT_TYPE_ID_IMPL);

        public static final TypeId INTEGER_ID = create(
            StoredFormatIds.INT_TYPE_ID, StoredFormatIds.INT_TYPE_ID_IMPL);

        public static final TypeId CHAR_ID = create(
            StoredFormatIds.CHAR_TYPE_ID, StoredFormatIds.CHAR_TYPE_ID_IMPL);
        
        /*
        ** Others are created on demand by the getBuiltInTypeId(int),
        ** if they are built-in (i.e.? Part of JDBC .Types),
        ** or by getBuiltInTypeId(string) if they are REF_NAME type.
        */

        private static final TypeId TINYINT_ID = create(
                StoredFormatIds.TINYINT_TYPE_ID, StoredFormatIds.TINYINT_TYPE_ID_IMPL);;

        private static final TypeId BIGINT_ID = create(
            StoredFormatIds.LONGINT_TYPE_ID, StoredFormatIds.LONGINT_TYPE_ID_IMPL);
        private static final TypeId REAL_ID = create(
                StoredFormatIds.REAL_TYPE_ID, StoredFormatIds.REAL_TYPE_ID_IMPL);
        private static final TypeId DOUBLE_ID = create(
                StoredFormatIds.DOUBLE_TYPE_ID, StoredFormatIds.DOUBLE_TYPE_ID_IMPL);
        private static final TypeId DECIMAL_ID =  new TypeId(StoredFormatIds.DECIMAL_TYPE_ID, new DecimalTypeIdImpl(false));
        private static final TypeId NUMERIC_ID =  new TypeId(StoredFormatIds.DECIMAL_TYPE_ID, new DecimalTypeIdImpl(true));
        private static final TypeId VARCHAR_ID = create(
                StoredFormatIds.VARCHAR_TYPE_ID, StoredFormatIds.VARCHAR_TYPE_ID_IMPL);
        private static final TypeId DATE_ID = create(
                StoredFormatIds.DATE_TYPE_ID, StoredFormatIds.DATE_TYPE_ID_IMPL);
        private static final TypeId TIME_ID = create(
                StoredFormatIds.TIME_TYPE_ID, StoredFormatIds.TIME_TYPE_ID_IMPL);
        private static final TypeId TIMESTAMP_ID = create(
                StoredFormatIds.TIMESTAMP_TYPE_ID, StoredFormatIds.TIMESTAMP_TYPE_ID_IMPL);
        private static final TypeId BIT_ID = create(
                StoredFormatIds.BIT_TYPE_ID, StoredFormatIds.BIT_TYPE_ID_IMPL);
        private static final TypeId VARBIT_ID = create(
                StoredFormatIds.VARBIT_TYPE_ID, StoredFormatIds.VARBIT_TYPE_ID_IMPL);
        private static final TypeId REF_ID = create(
                StoredFormatIds.REF_TYPE_ID, StoredFormatIds.REF_TYPE_ID_IMPL);
        private static final TypeId LONGVARCHAR_ID = create(
                StoredFormatIds.LONGVARCHAR_TYPE_ID, StoredFormatIds.LONGVARCHAR_TYPE_ID_IMPL);
        private static final TypeId LONGVARBIT_ID = create(
                StoredFormatIds.LONGVARBIT_TYPE_ID, StoredFormatIds.LONGVARBIT_TYPE_ID_IMPL);

        private static final TypeId BLOB_ID = create(
                StoredFormatIds.BLOB_TYPE_ID, StoredFormatIds.BLOB_TYPE_ID_IMPL);
        private static final TypeId CLOB_ID = create(
                StoredFormatIds.CLOB_TYPE_ID, StoredFormatIds.CLOB_TYPE_ID_IMPL);
        private static final TypeId XML_ID = create(
                StoredFormatIds.XML_TYPE_ID, StoredFormatIds.XML_TYPE_ID_IMPL);

        /**
         * Implementation of DECIMAL datatype for generating holders through getNull.
         * Set by the booted DataValueFactory implementation.
         */
        static DataValueDescriptor		decimalImplementation;

        /*
        ** Static methods to obtain TypeIds
        */
        
        /**
         * Create a TypeId for the given format identifiers using
         * a BaseTypeIdImpl. Used to create the static final variables
         * of this class.
         */
        private static TypeId create(int typeFormatId, int implTypeFormatId)
        {
            return new TypeId(typeFormatId, new BaseTypeIdImpl(implTypeFormatId)); 
        }
        
        
        /**
         * Get a TypeId of the given JDBC type.  This factory method is
         * intended to be used for built-in types.  For user-defined types,
         * we will need a factory method that takes a Java type name.
         *
         * @param JDBCTypeId    The JDBC Id of the type, as listed in
         *                      java.sql.Types
         *
         * @return      The appropriate TypeId, or null if there is no such
         *                      TypeId.
         */

        public static TypeId getBuiltInTypeId(int JDBCTypeId) {

        switch (JDBCTypeId) {
        case Types.TINYINT:
            return TINYINT_ID;

        case Types.SMALLINT:
            return SMALLINT_ID;

        case Types.INTEGER:
            return INTEGER_ID;

        case Types.BIGINT:
            return BIGINT_ID;

        case Types.REAL:
            return REAL_ID;

        case Types.FLOAT:
        case Types.DOUBLE:
            return DOUBLE_ID;

        case Types.DECIMAL:
            return DECIMAL_ID;

        case Types.NUMERIC:
            return NUMERIC_ID;

        case Types.CHAR:
            return CHAR_ID;

        case Types.VARCHAR:
            return VARCHAR_ID;

        case Types.DATE:
            return DATE_ID;
        case Types.TIME:
            return TIME_ID;

        case Types.TIMESTAMP:
            return TIMESTAMP_ID;

        case Types.BIT:
        case Types.BOOLEAN:
            return BOOLEAN_ID;

        case Types.BINARY:
            return BIT_ID;

        case Types.VARBINARY:
            return VARBIT_ID;

        case Types.LONGVARBINARY:
            return LONGVARBIT_ID;

        case Types.LONGVARCHAR:
            return LONGVARCHAR_ID;


        case Types.BLOB:
            return BLOB_ID;

        case Types.CLOB:
            return CLOB_ID;

        case JDBC40Translation.SQLXML:
            return XML_ID;
            
        default:
            return null;
        }
    }

        public static TypeId getUserDefinedTypeId(String className, boolean delimitedIdentifier)
        {
                return new TypeId(StoredFormatIds.USERDEFINED_TYPE_ID_V3,
                                        new UserDefinedTypeIdImpl(className), delimitedIdentifier
                                        );
        }

        /**
         * Get a TypeId for the class that corresponds to the given Java type
         * name.
         * 
         * @param javaTypeName
         *            The name of the Java type
         * 
         * @return A TypeId for the SQL type that corresponds to the Java type,
         *         null if there is no corresponding type.
         */
        public static TypeId getSQLTypeForJavaType(String javaTypeName)
        {
                if (javaTypeName.equals("java.lang.Boolean") ||
                        javaTypeName.equals("boolean"))
                {
                        return BOOLEAN_ID;
                }
                else if (javaTypeName.equals("byte[]"))
                {
                        return VARBIT_ID;
                }
                else if (javaTypeName.equals("java.lang.String"))
                {
                        return VARCHAR_ID;
                }
                else if (javaTypeName.equals("java.lang.Integer") ||
                                javaTypeName.equals("int"))
                {
                        return INTEGER_ID;
                }
                else if (javaTypeName.equals("byte"))
                {
                        return TINYINT_ID;
                }
                else if (javaTypeName.equals("short"))
                {
                    return SMALLINT_ID;
                }
                else if (javaTypeName.equals("java.lang.Long") ||
                                javaTypeName.equals("long"))
                {
                     return BIGINT_ID;
                }
                else if (javaTypeName.equals("java.lang.Float") ||
                                javaTypeName.equals("float"))
                {
                     return REAL_ID;
                }
                else if (javaTypeName.equals("java.lang.Double") ||
                                javaTypeName.equals("double"))
                {
                    return DOUBLE_ID;
                }
                else if (javaTypeName.equals("java.math.BigDecimal"))
                {
                    return DECIMAL_ID;
                }
                else if (javaTypeName.equals("java.sql.Date"))
                {
                    return DATE_ID;
                }
                else if (javaTypeName.equals("java.sql.Time"))
                {
                    return TIME_ID;
                }
                else if (javaTypeName.equals("java.sql.Timestamp"))
                {
                    return TIMESTAMP_ID;
                }
                else if (javaTypeName.equals("java.sql.Blob"))
                {
                    return BLOB_ID;
                }
                else if (javaTypeName.equals("java.sql.Clob"))
                {
                    return CLOB_ID;

                }
                else if (javaTypeName.equals("org.apache.derby.iapi.types.XML"))
                {
                    return XML_ID;
                }
                else
                {
                        /*
                        ** If it's a Java primitive type, return null to indicate that
                        ** there is no corresponding SQL type (all the Java primitive
                        ** types that have corresponding SQL types are handled above).
                        **
                        ** There is only one primitive type not mentioned above, char.
                        */
                        if (javaTypeName.equals("char"))
                        {
                                return null;
                        }

                        /*
                        ** It's a non-primitive type (a class) that does not correspond
                        ** to a SQL built-in type, so treat it as a user-defined type.
                        */
                        return TypeId.getUserDefinedTypeId(javaTypeName, false);
                }
        }

        /**
         * Given a SQL type name return the corresponding TypeId.
         * @param SQLTypeName Name of SQL type
         * @return TypeId or null if there is no corresponding SQL type.
         */
        public static TypeId getBuiltInTypeId(String SQLTypeName) {

        if (SQLTypeName.equals(BOOLEAN_NAME)) {
            return BOOLEAN_ID;
        }
        if (SQLTypeName.equals(CHAR_NAME)) {
            return CHAR_ID;
        }
        if (SQLTypeName.equals(DATE_NAME)) {
            return DATE_ID;
        }
        if (SQLTypeName.equals(DOUBLE_NAME)) {
            return DOUBLE_ID;
        }
        if (SQLTypeName.equals(FLOAT_NAME)) {
            return DOUBLE_ID;
        }
        if (SQLTypeName.equals(INTEGER_NAME)) {
            return INTEGER_ID;
        }
        if (SQLTypeName.equals(LONGINT_NAME)) {
            return BIGINT_ID;
        }
        if (SQLTypeName.equals(REAL_NAME)) {
            return REAL_ID;
        }
        if (SQLTypeName.equals(SMALLINT_NAME)) {
            return SMALLINT_ID;
        }
        if (SQLTypeName.equals(TIME_NAME)) {
            return TIME_ID;
        }
        if (SQLTypeName.equals(TIMESTAMP_NAME)) {
            return TIMESTAMP_ID;
        }
        if (SQLTypeName.equals(VARCHAR_NAME)) {
            return VARCHAR_ID;
        }
        if (SQLTypeName.equals(BIT_NAME)) {
            return BIT_ID;
        }
        if (SQLTypeName.equals(VARBIT_NAME)) {
            return VARBIT_ID;
        }
        if (SQLTypeName.equals(TINYINT_NAME)) {
            return TINYINT_ID;
        }
        if (SQLTypeName.equals(DECIMAL_NAME)) {
            return DECIMAL_ID;
        }
        if (SQLTypeName.equals(NUMERIC_NAME)) {
            return NUMERIC_ID;
        }
        if (SQLTypeName.equals(LONGVARCHAR_NAME)) {
            return LONGVARCHAR_ID;
        }
        if (SQLTypeName.equals(LONGVARBIT_NAME)) {
            return LONGVARBIT_ID;
        }
        if (SQLTypeName.equals(BLOB_NAME)) {
            return BLOB_ID;
        }
        if (SQLTypeName.equals(CLOB_NAME)) {
            return CLOB_ID;
        }
        if (SQLTypeName.equals(XML_NAME)) {
            return XML_ID;
        }

        // Types defined below here are SQL types and non-JDBC types that are
        // supported by Derby
        if (SQLTypeName.equals(REF_NAME)) {
            return REF_ID;
        }
        return null;
    }
      
    /**
     * Get the TypeId (fundemental type information)
     * for a catalog type.
     * @param catalogType
     * @return TypeId that represents the base type, null if not applicable.
     */
    static TypeId getTypeId(TypeDescriptor catalogType)
    {
        final int jdbcType = catalogType.getJDBCTypeId();
        TypeId typeId = TypeId.getBuiltInTypeId(jdbcType);
        if (typeId != null)
            return typeId;
        
        if (jdbcType == Types.JAVA_OBJECT) {
            return TypeId.getUserDefinedTypeId(catalogType.getTypeName(), false);
        }
        
        return null;
    }

        /*
         * * Instance fields and methods
         */

        private BaseTypeIdImpl  baseTypeId;
        private int                             formatId;

        /* Set in setTypeIdSpecificInstanceVariables() as needed */
        private boolean                 classNameWasDelimitedIdentifier;
        private boolean                 isBitTypeId;
        private boolean                 isLOBTypeId;
        private boolean                 isBooleanTypeId;
        private boolean                 isConcatableTypeId;
        private boolean                 isDecimalTypeId;
        private boolean                 isLongConcatableTypeId;
        private boolean                 isNumericTypeId;
        private boolean                 isRefTypeId;
        private boolean                 isStringTypeId;
        private boolean                 isFloatingPointTypeId;
        private boolean                 isRealTypeId;
        private boolean                 isDateTimeTimeStampTypeId;
        private boolean                 isUserDefinedTypeId;
        private int                             maxPrecision;
        private int                             maxScale;
        private int                             typePrecedence;
        private String                  javaTypeName;
        private int                             maxMaxWidth;

        /**
         * Constructor for a TypeId
         *
         * @param formatId      Format id of specific type id.
         * @param baseTypeId    The Base type id
         */
        public TypeId(int formatId, BaseTypeIdImpl baseTypeId)
        {
                this.formatId = formatId;
                this.baseTypeId = baseTypeId;
                setTypeIdSpecificInstanceVariables();
        }
        /**
         * Constructor for a TypeId for user defined types
         *
         * @param formatId                                                      Format id of specific type id.
         * @param baseTypeId                                            The Base type id
         * @param classNameWasDelimitedIdentifier       Whether or not the class name
         *                                                                                      was a delimited identifier
         */
        public TypeId(int formatId, BaseTypeIdImpl baseTypeId,
                                                 boolean classNameWasDelimitedIdentifier)
        {
                this.formatId = formatId;
                this.baseTypeId = baseTypeId;
                this.classNameWasDelimitedIdentifier = classNameWasDelimitedIdentifier;
                setTypeIdSpecificInstanceVariables();
        }

        /**
         * we want equals to say if these are the same type id or not.
         */
        public boolean equals(Object that)
        {
                if (that instanceof TypeId)
                        return this.getSQLTypeName().equals(((TypeId)that).getSQLTypeName());
                else
                        return false;
        }

        /*
          Hashcode which works with equals.
          */
        public int hashCode()
        {
                return this.getSQLTypeName().hashCode();
        }


        private void setTypeIdSpecificInstanceVariables()
        {
                switch (formatId)
                {
                        case StoredFormatIds.BIT_TYPE_ID:
                                typePrecedence = BIT_PRECEDENCE;
                                javaTypeName = "byte[]";
                                maxMaxWidth = TypeId.BIT_MAXWIDTH;
                                isBitTypeId = true;
                                isConcatableTypeId = true;
                                break;

                        case StoredFormatIds.BOOLEAN_TYPE_ID:
                                typePrecedence = BOOLEAN_PRECEDENCE;
                                javaTypeName = "java.lang.Boolean";
                                maxMaxWidth = TypeId.BOOLEAN_MAXWIDTH;
                                isBooleanTypeId = true;
                                break;

                        case StoredFormatIds.CHAR_TYPE_ID:
                                typePrecedence = CHAR_PRECEDENCE;
                                javaTypeName = "java.lang.String";
                                maxMaxWidth = TypeId.CHAR_MAXWIDTH;
                                isStringTypeId = true;
                                isConcatableTypeId = true;
                                break;

                        case StoredFormatIds.DATE_TYPE_ID:
                                typePrecedence = DATE_PRECEDENCE;
                                javaTypeName = "java.sql.Date";
                                maxMaxWidth = TypeId.DATE_MAXWIDTH;
                                maxPrecision = TypeId.DATE_MAXWIDTH;
                                isDateTimeTimeStampTypeId = true;
                                break;

                        case StoredFormatIds.DECIMAL_TYPE_ID:
                                maxPrecision = TypeId.DECIMAL_PRECISION;
                                maxScale = TypeId.DECIMAL_SCALE;
                                typePrecedence = DECIMAL_PRECEDENCE;
                                javaTypeName = "java.math.BigDecimal";
                                maxMaxWidth = TypeId.DECIMAL_MAXWIDTH;
                                isDecimalTypeId = true;
                                isNumericTypeId = true;
                                break;

                        case StoredFormatIds.DOUBLE_TYPE_ID:
                                maxPrecision = TypeId.DOUBLE_PRECISION;
                                maxScale = TypeId.DOUBLE_SCALE;
                                typePrecedence = DOUBLE_PRECEDENCE;
                                javaTypeName = "java.lang.Double";
                                maxMaxWidth = TypeId.DOUBLE_MAXWIDTH;
                                isNumericTypeId = true;
								isFloatingPointTypeId = true;
                                break;

                        case StoredFormatIds.INT_TYPE_ID:
                                maxPrecision = TypeId.INT_PRECISION;
                                maxScale = TypeId.INT_SCALE;
                                typePrecedence = INT_PRECEDENCE;
                                javaTypeName = "java.lang.Integer";
                                maxMaxWidth = TypeId.INT_MAXWIDTH;
                                isNumericTypeId = true;
                                break;

                        case StoredFormatIds.LONGINT_TYPE_ID:
                                maxPrecision = TypeId.LONGINT_PRECISION;
                                maxScale = TypeId.LONGINT_SCALE;
                                typePrecedence = LONGINT_PRECEDENCE;
                                javaTypeName = "java.lang.Long";
                                maxMaxWidth = TypeId.LONGINT_MAXWIDTH;
                                isNumericTypeId = true;
                                break;

                        case StoredFormatIds.LONGVARBIT_TYPE_ID:
                                typePrecedence = LONGVARBIT_PRECEDENCE;
                                javaTypeName = "byte[]";
                                maxMaxWidth = TypeId.LONGVARBIT_MAXWIDTH;
                                isBitTypeId = true;
                                isConcatableTypeId = true;
                                isLongConcatableTypeId = true;
                                break;

                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                                typePrecedence = LONGVARCHAR_PRECEDENCE;
                                javaTypeName = "java.lang.String";
                                maxMaxWidth = TypeId.LONGVARCHAR_MAXWIDTH;
                                isStringTypeId = true;
                                isConcatableTypeId = true;
                                isLongConcatableTypeId = true;
                                break;

                        case StoredFormatIds.REAL_TYPE_ID:
                                maxPrecision = TypeId.REAL_PRECISION;
                                maxScale = TypeId.REAL_SCALE;
                                typePrecedence = REAL_PRECEDENCE;
                                javaTypeName = "java.lang.Float";
                                maxMaxWidth = TypeId.REAL_MAXWIDTH;
                                isNumericTypeId = true;
                                isRealTypeId = true;
                                isFloatingPointTypeId = true;
                                break;

                        case StoredFormatIds.REF_TYPE_ID:
                                typePrecedence = REF_PRECEDENCE;
                                isRefTypeId = true;
                                break;

                        case StoredFormatIds.SMALLINT_TYPE_ID:
                                maxPrecision = TypeId.SMALLINT_PRECISION;
                                maxScale = TypeId.SMALLINT_SCALE;
                                typePrecedence = SMALLINT_PRECEDENCE;
                                javaTypeName = "java.lang.Integer";
                                maxMaxWidth = TypeId.SMALLINT_MAXWIDTH;
                                isNumericTypeId = true;
                                break;

                        case StoredFormatIds.TIME_TYPE_ID:
                                typePrecedence = TIME_PRECEDENCE;
                                javaTypeName = "java.sql.Time";
                                maxScale = TypeId.TIME_SCALE;
                                maxMaxWidth = TypeId.TIME_MAXWIDTH;
                                maxPrecision = TypeId.TIME_MAXWIDTH;
                                isDateTimeTimeStampTypeId = true;
                                break;

                        case StoredFormatIds.TIMESTAMP_TYPE_ID:
                                typePrecedence = TIMESTAMP_PRECEDENCE;
                                javaTypeName = "java.sql.Timestamp";
                                maxScale = TypeId.TIMESTAMP_SCALE;
                                maxMaxWidth = TypeId.TIMESTAMP_MAXWIDTH;
                                maxPrecision = TypeId.TIMESTAMP_MAXWIDTH;
                                isDateTimeTimeStampTypeId = true;
                                break;

                        case StoredFormatIds.TINYINT_TYPE_ID:
                                maxPrecision = TypeId.TINYINT_PRECISION;
                                maxScale = TypeId.TINYINT_SCALE;
                                typePrecedence = TINYINT_PRECEDENCE;
                                javaTypeName = "java.lang.Integer";
                                maxMaxWidth = TypeId.TINYINT_MAXWIDTH;
                                isNumericTypeId = true;
                                break;

                        case StoredFormatIds.USERDEFINED_TYPE_ID_V3:
                                if (baseTypeId != null)
                                {
                                        setUserTypeIdInfo();
                                }
                                else
                                {
                                        typePrecedence = USER_PRECEDENCE;
                                }
                                maxMaxWidth = -1;
                                isUserDefinedTypeId = true;
                                break;

                        case StoredFormatIds.VARBIT_TYPE_ID:
                                typePrecedence = VARBIT_PRECEDENCE;
                                javaTypeName = "byte[]";
                                maxMaxWidth = TypeId.VARBIT_MAXWIDTH;
                                isBitTypeId = true;
                                isConcatableTypeId = true;
                                break;

                        case StoredFormatIds.BLOB_TYPE_ID:
                                typePrecedence = BLOB_PRECEDENCE;
                                javaTypeName = "java.sql.Blob";
                                maxMaxWidth = TypeId.BLOB_MAXWIDTH;
                                isBitTypeId = true;
                                isConcatableTypeId = true;
                                isLongConcatableTypeId = true; // ??
                                isLOBTypeId = true;
                                break;

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                                typePrecedence = VARCHAR_PRECEDENCE;
                                javaTypeName = "java.lang.String";
                                maxMaxWidth = TypeId.VARCHAR_MAXWIDTH;
                                isStringTypeId = true;
                                isConcatableTypeId = true;
                                break;

                      case StoredFormatIds.CLOB_TYPE_ID:
                              typePrecedence = CLOB_PRECEDENCE;
                              javaTypeName = "java.sql.Clob";
                              maxMaxWidth = TypeId.CLOB_MAXWIDTH;
                              isStringTypeId = true;
                              isConcatableTypeId = true;
                              isLongConcatableTypeId = true; // ??
                              isLOBTypeId = true;
                              break;

                      case StoredFormatIds.XML_TYPE_ID:

                              typePrecedence = XML_PRECEDENCE;
                              javaTypeName = "org.apache.derby.iapi.types.XML";
                              maxMaxWidth = TypeId.XML_MAXWIDTH;

                              // We set this to true in order to disallow use
                              // of the XML datatype for procedure/function args.
                              isLongConcatableTypeId = true;
                              break;

                }
        }
        /**
         * JDBC has its own idea of type identifiers which is different from
         * the Derby internal type ids.  The JDBC type ids are defined
         * as public final static ints in java.sql.Types.  This method translates
         * a Derby internal TypeId to a JDBC type id. For java objects this
         * returns JAVA_OBJECT in Java2 and OTHER in JDK 1.1. For Boolean datatypes,
         * this returns Type.BOOLEAN in JDK1.4 and Type.BIT for jdks prior to 1.4
         *
         * @return      The JDBC type Id for this type
         */
        public final int getJDBCTypeId()
        {
                return baseTypeId.getJDBCTypeId();
        }
        /**
         * Returns the SQL name of the datatype. If it is a user-defined type,
         * it returns the full Java path name for the datatype, meaning the
         * dot-separated path including the package names.
         *
         * @return      A String containing the SQL name of this type.
         */
        public String   getSQLTypeName()
        {
                return baseTypeId.getSQLTypeName();
        }

        /**
         * Tell whether this is a built-in type.
         * NOTE: There are 3 "classes" of types:
         *                      built-in                - system provided types which are implemented internally
         *                                                        (int, smallint, etc.)
         *                      system built-in - system provided types, independent of implementation
         *                                                        (date, time, etc.)
         *                      user types              - types implemented outside of the system
         *                                                        (java.lang.Integer, asdf.asdf.asdf, etc.)
         *
         * @return      true for built-in types, false for user-defined types.
         */
        public final boolean userType()
        {
                return baseTypeId.userType();
        }

        /**
         * Get the maximum precision of the type.  For types with variable
         * precision, this is an arbitrary high precision.
         *
         * @return      The maximum precision of the type
         */
        public int getMaximumPrecision()
        {
                return maxPrecision;
        }

        /**
         * Get the maximum scale of the type.  For types with variable scale,
         * this is an arbitrary high scale.
         *
         * @return      The maximum scale of the type
         */
        public int getMaximumScale()
        {
                return maxScale;
        }

        private void setUserTypeIdInfo()
        {
                UserDefinedTypeIdImpl baseUserTypeId =
                                                        (UserDefinedTypeIdImpl) baseTypeId;
                typePrecedence = USER_PRECEDENCE;
                javaTypeName = baseUserTypeId.getClassName();
        }

        /**
         * For user types, tell whether or not the class name was a
         * delimited identifier. For all other types, return false.
         *
         * @return Whether or not the class name was a delimited identifier.
         */
        public boolean getClassNameWasDelimitedIdentifier()
        {
                return classNameWasDelimitedIdentifier;
        }

        /**
         * Does this TypeId represent a TypeId for a StringDataType.
         *
         * @return Whether or not this TypeId represents a TypeId for a StringDataType.
         */
        public boolean isStringTypeId()
        {
                return isStringTypeId;
        }

		/**
		 * Is this a TypeId for DATE/TIME/TIMESTAMP
		 *
		 * @return true if this is a DATE/TIME/TIMESTAMP
		 */
		public boolean isDateTimeTimeStampTypeId()
		{
				return isDateTimeTimeStampTypeId;
		}

		/**
		 * Is this a TypeId for REAL
		 *
		 * @return true if this is a REAL
		 */
		public boolean isRealTypeId()
		{
				return isRealTypeId;
		}

		/**
		 * Is this a TypeId for floating point (REAL/DOUBLE)
		 *
		 * @return true if this is a REAL or DOUBLE
		 */
		public boolean isFloatingPointTypeId()
		{
				return isFloatingPointTypeId;
		}
		
		/**
		 * Is this a TypeId for DOUBLE
		 *
		 * @return true if this is a DOUBLE
		 */
		public boolean isDoubleTypeId()
		{
				return isFloatingPointTypeId && (! isRealTypeId);
		}
	
		/**
		 * Is this a fixed string type?
		 * @return true if this is CHAR
		 */
		public boolean isFixedStringTypeId()
		{
				return (formatId == StoredFormatIds.CHAR_TYPE_ID);
		}

		/** 
		 *Is this a Clob?
		 * @return true if this is CLOB
		 */
		public boolean isClobTypeId()
		{
			   return (formatId == StoredFormatIds.CLOB_TYPE_ID);
		}

		/** 
		 *Is this a Blob?
		 * @return true if this is BLOB
		 */
		public boolean isBlobTypeId()
		{
				return ((formatId == StoredFormatIds.BLOB_TYPE_ID));
		}

	
		/** 
		 *Is this a LongVarchar?
		 * @return true if this is LongVarchar
		 */
		public boolean isLongVarcharTypeId()
		{
				return (formatId == StoredFormatIds.LONGVARCHAR_TYPE_ID);
		}


		/** 
		 * Is this DATE/TIME or TIMESTAMP?
		 *
		 * @return true if this DATE/TIME or TIMESTAMP
		 */
		public boolean isDateTimeTimeStampTypeID()
		{
				return ((formatId == StoredFormatIds.DATE_TYPE_ID) ||
						(formatId == StoredFormatIds.TIME_TYPE_ID) ||
						(formatId == StoredFormatIds.TIMESTAMP_TYPE_ID));
		}

        /** 
         *Is this an XML doc?
         * @return true if this is XML
         */
        public boolean isXMLTypeId()
        {
               return (formatId == StoredFormatIds.XML_TYPE_ID);
        }

	
        /**
         * Tell whether this type is orderable, that is, can participate
         * in comparisons.
         *
         * @param cf    A ClassFactory
         *
         * @return      true for orderable types, false for non-orderable types.
         */
        public boolean orderable(ClassFactory cf)
        {
                boolean orderable;
                switch (formatId)
                {
                        // cmp not allowed, indexing not allowed
                        case StoredFormatIds.BLOB_TYPE_ID:
                        case StoredFormatIds.CLOB_TYPE_ID:
                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                        case StoredFormatIds.XML_TYPE_ID:
                        case StoredFormatIds.LONGVARBIT_TYPE_ID:
                                return false;

                        case StoredFormatIds.USERDEFINED_TYPE_ID_V3:
                                /* Is this type orderable? */

                                // For user java classes we are orderable if we
                                // implement java.lang.Orderable (JDK1.2) or
                                // have a int compareTo(Object) method (JDK1.1 or JDK1.2)
                                UserDefinedTypeIdImpl baseUserTypeId =
                                                                                (UserDefinedTypeIdImpl) baseTypeId;

                                String className = baseUserTypeId.getClassName();

                                try 
                                {
                                        Class c = cf.getClassInspector().getClass(className);
                                        orderable = java.lang.Comparable.class.isAssignableFrom(c);
                                } 
                                catch (ClassNotFoundException cnfe) 
                                {
                                        orderable = false;
                                } 
                                break;

                        default:
                                orderable = true;
                }

                return orderable;
        }

        /**
         * Each built-in type in JSQL has a precedence.  This precedence determines
         * how to do type promotion when using binary operators.  For example, float
         * has a higher precedence than int, so when adding an int to a float, the
         * result type is float.
         *
         * The precedence for some types is arbitrary.  For example, it doesn't
         * matter what the precedence of the boolean type is, since it can't be
         * mixed with other types.  But the precedence for the number types is
         * critical.  The SQL standard requires that exact numeric types be
         * promoted to approximate numeric when one operator uses both.  Also,
         * the precedence is arranged so that one will not lose precision when
         * promoting a type.
         * NOTE: char, varchar, and longvarchar must appear at the bottom of
         * the hierarchy, but above USER_PRECEDENCE, since we allow the implicit
         * conversion of those types to any other built-in system type.
         *
         * @return              The precedence of this type.
         */
        public int typePrecedence()
        {
                return typePrecedence;
        }

         /**
         * Get the name of the corresponding Java type.
         *
         * Each SQL type has a corresponding Java type.  When a SQL value is
         * passed to a Java method, it is translated to its corresponding Java
         * type.  For example, when a SQL date column is passed to a method,
         * it is translated to a java.sql.Date.
         *
         * @return      The name of the corresponding Java type.
         */
        public String getCorrespondingJavaTypeName()
        {
                if (SanityManager.DEBUG)
                {
                        if (formatId == StoredFormatIds.REF_TYPE_ID)
                        {
                                SanityManager.THROWASSERT("getCorrespondingJavaTypeName not implemented for StoredFormatIds.REF_TYPE_ID");
                        }
                        SanityManager.ASSERT(javaTypeName != null,
                                "javaTypeName expected to be non-null");
                }
                return javaTypeName;
        }

         /**
         * Get the name of the corresponding Java type.
         *
         * This method is used directly from EmbedResultSetMetaData (jdbc)
         * to return the corresponding type (as choosen by getObject).
         * It solves a specific problem for BLOB types where the 
         * getCorrespondingJavaTypeName() is used internall for casting
         * which doesn't work if changed from byte[] to java.sql.Blob.
         * So we do it here instread, to avoid unexpected sideeffects.
         *
         * @return      The name of the corresponding Java type.
         */
        public String getResultSetMetaDataTypeName()
        {
            if ((BLOB_ID != null) && BLOB_ID.equals(this))
                return "java.sql.Blob";
            if ((CLOB_ID != null) && CLOB_ID.equals(this))
                return "java.sql.Clob";
            return getCorrespondingJavaTypeName();
        }

        /**
         * Get the maximum maximum width of the type (that's not a typo).  For
         * types with variable length, this is the absolute maximum for the type.
         *
         * @return      The maximum maximum width of the type
         */
        public int getMaximumMaximumWidth()
        {
                return maxMaxWidth;
        }

        /**
         * Converts this TypeId, given a data type descriptor (including length/precision),
         * to a string. E.g.
         *
         *                      VARCHAR(30)
         *
         *
         *      For most data types, we just return the SQL type name.
         *
         *      @param  dts     Data type descriptor that holds the length/precision etc. as necessary
         *
         *       @return        String version of datatype, suitable for running through
         *                      the Parser.
         */
        public String   toParsableString(DataTypeDescriptor dts)
        {
                return  baseTypeId.toParsableString(dts.getCatalogType());
        }

        /**
         * Is this a type id for a numeric type?
         *
         * @return Whether or not this a type id for a numeric type.
         */
        public boolean isNumericTypeId()
        {
                return isNumericTypeId;
        }

        /**
         * Is this a type id for a decimal type?
         *
         * @return Whether or not this a type id for a decimal type.
         */
        public boolean isDecimalTypeId()
        {
                return isDecimalTypeId;
        }


        /**
         * Is this a type id for a boolean type?
         *
         * @return Whether or not this a type id for a boolean type.
         */
        public boolean isBooleanTypeId()
        {
                return isBooleanTypeId;
        }

        /**
         * Is this a type id for a ref type?
         *
         * @return Whether or not this a type id for a ref type.
         */
        public boolean isRefTypeId()
        {
                return isRefTypeId;
        }

        /**
         * Is this a type id for a concatable type?
         *
         * @return Whether or not this a type id for a concatable type.
         */
        public boolean isConcatableTypeId()
        {
                return isConcatableTypeId;
        }

        /**
         * Is this a type id for a bit type?
         *
         * @return Whether or not this a type id for a bit type.
         */
        public boolean isBitTypeId()
        {
                return isBitTypeId;
        }

        /**
         * Is this a type id for a LOB type?
         *
         * @return Whether or not this a type id for a LOB type.
         */
        public boolean isLOBTypeId()
        {
                return isLOBTypeId;
        }


        /**
         * Is this a type id for a long concatable type?
         *
         * @return Whether or not this a type id for a long concatable type.
         */
        public boolean isLongConcatableTypeId()
        {
                return isLongConcatableTypeId;
        }

        /**
         * Is this a type id for a user defined type?
         *
         * @return Whether or not this a type id for a user defined type.
         */
        public boolean isUserDefinedTypeId()
        {
                return isUserDefinedTypeId;
        }


        /**
         * Get the formatID which corresponds to this class.
         *
         *      @return the formatID of this class
         */
        public  int     getTypeFormatId()       
        { 
                return formatId; 
        }


        /** 
         *  Get SQL null value.
         *  @return SQL null value for this type.
         */
        public DataValueDescriptor getNull()
        {
                switch (formatId)
                {
                        case StoredFormatIds.BIT_TYPE_ID:
                                return new SQLBit();

                        case StoredFormatIds.BOOLEAN_TYPE_ID:
                                return new SQLBoolean();

                        case StoredFormatIds.CHAR_TYPE_ID:
                                return new SQLChar();

                        // Implementation of DECIMAL can change.
                        case StoredFormatIds.DECIMAL_TYPE_ID:
                        	return decimalImplementation.getNewNull();

                        case StoredFormatIds.DOUBLE_TYPE_ID:
                                return new SQLDouble();

                        case StoredFormatIds.INT_TYPE_ID:
                                return new SQLInteger();

                        case StoredFormatIds.LONGINT_TYPE_ID:
                                return new SQLLongint();

                        case StoredFormatIds.LONGVARBIT_TYPE_ID:
                                return new SQLLongVarbit();

                        case StoredFormatIds.BLOB_TYPE_ID:
                                return new SQLBlob();

                        case StoredFormatIds.CLOB_TYPE_ID:
                                return new SQLClob();

                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                                return new SQLLongvarchar();

                        case StoredFormatIds.REAL_TYPE_ID:
                                return new SQLReal();

                        case StoredFormatIds.REF_TYPE_ID:
                                return new SQLRef();

                        case StoredFormatIds.SMALLINT_TYPE_ID:
                                return new SQLSmallint();

                        case StoredFormatIds.TINYINT_TYPE_ID:
                                return new SQLTinyint();

                        case StoredFormatIds.DATE_TYPE_ID:
                                return new SQLDate();

                        case StoredFormatIds.TIME_TYPE_ID:
                                return new SQLTime();

                        case StoredFormatIds.TIMESTAMP_TYPE_ID:
                                return new SQLTimestamp();

                        case StoredFormatIds.USERDEFINED_TYPE_ID_V3:
                                return new UserType();

                        case StoredFormatIds.VARBIT_TYPE_ID:
                                return new SQLVarbit();

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                                return new SQLVarchar();

                        case StoredFormatIds.XML_TYPE_ID:
                                return new XML();

                        default:
                                if (SanityManager.DEBUG)
                                {
                                        SanityManager.THROWASSERT(
                                                "unexpected formatId in getNull() - " + formatId);
                                }
                                return null;
                }
        }
        /**
         * Is this type StreamStorable?
         *
         * @return      true if this type has variable length.
         */
        public boolean  streamStorable() {
                return isStringTypeId() || isBitTypeId();
        }


        //
        //      Class methods
        //

        /**
         * Get the approximate length of this type in bytes.
         * For most datatypes this is just going to be
         * dts.getMaximumWidth().  Some types, such as
         * bit, will override this.
         *
         * @param dts Data type descriptor that holds the 
         *              length/precision etc. as necessary
         *
         * @return the length in bytes
         */
        public int getApproximateLengthInBytes(DataTypeDescriptor dts)
        {
                switch (formatId)
                {
                        case StoredFormatIds.BIT_TYPE_ID:
                                return (int)(Math.ceil(dts.getMaximumWidth()/8d));

                        case StoredFormatIds.CHAR_TYPE_ID:
                                return (2 * dts.getMaximumWidth()) + 2;

                        case StoredFormatIds.DECIMAL_TYPE_ID:
                                // Return 200 if precision is max int
                                if (dts.getPrecision() == Integer.MAX_VALUE)
                                {
                                        return 200;
                                }
                                else
                                {
                                        return 8 + (int) (Math.ceil(((double)dts.getPrecision())/2d));
                                }

                        case StoredFormatIds.LONGVARBIT_TYPE_ID:
                        case StoredFormatIds.BLOB_TYPE_ID:
                        case StoredFormatIds.CLOB_TYPE_ID:
                        // RESOLVE: Should XML be here?  What's this value mean, anyway?
                        case StoredFormatIds.XML_TYPE_ID:
                                return 10240;

                        case StoredFormatIds.REF_TYPE_ID:
                                return 16;

                        case StoredFormatIds.USERDEFINED_TYPE_ID_V3:
                                /* For user types we'll guess on the high side
                                ** (200) to avoid being too low in most cases.
                                */
                                return 200;

                        case StoredFormatIds.VARBIT_TYPE_ID:
                                // Return 200 if maximum width is max int
                                if (dts.getMaximumWidth() == Integer.MAX_VALUE)
                                {
                                        return 200;
                                }
                                else
                                {
                                        return (int)(Math.ceil(dts.getMaximumWidth()/8d));
                                }

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                                // Return 200 if maximum width is max int
                                if (dts.getMaximumWidth() == Integer.MAX_VALUE)
                                {
                                        return 200;
                                }
                                else
                                {
                                        return (dts.getMaximumWidth() * 2) + 2;
                                }
                        /*
                        ** For Date/time we know the exact size
                        ** thanks to some investigative work by
                        ** someone or other (sad isn't it).  
                        */
                        case StoredFormatIds.DATE_TYPE_ID:
                                return 18;
                        case StoredFormatIds.TIME_TYPE_ID:
                                return 16;
                        case StoredFormatIds.TIMESTAMP_TYPE_ID:
                                return 29;

                        default:
                                return dts.getMaximumWidth();
                }
        }

        /**
         * Get the base type id that is embedded in this type id.  The base type
         * id is an object with a minimal implementation of TypeId that is intended
         * to be usable on the client side.
         */
        public BaseTypeIdImpl getBaseTypeId()
        {
                return baseTypeId;
        }

        /**
         * Get the precision of the merge of two Decimals
         *
         * @param leftType the left type
         * @param rightType the left type
         *
         * @return      the resultant precision
         */
        public int getPrecision(DataTypeDescriptor leftType,
                                        DataTypeDescriptor rightType)
        {
                if (SanityManager.DEBUG)
                {
                        if (formatId != StoredFormatIds.DECIMAL_TYPE_ID)
                        {
                                SanityManager.THROWASSERT(
                                        "getPrecision() not expected to be called for formatId - " + formatId);
                        }
                }
                long lscale = (long)leftType.getScale();
                long rscale = (long)rightType.getScale();
                long lprec = (long)leftType.getPrecision();
                long rprec = (long)rightType.getPrecision();
                long val;

                /*
                ** Take the maximum left of decimal digits plus the scale.
                */
                val = this.getScale(leftType, rightType) +
                                        Math.max(lprec - lscale, rprec - rscale);

                if (val > Integer.MAX_VALUE)
                {
                        val = Integer.MAX_VALUE;
                }
                return (int)val;
        }

        /**
         * Get the scale of the merge of two decimals
         *
         * @param leftType the left type
         * @param rightType the left type
         *
         * @return      the resultant precision
         */
        public int getScale(DataTypeDescriptor leftType,
                                                DataTypeDescriptor rightType)
        {
                if (SanityManager.DEBUG)
                {
                        if (formatId != StoredFormatIds.DECIMAL_TYPE_ID)
                        {
                                SanityManager.THROWASSERT(
                                        "getPrecision() not expected to be called for formatId - " + formatId);
                        }
                }
                /*
                ** Retain greatest scale
                */
                return Math.max(leftType.getScale(), rightType.getScale());
        }

        /**
         * Does type hava a declared variable length (defined by the application).
         * Examples are CHAR(10), CLOB(1M).
         * Unbounded long types, like LONG VARCHAR return false here.
         * @return boolean true if type is variable length false if not.  
         */
        public boolean variableLength()
        {
                switch (formatId)
                {
                        case StoredFormatIds.BIT_TYPE_ID:
                        case StoredFormatIds.VARBIT_TYPE_ID:
                        case StoredFormatIds.DECIMAL_TYPE_ID:
                        case StoredFormatIds.CHAR_TYPE_ID:
                        case StoredFormatIds.VARCHAR_TYPE_ID:
                        case StoredFormatIds.BLOB_TYPE_ID:
                        case StoredFormatIds.CLOB_TYPE_ID:
                                 return true;

                        default:
                                return false;
                }
        }
}

