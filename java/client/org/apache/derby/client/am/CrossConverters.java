/*

   Derby - Class org.apache.derby.client.am.CrossConverters

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.apache.derby.shared.common.reference.SQLState;

// All currently supported derby types are mapped to one of the following jdbc types:
// java.sql.Types.SMALLINT;
// java.sql.Types.INTEGER;
// java.sql.Types.BIGINT;
// java.sql.Types.REAL;
// java.sql.Types.DOUBLE;
// java.sql.Types.DECIMAL;
// java.sql.Types.DATE;
// java.sql.Types.TIME;
// java.sql.Types.TIMESTAMP;
// java.sql.Types.CHAR;
// java.sql.Types.VARCHAR;
// java.sql.Types.LONGVARCHAR;
// java.sql.Types.CLOB;
// java.sql.Types.BLOB;
//

final class CrossConverters {

    /**
     * Value used to signal unknown length of data.
     */
    public static final int UNKNOWN_LENGTH = Integer.MIN_VALUE;

    private final static java.math.BigDecimal bdMaxByteValue__ =
            java.math.BigDecimal.valueOf(Byte.MAX_VALUE);
    private final static java.math.BigDecimal bdMinByteValue__ =
            java.math.BigDecimal.valueOf(Byte.MIN_VALUE);
    private final static java.math.BigDecimal bdMaxShortValue__ =
            java.math.BigDecimal.valueOf(Short.MAX_VALUE);
    private final static java.math.BigDecimal bdMinShortValue__ =
            java.math.BigDecimal.valueOf(Short.MIN_VALUE);
    private final static java.math.BigDecimal bdMaxIntValue__ =
            java.math.BigDecimal.valueOf(Integer.MAX_VALUE);
    private final static java.math.BigDecimal bdMinIntValue__ =
            java.math.BigDecimal.valueOf(Integer.MIN_VALUE);
    private final static java.math.BigDecimal bdMaxLongValue__ =
            java.math.BigDecimal.valueOf(Long.MAX_VALUE);
    private final static java.math.BigDecimal bdMinLongValue__ =
            java.math.BigDecimal.valueOf(Long.MIN_VALUE);
    private final static java.math.BigDecimal bdMaxFloatValue__ =
            new java.math.BigDecimal(Float.MAX_VALUE);
    private final static java.math.BigDecimal bdMinFloatValue__ =
            new java.math.BigDecimal(-Float.MAX_VALUE);
    private final static java.math.BigDecimal bdMaxDoubleValue__ =
            new java.math.BigDecimal(Double.MAX_VALUE);
    private final static java.math.BigDecimal bdMinDoubleValue__ =
            new java.math.BigDecimal(-Double.MAX_VALUE);

    // Since BigDecimals are immutable, we can return pointers to these canned 0's and 1's.
    private final static java.math.BigDecimal bdZero__ = java.math.BigDecimal.valueOf(0);
    private final static java.math.BigDecimal bdOne__ = java.math.BigDecimal.valueOf(1);

    // ---------------------- state ----------------------------------------------

    Agent agent_;

    // ----------------------constructors/finalizer-------------------------------

    CrossConverters(Agent agent) {
        agent_ = agent;
    }

    // ---------------------------------------------------------------------------
    // The following methods are used for input cross conversion.
    // ---------------------------------------------------------------------------

    //---------------------------- setObject() methods ---------------------------

    // Convert from boolean source to target type.
    // In support of PS.setBoolean().
    // See differences.html for DNC setBoolean() semantics.
    final Object setObject(int targetType, boolean source) throws SqlException {
        return setObject(targetType, (short) (source ? 1 : 0));
    }

    // Convert from byte source to target type
    // In support of PS.setByte()
    final Object setObject(int targetType, byte source) throws SqlException {
        return setObject(targetType, (short) source);
    }

    // Convert from short source to target type
    // In support of PS.setShort()
    final Object setObject(int targetType, short source) throws SqlException {
        switch (targetType) {
        case Types.SMALLINT:
            return new Short(source);

        case Types.INTEGER:
            return new Integer(source);

        case Types.BIGINT:
            return new Long(source);

        case Types.REAL:
            return new Float(source);

        case Types.DOUBLE:
            return new Double(source);

        case Types.DECIMAL:
            return java.math.BigDecimal.valueOf(source);

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "byte", Types.getTypeString(targetType));
        }
    }

    // Convert from integer source to target type
    // In support of PS.setInt()
    final Object setObject(int targetType, int source) throws SqlException {
        switch (targetType) {
        case Types.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Short((short) source);

        case Types.INTEGER:
            return new Integer(source);

        case Types.BIGINT:
            return new Long(source);

        case Types.REAL:
            return new Float(source);

        case Types.DOUBLE:
            return new Double(source);

        case Types.DECIMAL:
            return java.math.BigDecimal.valueOf(source);

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                    "int", Types.getTypeString(targetType));
        }
    }

    // This method is used in lieu of setObject(targetType, sourceObject) because we
    // don't support the BIT/BOOLEAN as underlying DERBY targetTypes.
    final boolean setBooleanFromObject(Object source, int sourceType) throws SqlException {
        switch (sourceType) {
        case Types.SMALLINT:
            return getBooleanFromShort(((Short) source).shortValue());
        case Types.INTEGER:
            return getBooleanFromInt(((Integer) source).intValue());
        case Types.BIGINT:
            return getBooleanFromLong(((java.math.BigInteger) source).longValue());
        case Types.REAL:
            return getBooleanFromFloat(((Float) source).floatValue());
        case Types.DOUBLE:
            return getBooleanFromDouble(((Double) source).doubleValue());
        case Types.DECIMAL:
            return getBooleanFromLong(((java.math.BigDecimal) source).longValue());
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return getBooleanFromString((String) source);
        default:
            throw new ColumnTypeConversionException(agent_.logWriter_,
                Types.getTypeString(sourceType), "boolean");
        }
    }

    // This method is used in lieu of setObject(targetType, sourceObject) because we
    // don't support the BIT/BOOLEAN as underlying DERBY targetTypes.
    final byte setByteFromObject(Object source, int sourceType) throws SqlException {
        switch (sourceType) {
        case Types.SMALLINT:
            return getByteFromShort(((Short) source).shortValue());
        case Types.INTEGER:
            return getByteFromInt(((Integer) source).intValue());
        case Types.BIGINT:
            return getByteFromLong(((java.math.BigInteger) source).longValue());
        case Types.REAL:
            return getByteFromFloat(((Float) source).floatValue());
        case Types.DOUBLE:
            return getByteFromDouble(((Double) source).doubleValue());
        case Types.DECIMAL:
            return getByteFromLong(((java.math.BigDecimal) source).longValue());
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return getByteFromString((String) source);
        default:
            throw new ColumnTypeConversionException(agent_.logWriter_,
                Types.getTypeString(sourceType), "byte");
        }
    }

    // Convert from long source to target type
    // In support of PS.setLong()
    final Object setObject(int targetType, long source) throws SqlException {
        switch (targetType) {
        case Types.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Short((short) source);

        case Types.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Integer((int) source);

        case Types.BIGINT:
            return new Long(source);

        case Types.REAL:
            return new Float(source);

        case Types.DOUBLE:
            return new Double(source);

        case Types.DECIMAL:
            return java.math.BigDecimal.valueOf(source);

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "long", Types.getTypeString(targetType));
        }
    }

    // Convert from floating point source to target type
    // In support of PS.setFloat()
    final Object setObject(int targetType, float source) throws SqlException {
        switch (targetType) {
        case Types.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Short((short) source);

        case Types.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Integer((int) source);

        case Types.BIGINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Long.MAX_VALUE || source < Long.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Long((long) source);

        case Types.REAL:
            if (Configuration.rangeCheckCrossConverters &&
                    // change the check from (source > Float.MAX_VALUE || source < -Float.MIN_VALUE))
                    // to the following:
                    //-----------------------------------------------------------------------------------
                    //   -infinity                             0                            +infinity
                    //           |__________________________|======|________________________|
                    //  <-3.4E+38|                          |      |                        |>+3.4E+38
                    //           |                          |      |_________________       |
                    //           |                          |-1.4E-45 <X< +1.4E-45
                    //           |                          |________________________
                    //-----------------------------------------------------------------------------------
                    (source == Float.POSITIVE_INFINITY || source == Float.NEGATIVE_INFINITY)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Float(source);

        case Types.DOUBLE:
            if (Configuration.rangeCheckCrossConverters &&
                    //-------------------------------------------------------------------------------------
                    //    -infinity                             0                            +infinity
                    //            |__________________________|======|________________________|
                    // <-1.79E+308|                          |      |                        |>+1.79E+308
                    //            |                          |      |_________________       |
                    //            |                          |-4.9E-324 <X< +4.9E-324
                    //            |                          |________________________
                    //-------------------------------------------------------------------------------------
                    (source == Double.POSITIVE_INFINITY || source == Double.NEGATIVE_INFINITY)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            // source passed in is a float, do we need to check if the source already contains "infinity"??
            return new Double(String.valueOf(source));

        case Types.DECIMAL:
            // Can't use the following commented out line because it changes precision of the result.
            //return new java.math.BigDecimal (source);
            return new java.math.BigDecimal(String.valueOf(source));  // This matches derby semantics

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "float", Types.getTypeString(targetType));
        }
    }

    // Convert from double floating point source to target type
    // In support of PS.setDouble()
    final Object setObject(int targetType, double source) throws SqlException {
        switch (targetType) {
        case Types.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Short((short) source);

        case Types.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Integer((int) source);

        case Types.BIGINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Long.MAX_VALUE || source < Long.MIN_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Long((long) source);

        case Types.REAL:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Float.MAX_VALUE || source < -Float.MAX_VALUE)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Float((float) source);

        case Types.DOUBLE:
            if (Configuration.rangeCheckCrossConverters &&
                    // change the check from (source > Double.MAX_VALUE || source < -Double.MIN_VALUE))
                    // to the following:
                    //-------------------------------------------------------------------------------------
                    //    -infinity                             0                            +infinity
                    //            |__________________________|======|________________________|
                    // <-1.79E+308|                          |      |                        |>+1.79E+308
                    //            |                          |      |_________________       |
                    //            |                          |-4.9E-324 <X< +4.9E-324
                    //            |                          |________________________
                    //-------------------------------------------------------------------------------------
                    (source == Double.POSITIVE_INFINITY || source == Double.NEGATIVE_INFINITY)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Double(source);

        case Types.DECIMAL:
            return new java.math.BigDecimal(String.valueOf(source));  // This matches derby semantics
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "double", Types.getTypeString(targetType));
        }
    }

    // Convert from big decimal source to target type
    // In support of PS.setBigDecimal()
    final Object setObject(int targetType, java.math.BigDecimal source) throws SqlException {
        switch (targetType) {
        case Types.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxShortValue__) == 1 || source.compareTo(bdMinShortValue__) == -1)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Short(source.shortValue());

        case Types.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxIntValue__) == 1 || source.compareTo(bdMinIntValue__) == -1)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Integer(source.intValue());

        case Types.BIGINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxLongValue__) == 1 || source.compareTo(bdMinLongValue__) == -1)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Long(source.longValue());

        case Types.REAL:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxFloatValue__) == 1 || source.compareTo(bdMinFloatValue__) == -1)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Float(source.floatValue());

        case Types.DOUBLE:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxDoubleValue__) == 1 || source.compareTo(bdMinDoubleValue__) == -1)) {
                throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
            }
            return new Double(source.doubleValue());

        case Types.DECIMAL:
            return source;

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.Math.BigDecimal", Types.getTypeString(targetType));
        }
    }

    // Convert from date source to target type
    // In support of PS.setDate()
    final Object setObject(int targetType, java.sql.Date source) throws SqlException {
        switch (targetType) {

        case java.sql.Types.DATE:
            return source;

        case java.sql.Types.TIMESTAMP:
            return new java.sql.Timestamp(source.getTime());

        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Date", Types.getTypeString(targetType));
        }
    }

    // Convert from time source to target type
    // In support of PS.setTime()
    final Object setObject(int targetType, java.sql.Time source) throws SqlException {
        switch (targetType) {

        case java.sql.Types.TIME:
            return source;

        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Time", Types.getTypeString(targetType));
        }
    }

    // Convert from date source to target type
    // In support of PS.setTimestamp()
    final Object setObject(int targetType, java.sql.Timestamp source) throws SqlException {
        switch (targetType) {

        case java.sql.Types.TIMESTAMP:
            return source;

        case java.sql.Types.TIME:
            return new java.sql.Time(source.getTime());

        case java.sql.Types.DATE:
            return new java.sql.Date(source.getTime());

        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Timestamp", Types.getTypeString(targetType));
        }
    }

    // setString() against BINARY columns cannot be implemented consistently because w/out metadata, we'll send char encoding bytes.
    // So we refuse setString() requests altogether.
    // Convert from string source to target type.
    // In support of PS.setString()
    final Object setObject(int targetDriverType, String source) throws SqlException {
        try {
            switch (targetDriverType) {
            case Types.SMALLINT:
                return Short.valueOf(source);

            case Types.INTEGER:
                return Integer.valueOf(source);

            case Types.BIGINT:
                return Long.valueOf(source);

            case Types.REAL:
                return Float.valueOf(source);

            case Types.DOUBLE:
                return Double.valueOf(source);

            case Types.DECIMAL:
                return new java.math.BigDecimal(source);

            case java.sql.Types.DATE:
                return date_valueOf(source, null);

            case java.sql.Types.TIME:
                return time_valueOf(source, null);

            case java.sql.Types.TIMESTAMP:
                return timestamp_valueOf(source, null);

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return source;

            case Types.CLOB:
                return new Clob(agent_, source);

                // setString() against BINARY columns is problematic because w/out metadata, we'll send char encoding bytes.
                // So we refuse setString() requests altogether.
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "String", Types.getTypeString(targetDriverType));
            }
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId 
                    (SQLState.LANG_FORMAT_EXCEPTION), 
                    Types.getTypeString(targetDriverType),
                    e);                    
        }
    }

    // ------ method to convert to targetJdbcType ------
    /**
     * Convert the input targetJdbcType to the correct JdbcType used by CrossConverters.
     */
    public static int getInputJdbcType(int jdbcType) {
        switch (jdbcType) {
        case java.sql.Types.BIT:
        case java.sql.Types.BOOLEAN:
        case java.sql.Types.TINYINT:
        case java.sql.Types.SMALLINT:
            return java.sql.Types.INTEGER;
        case java.sql.Types.NUMERIC:
            return java.sql.Types.DECIMAL;
        case java.sql.Types.FLOAT:
            return java.sql.Types.DOUBLE;
        default:
            return jdbcType;
        }

    }


    // -- methods in support of setObject(String)/getString() on BINARY columns---


    // Convert from byte[] source to target type
    // In support of PS.setBytes()
    final Object setObject(int targetType, byte[] source) throws SqlException {
        switch (targetType) {
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return source;
        case Types.BLOB:
            return new Blob(source, agent_, 0);
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "byte[]", Types.getTypeString(targetType));
        }
    }

    // Convert from Reader source to target type
    // In support of PS.setCharacterStream()
    final Object setObject(int targetType, java.io.Reader source, int length) throws SqlException {
        switch (targetType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return setStringFromReader(source, length);
        case Types.CLOB:
            if (length == CrossConverters.UNKNOWN_LENGTH) {
                return new Clob(agent_, source);
            }
            return new Clob(agent_, source, length);
            // setCharacterStream() against BINARY columns is problematic because w/out metadata, we'll send char encoding bytes.
            // There's no clean solution except to just not support setObject(String/Reader/Stream)
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.io.Reader", Types.getTypeString(targetType));
        }
    }

    // create a String by reading all of the bytes from reader
    private final String setStringFromReader(java.io.Reader r, int length) throws SqlException {
        java.io.StringWriter sw = new java.io.StringWriter();
        try {
            int read = r.read();
            int totalRead = 0;
            while (read != -1) {
                totalRead++;
                sw.write(read);
                read = r.read();
            }
            if (length != CrossConverters.UNKNOWN_LENGTH &&
                    length != totalRead) {
                throw new SqlException(agent_.logWriter_, 
                		new ClientMessageId (SQLState.READER_UNDER_RUN));
            }
            return sw.toString();
        } catch (java.io.IOException e) {
            throw SqlException.javaException(agent_.logWriter_, e);
        }
    }

    // Convert from InputStream source to target type.
    // In support of PS.setAsciiStream, PS.setUnicodeStream
    // Note: PS.setCharacterStream() is handled by setObject(Reader)
    final Object setObjectFromCharacterStream(int targetType, java.io.InputStream source, String encoding, int length) throws SqlException {
        switch (targetType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return setStringFromStream(source, encoding, length);
        case Types.CLOB:
            if (length == CrossConverters.UNKNOWN_LENGTH) {
                return new Clob(agent_, source, encoding);
            }
            return new Clob(agent_, source, encoding, length);
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.io.InputStream", Types.getTypeString(targetType));
        }
    }


    // create a String by reading all of the bytes from inputStream, applying encoding
    private final String setStringFromStream(java.io.InputStream is, String encoding, int length) throws SqlException {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            int totalRead = 0;

            try {
                int read = is.read();
                while (read != -1) {
                    totalRead++;
                    baos.write(read);
                    read = is.read();
                }
            } catch (java.io.IOException e) {
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.JAVA_EXCEPTION),
                    e.getClass().getName(), e.getMessage(), e);
            }

            if (length != CrossConverters.UNKNOWN_LENGTH &&
                    length != totalRead) {
                throw new SqlException(agent_.logWriter_, 
                		new ClientMessageId (SQLState.READER_UNDER_RUN));
            }

            return new String(baos.toByteArray(), encoding);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId (SQLState.UNSUPPORTED_ENCODING), 
                    "java.io.InputStream", "String", e);
        }
    }

    // Convert from Blob source to target type
    // In support of PS.setBlob()
    final Object setObject(int targetType, java.sql.Blob source) throws SqlException {
        switch (targetType) {
        case Types.BLOB:
            return source;
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Blob", Types.getTypeString(targetType));
        }
    }

    // Convert from InputStream source to target type
    // In support of PS.setBinaryStream()
    final Object setObjectFromBinaryStream(int targetType, java.io.InputStream source, int length) throws SqlException {
        switch (targetType) {
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            return setBytesFromStream(source, length);
        case Types.BLOB:
            if (length == CrossConverters.UNKNOWN_LENGTH) {
                return new Blob(agent_, source);
            }
            return new Blob(agent_, source, length);
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.io.InputStream", Types.getTypeString(targetType));
        }
    }

    // create a byte[] by reading all of the bytes from inputStream
    private final byte[] setBytesFromStream(java.io.InputStream is, int length) throws SqlException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        int totalRead = 0;

        try {
            int read = is.read();
            while (read != -1) {
                totalRead++;
                baos.write(read);
                read = is.read();
            }

            if (length != CrossConverters.UNKNOWN_LENGTH &&
                    length != totalRead) {
                throw new SqlException(agent_.logWriter_,
                		new ClientMessageId (SQLState.READER_UNDER_RUN));
            }
        } catch (java.io.IOException e) {
            throw SqlException.javaException(agent_.logWriter_, e);
        }
        return baos.toByteArray();
    }

    // Convert from Clob source to target type
    // In support of PS.setClob()
    final Object setObject(int targetType, java.sql.Clob source) throws SqlException {
        switch (targetType) {
        case Types.CLOB:
            return source;
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Clob", Types.getTypeString(targetType));
        }
    }

    // The Java compiler uses static binding, so we can't rely on the strongly
    // typed setObject() methods above for each of the Java Object instance types.
    final Object setObject(int targetType, Object source) throws SqlException {
        if (source == null) {
            return null;
        } else if (source instanceof Boolean) {
            return setObject(targetType, ((Boolean) source).booleanValue());
        } else if (source instanceof Integer) {
            return setObject(targetType, ((Integer) source).intValue());
        } else if (source instanceof Long) {
            return setObject(targetType, ((Long) source).longValue());
        } else if (source instanceof Float) {
            return setObject(targetType, ((Float) source).floatValue());
        } else if (source instanceof Double) {
            return setObject(targetType, ((Double) source).doubleValue());
        } else if (source instanceof java.math.BigDecimal) {
            return setObject(targetType, (java.math.BigDecimal) source);
        } else if (source instanceof java.sql.Date) {
            return setObject(targetType, (java.sql.Date) source);
        } else if (source instanceof java.sql.Time) {
            return setObject(targetType, (java.sql.Time) source);
        } else if (source instanceof java.sql.Timestamp) {
            return setObject(targetType, (java.sql.Timestamp) source);
        } else if (source instanceof String) {
            return setObject(targetType, (String) source);
        } else if (source instanceof byte[]) {
            return setObject(targetType, (byte[]) source);
        } else if (source instanceof java.sql.Blob) {
            return setObject(targetType, (java.sql.Blob) source);
        } else if (source instanceof java.sql.Clob) {
            return setObject(targetType, (java.sql.Clob) source);
        } else if (source instanceof java.sql.Array) {
            return setObject(targetType, (java.sql.Array) source);
        } else if (source instanceof java.sql.Ref) {
            return setObject(targetType, (java.sql.Ref) source);
        } else if (source instanceof Short) {
            return setObject(targetType, ((Short) source).shortValue());
        } else if (source instanceof Byte) {
            return setObject(targetType, ((Byte) source).byteValue());
        } else {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                source.getClass().getName(), Types.getTypeString(targetType));
        }
    }

    // move all these to Cursor and rename to crossConvertFrom*To*()
    // ---------------------------------------------------------------------------
    // The following methods are used for output cross conversion.
    // ---------------------------------------------------------------------------

    //---------------------------- getBoolean*() methods -------------------------

    final boolean getBooleanFromByte(byte source) throws SqlException {
        return source != 0;
    }

    final boolean getBooleanFromShort(short source) throws SqlException {
        return source != 0;
    }

    final boolean getBooleanFromInt(int source) throws SqlException {
        return source != 0;
    }

    final boolean getBooleanFromLong(long source) throws SqlException {
        return source != 0;
    }

    final boolean getBooleanFromFloat(float source) throws SqlException {
        return source != 0;
    }

    final boolean getBooleanFromDouble(double source) throws SqlException {
        return source != 0;
    }

    final boolean getBooleanFromBigDecimal(java.math.BigDecimal source) throws SqlException {
        return source.intValue() != 0;
    }

    // See differences.html for DNC getBoolean() semantics.
    final boolean getBooleanFromString(String source) throws SqlException {
        return !(source.trim().equals("0") || source.trim().equals("false"));
    }

    //---------------------------- getByte*() methods ----------------------------

    final byte getByteFromShort(short source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Byte.MAX_VALUE || source < java.lang.Byte.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (byte) source;
    }

    final byte getByteFromInt(int source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Byte.MAX_VALUE || source < java.lang.Byte.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (byte) source;
    }

    final byte getByteFromLong(long source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Byte.MAX_VALUE || source < java.lang.Byte.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (byte) source;
    }

    final byte getByteFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Byte.MAX_VALUE || source < java.lang.Byte.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (byte) source;
    }

    final byte getByteFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Byte.MAX_VALUE || source < java.lang.Byte.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (byte) source;
    }

    final byte getByteFromBigDecimal(java.math.BigDecimal source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source.compareTo(bdMaxByteValue__) == 1 || source.compareTo(bdMinByteValue__) == -1)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }
        return (byte) source.intValue();
    }

    final byte getByteFromBoolean(boolean source) throws SqlException {
        return source ? (byte) 1 : (byte) 0;
    }

    final byte getByteFromString(String source) throws SqlException {
        try {
            return parseByte(source);
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId 
            		(SQLState.LANG_FORMAT_EXCEPTION), "byte", e);
        }
    }

    //---------------------------- getShort*() methods ---------------------------

    final short getShortFromInt(int source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Short.MAX_VALUE || source < java.lang.Short.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (short) source;
    }

    final short getShortFromLong(long source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Short.MAX_VALUE || source < java.lang.Short.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (short) source;
    }

    final short getShortFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Short.MAX_VALUE || source < java.lang.Short.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (short) source;
    }

    final short getShortFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Short.MAX_VALUE || source < java.lang.Short.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (short) source;
    }

    final short getShortFromBigDecimal(java.math.BigDecimal source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source.compareTo(bdMaxShortValue__) == 1 || source.compareTo(bdMinShortValue__) == -1)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }
        return (short) source.intValue();
    }

    final short getShortFromBoolean(boolean source) throws SqlException {
        return source ? (short) 1 : (short) 0;
    }

    final short getShortFromString(String source) throws SqlException {
        try {
            return parseShort(source);
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId 
            		(SQLState.LANG_FORMAT_EXCEPTION), 
            		"short", e);
        }
    }

    //---------------------------- getInt*() methods -----------------------------

    final int getIntFromLong(long source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Integer.MAX_VALUE || source < java.lang.Integer.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (int) source;
    }

    final int getIntFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Integer.MAX_VALUE || source < java.lang.Integer.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (int) source;
    }

    final int getIntFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Integer.MAX_VALUE || source < java.lang.Integer.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (int) source;
    }

    final int getIntFromBigDecimal(java.math.BigDecimal source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source.compareTo(bdMaxIntValue__) == 1 || source.compareTo(bdMinIntValue__) == -1)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }
        return source.intValue();
    }

    final int getIntFromBoolean(boolean source) throws SqlException {
        return source ? (int) 1 : (int) 0;
    }

    final int getIntFromString(String source) throws SqlException {
        try {
            return parseInt(source);
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
            		"int", e);
        }
    }

    //---------------------------- getLong*() methods ----------------------------

    final long getLongFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Long.MAX_VALUE || source < java.lang.Long.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (long) source;
    }

    final long getLongFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > java.lang.Long.MAX_VALUE || source < java.lang.Long.MIN_VALUE)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (long) source;
    }

    final long getLongFromBigDecimal(java.math.BigDecimal source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source.compareTo(bdMaxLongValue__) == 1 || source.compareTo(bdMinLongValue__) == -1)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }
        return source.longValue();
    }

    final long getLongFromBoolean(boolean source) throws SqlException {
        return source ? (long) 1 : (long) 0;
    }

    final long getLongFromString(String source) throws SqlException {
        try {
            return parseLong(source);
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
        			"long", e);
        }
    }

    //---------------------------- getFloat*() methods ---------------------------

    final float getFloatFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                Float.isInfinite((float)source)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }

        return (float) source;
    }

    final float getFloatFromBigDecimal(java.math.BigDecimal source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source.compareTo(bdMaxFloatValue__) == 1 || source.compareTo(bdMinFloatValue__) == -1)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }
        return source.floatValue();
    }

    final float getFloatFromBoolean(boolean source) throws SqlException {
        return source ? (float) 1 : (float) 0;
    }

    final float getFloatFromString(String source) throws SqlException {
        try {
            return Float.parseFloat(source.trim());
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    "float", e);
        }
    }

    //---------------------------- getDouble*() methods --------------------------

    final double getDoubleFromBigDecimal(java.math.BigDecimal source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source.compareTo(bdMaxDoubleValue__) == 1 || source.compareTo(bdMinDoubleValue__) == -1)) {
            throw new LossOfPrecisionConversionException(agent_.logWriter_, String.valueOf(source));
        }
        return source.doubleValue();
    }

    final double getDoubleFromBoolean(boolean source) throws SqlException {
        return source ? (double) 1 : (double) 0;
    }

    final double getDoubleFromString(String source) throws SqlException {
        try {
            return Double.parseDouble(source.trim());
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    "double", e);
        }
    }

    //---------------------------- getBigDecimal*() methods ----------------------

    final java.math.BigDecimal getBigDecimalFromBoolean(boolean source) throws SqlException {
        return source ? bdOne__ : bdZero__;
    }

    final java.math.BigDecimal getBigDecimalFromString(String source) throws SqlException {
        try {
            // Unfortunately, the big decimal constructor calls java.lang.Long.parseLong(),
            // which doesn't like spaces, so we have to call trim() to get rid of the spaces from CHAR columns.
            return new java.math.BigDecimal(source.trim());
        } catch (java.lang.NumberFormatException e) {
            throw new SqlException(agent_.logWriter_,
            		new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    "java.math.BigDecimal", e);
        }
    }

    //---------------------------- getString*() methods --------------------------

    final String getStringFromBoolean(boolean source) throws SqlException {
        return source ? "1" : "0";
    }

    final String getStringFromBytes(byte[] bytes) throws SqlException {
        StringBuffer stringBuffer = new StringBuffer(bytes.length * 2);
        for (int i = 0; i < bytes.length; i++) {
            String hexForByte = Integer.toHexString(bytes[i] & 0xff);
            // If the byte is x0-F, prepend a "0" in front to ensure 2 char representation
            if (hexForByte.length() == 1) {
                stringBuffer.append('0');
            }
            stringBuffer.append(hexForByte);
        }
        return stringBuffer.toString();
    }


    // All Numeric, and Date/Time types use String.valueOf (source)

    //---------------------------- getDate*() methods ----------------------------

    final Date getDateFromString(String source, Calendar cal)
            throws SqlException {
        try {
            return date_valueOf(source, cal);
        } catch (java.lang.IllegalArgumentException e) { // subsumes NumberFormatException
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.LANG_DATE_SYNTAX_EXCEPTION), e);
        }
    }

    final java.sql.Date getDateFromTime(java.sql.Time source) throws SqlException {
        return new java.sql.Date(source.getTime());
    }

    final java.sql.Date getDateFromTimestamp(java.sql.Timestamp source) throws SqlException {
        return new java.sql.Date(source.getTime());
    }

    //---------------------------- getTime*() methods ----------------------------

    final Time getTimeFromString(String source, Calendar cal)
            throws SqlException {
        try {
            return time_valueOf(source, cal);
        } catch (java.lang.IllegalArgumentException e) { // subsumes NumberFormatException
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.LANG_DATE_SYNTAX_EXCEPTION), e);
        }
    }

    final java.sql.Time getTimeFromTimestamp(java.sql.Timestamp source) throws SqlException {
        return new java.sql.Time(source.getTime());
    }

    //---------------------------- getTimestamp*() methods -----------------------

    final Timestamp getTimestampFromString(String source, Calendar cal)
            throws SqlException {
        try {
            return timestamp_valueOf(source, cal);
        } catch (java.lang.IllegalArgumentException e) {  // subsumes NumberFormatException
            throw new SqlException(agent_.logWriter_, 
            		new ClientMessageId (SQLState.LANG_DATE_SYNTAX_EXCEPTION), e);
        }
    }

    final java.sql.Timestamp getTimestampFromTime(java.sql.Time source) throws SqlException {
        return new java.sql.Timestamp(source.getTime());
    }

    final java.sql.Timestamp getTimestampFromDate(java.sql.Date source) throws SqlException {
        return new java.sql.Timestamp(source.getTime());
    }

    /**
     * Convert a string to a date in the specified calendar. Accept the same
     * format as {@code java.sql.Date.valueOf()}.
     *
     * @param s the string to parse
     * @param cal the calendar (or null to use the default calendar)
     * @return a {@code java.sql.Date} value that represents the date in the
     * calendar {@code cal}
     * @throws IllegalArgumentException if the format of the string is invalid
     */
    final Date date_valueOf(String s, Calendar cal) {
        String formatError = "JDBC Date format must be yyyy-mm-dd";
        if (s == null) {
            throw new java.lang.IllegalArgumentException(formatError);
        }
        s = s.trim();

        if (cal == null) {
            return Date.valueOf(s);
        }

        cal.clear();
        initDatePortion(cal, s);

        // Normalize time components as specified by java.util.Date.
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return new Date(cal.getTimeInMillis());
    }

    /**
     * Initialize the date components of a {@code java.util.Calendar} from
     * a string on the format YYYY-MM-DD. All other components are left
     * untouched.
     *
     * @param cal the calendar whose date components to initialize
     * @param date a string representing a date
     * @throws IllegalArgumentException if the date string is not on the
     * format YYYY-MM-DD
     */
    private static void initDatePortion(Calendar cal, String date) {

        // Expect string on format YYYY-MM-DD
        if (date.length() != 10 ||
                date.charAt(4) != '-' || date.charAt(7) != '-') {
            throw new IllegalArgumentException();
        }

        int year =
                digit(date.charAt(0)) * 1000 +
                digit(date.charAt(1)) * 100 +
                digit(date.charAt(2)) * 10 +
                digit(date.charAt(3));

        int month =
                digit(date.charAt(5)) * 10 +
                digit(date.charAt(6));

        int day =
                digit(date.charAt(8)) * 10 +
                digit(date.charAt(9));

        cal.set(year, month, day);
    }

    /**
     * Convert a character to a digit.
     *
     * @param ch the character
     * @return the corresponding digit (0-9)
     * @throws IllegalArgumentException if {@code ch} doesn't represent a digit
     */
    private static int digit(char ch) {
        int result = Character.digit(ch, 10);
        if (result == -1) {
            throw new IllegalArgumentException();
        }
        return result;
    }

    /**
     * Convert a string to a time in the specified calendar. Accept the same
     * format as {@code java.sql.Time.valueOf()}.
     *
     * @param s the string to parse
     * @param cal the calendar (or null to use the default calendar)
     * @return a {@code java.sql.Time} value that represents the time in the
     * calendar {@code cal}
     * @throws IllegalArgumentException if the format of the string is invalid
     */
    final Time time_valueOf(String s, Calendar cal) {
        String formatError = "JDBC Time format must be hh:mm:ss";
        if (s == null) {
            throw new java.lang.IllegalArgumentException();
        }
        s = s.trim();

        if (cal == null) {
            return Time.valueOf(s);
        }

        cal.clear();
        initTimePortion(cal, s);

        // Normalize date components as specified by java.sql.Time.
        cal.set(1970, Calendar.JANUARY, 1);

        return new Time(cal.getTimeInMillis());
    }

    /**
     * Initialize the time components of a {@code java.util.Calendar} from a
     * string on the format HH:MM:SS. All other components are left untouched.
     *
     * @param cal the calendar whose time components to initialize
     * @param time a string representing a time
     * @throws IllegalArgumentException if the time string is not on the
     * format HH:MM:SS
     */
    private void initTimePortion(Calendar cal, String time) {
        // Expect string on format HH:MM:SS
        if (time.length() != 8 ||
                time.charAt(2) != ':' || time.charAt(5) != ':') {
            throw new IllegalArgumentException();
        }

        int hour = digit(time.charAt(0)) * 10 + digit(time.charAt(1));
        int minute = digit(time.charAt(3)) * 10 + digit(time.charAt(4));
        int second = digit(time.charAt(6)) * 10 + digit(time.charAt(7));

        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
    }

    /**
     * Convert a string to a timestamp in the specified calendar. Accept the
     * same format as {@code java.sql.Timestamp.valueOf()}.
     *
     * @param s the string to parse
     * @param cal the calendar (or null to use the default calendar)
     * @return a {@code java.sql.Timestamp} value that represents the timestamp
     * in the calendar {@code cal}
     * @throws IllegalArgumentException if the format of the string is invalid
     */
    final Timestamp timestamp_valueOf(String s, Calendar cal) {
        String formatError = "JDBC Timestamp format must be yyyy-mm-dd hh:mm:ss.fffffffff";
        if (s == null) {
            throw new java.lang.IllegalArgumentException();
        }

        s = s.trim();

        if (cal == null) {
            return Timestamp.valueOf(s);
        }

        cal.clear();

        // Split into date and time components
        String[] dateAndTime = s.split(" ");
        if (dateAndTime.length != 2) {
            throw new IllegalArgumentException();
        }

        String dateString = dateAndTime[0];
        String timeAndNanoString = dateAndTime[1];

        initDatePortion(cal, dateString);

        // Split the time and nano components. The nano component is optional,
        // and is separated from the time component with a decimal point.
        String[] timeAndNanos = timeAndNanoString.split("\\.");
        if (timeAndNanos.length < 1 || timeAndNanos.length > 2) {
            throw new IllegalArgumentException();
        }

        String timeString = timeAndNanos[0];

        initTimePortion(cal, timeString);

        int nanos = 0;
        if (timeAndNanos.length > 1) {
            String nanoString = timeAndNanos[1];
            int extraZeros = 9 - nanoString.length();
            if (extraZeros < 0) {
                throw new IllegalArgumentException();
            }
            // parseInt() may throw NumberFormatException. NFE is a subclass
            // of IllegalArgumentException, so no need to document separately
            // in the javadoc.
            nanos = Integer.parseInt(nanoString);
            for (int i = 0; i < extraZeros; i++) {
                nanos *= 10;
            }
        }

        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(nanos);
        return ts;
    }

    private final byte parseByte(String s) throws NumberFormatException {
        int i = parseInt(s);
        if (i < Byte.MIN_VALUE || i > Byte.MAX_VALUE) {
            throw new NumberFormatException();
        }
        return (byte) i;
    }

    private final short parseShort(String s) throws NumberFormatException {
        int i = parseInt(s);
        if (i < Short.MIN_VALUE || i > Short.MAX_VALUE) {
            throw new NumberFormatException();
        }
        return (short) i;
    }

    // Custom version of java.lang.parseInt() that allows for space padding of char fields.
    private final int parseInt(String s) throws NumberFormatException {
        if (s == null) {
            throw new NumberFormatException("null");
        }

        int result = 0;
        boolean negative = false;
        int i = 0;
        int max = s.length();
        int limit;
        int multmin;
        int digit;

        if (max == 0) {
            throw new NumberFormatException(s);
        }

        if (s.charAt(0) == '-') {
            negative = true;
            limit = Integer.MIN_VALUE;
            i++;
        } else {
            limit = -Integer.MAX_VALUE;
        }
        multmin = limit / 10;
        // Special handle the first digit to get things started.
        if (i < max) {
            digit = Character.digit(s.charAt(i++), 10);
            if (digit < 0) {
                throw new NumberFormatException(s);
            } else {
                result = -digit;
            }
        }
        // Now handle all the subsequent digits or space padding.
        while (i < max) {
            char c = s.charAt(i++);
            if (c == ' ') {
                skipPadding(s, i, max);
                break;
            }
            // Accumulating negatively avoids surprises near MAX_VALUE
            digit = Character.digit(c, 10);
            if (digit < 0) {
                throw new NumberFormatException(s);
            }
            if (result < multmin) {
                throw new NumberFormatException(s);
            }
            result *= 10;
            if (result < limit + digit) {
                throw new NumberFormatException(s);
            }
            result -= digit;
        }
        if (negative) {
            if (i > 1) {
                return result;
            } else { // Only got "-"
                throw new NumberFormatException(s);
            }
        } else {
            return -result;
        }
    }

    private final long parseLong(String s) throws NumberFormatException {
        if (s == null) {
            throw new NumberFormatException("null");
        }

        long result = 0;
        boolean negative = false;
        int i = 0, max = s.length();
        long limit;
        long multmin;
        int digit;

        if (max == 0) {
            throw new NumberFormatException(s);
        }

        if (s.charAt(0) == '-') {
            negative = true;
            limit = Long.MIN_VALUE;
            i++;
        } else {
            limit = -Long.MAX_VALUE;
        }
        multmin = limit / 10;
        if (i < max) {
            digit = Character.digit(s.charAt(i++), 10);
            if (digit < 0) {
                throw new NumberFormatException(s);
            } else {
                result = -digit;
            }
        }
        while (i < max) {
            char c = s.charAt(i++);
            if (c == ' ') {
                skipPadding(s, i, max);
                break;
            }
            // Accumulating negatively avoids surprises near MAX_VALUE
            digit = Character.digit(c, 10);
            if (digit < 0) {
                throw new NumberFormatException(s);
            }
            if (result < multmin) {
                throw new NumberFormatException(s);
            }
            result *= 10;
            if (result < limit + digit) {
                throw new NumberFormatException(s);
            }
            result -= digit;
        }
        if (negative) {
            if (i > 1) {
                return result;
            } else {	// Only got "-"
                throw new NumberFormatException(s);
            }
        } else {
            return -result;
        }
    }

    private final void skipPadding(String s, int i, int length) throws NumberFormatException {
        while (i < length) {
            if (s.charAt(i++) != ' ') {
                throw new NumberFormatException(s);
            }
        }
    }
}
