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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Locale;
import org.apache.derby.shared.common.reference.SQLState;

// All currently supported derby types are mapped to one of the following jdbc types:
// Types.SMALLINT;
// Types.INTEGER;
// Types.BIGINT;
// Types.REAL;
// Types.DOUBLE;
// Types.DECIMAL;
// Types.DATE;
// Types.TIME;
// Types.TIMESTAMP;
// Types.CHAR;
// Types.VARCHAR;
// Types.LONGVARCHAR;
// Types.CLOB;
// Types.BLOB;
//

final class CrossConverters {

    /**
     * Value used to signal unknown length of data.
     */
    public static final int UNKNOWN_LENGTH = Integer.MIN_VALUE;

    private final static BigDecimal bdMaxByteValue__ =
            BigDecimal.valueOf(Byte.MAX_VALUE);
    private final static BigDecimal bdMinByteValue__ =
            BigDecimal.valueOf(Byte.MIN_VALUE);
    private final static BigDecimal bdMaxShortValue__ =
            BigDecimal.valueOf(Short.MAX_VALUE);
    private final static BigDecimal bdMinShortValue__ =
            BigDecimal.valueOf(Short.MIN_VALUE);
    private final static BigDecimal bdMaxIntValue__ =
            BigDecimal.valueOf(Integer.MAX_VALUE);
    private final static BigDecimal bdMinIntValue__ =
            BigDecimal.valueOf(Integer.MIN_VALUE);
    private final static BigDecimal bdMaxLongValue__ =
            BigDecimal.valueOf(Long.MAX_VALUE);
    private final static BigDecimal bdMinLongValue__ =
            BigDecimal.valueOf(Long.MIN_VALUE);
    private final static BigDecimal bdMaxFloatValue__ =
            new BigDecimal(Float.MAX_VALUE);
    private final static BigDecimal bdMinFloatValue__ =
            new BigDecimal(-Float.MAX_VALUE);
    private final static BigDecimal bdMaxDoubleValue__ =
            new BigDecimal(Double.MAX_VALUE);
    private final static BigDecimal bdMinDoubleValue__ =
            new BigDecimal(-Double.MAX_VALUE);

    // Since BigDecimals are immutable, we can return pointers to these canned 0's and 1's.
    private final static BigDecimal bdZero__ = BigDecimal.valueOf(0);
    private final static BigDecimal bdOne__ = BigDecimal.valueOf(1);

    // ---------------------- state ----------------------------------------------

    private final Agent agent_;

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
    final Object setObject(int targetType, boolean source) throws SqlException {
        short numVal = source ? (short) 1 : 0;
        switch (targetType) {
        case ClientTypes.BIT:
        case ClientTypes.BOOLEAN:
            return Boolean.valueOf(source);

        case ClientTypes.SMALLINT:
            return Short.valueOf(numVal);

        case ClientTypes.INTEGER:
            return Integer.valueOf(numVal);

        case ClientTypes.BIGINT:
            return Long.valueOf(numVal);

        case ClientTypes.REAL:
            return Float.valueOf(numVal);

        case ClientTypes.DOUBLE:
            return Double.valueOf(numVal);

        case ClientTypes.DECIMAL:
            return BigDecimal.valueOf(numVal);

        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "boolean", ClientTypes.getTypeString(targetType));
        }
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
        case ClientTypes.BIT:
        case ClientTypes.BOOLEAN:
            return Boolean.valueOf(source != 0);

        case ClientTypes.SMALLINT:
            return Short.valueOf(source);

        case ClientTypes.INTEGER:
            return Integer.valueOf(source);

        case ClientTypes.BIGINT:
            return Long.valueOf(source);

        case ClientTypes.REAL:
            return Float.valueOf(source);

        case ClientTypes.DOUBLE:
            return Double.valueOf(source);

        case ClientTypes.DECIMAL:
            return BigDecimal.valueOf(source);

        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "byte", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from integer source to target type
    // In support of PS.setInt()
    final Object setObject(int targetType, int source) throws SqlException {
        switch (targetType) {
        case ClientTypes.BIT:
        case ClientTypes.BOOLEAN:
            return Boolean.valueOf(source != 0);

        case ClientTypes.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "SMALLINT");
            }
            return Short.valueOf((short) source);

        case ClientTypes.INTEGER:
            return Integer.valueOf(source);

        case ClientTypes.BIGINT:
            return Long.valueOf(source);

        case ClientTypes.REAL:
            return Float.valueOf(source);

        case ClientTypes.DOUBLE:
            return Double.valueOf(source);

        case ClientTypes.DECIMAL:
            return BigDecimal.valueOf(source);

        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                    "int", ClientTypes.getTypeString(targetType));
        }
    }

    // This method is used in lieu of setObject(targetType, sourceObject) because we
    // don't support the BIT/BOOLEAN as underlying DERBY targetTypes.
    final boolean setBooleanFromObject(Object source, int sourceType) throws SqlException {
        switch (sourceType) {
        case ClientTypes.SMALLINT:
            return getBooleanFromShort(((Short) source).shortValue());
        case ClientTypes.INTEGER:
            return getBooleanFromInt(((Integer) source).intValue());
        case ClientTypes.BIGINT:
            return getBooleanFromLong(((BigInteger) source).longValue());
        case ClientTypes.REAL:
            return getBooleanFromFloat(((Float) source).floatValue());
        case ClientTypes.DOUBLE:
            return getBooleanFromDouble(((Double) source).doubleValue());
        case ClientTypes.DECIMAL:
            return getBooleanFromLong(((BigDecimal) source).longValue());
        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return getBooleanFromString((String) source);
        default:
            throw new ColumnTypeConversionException(agent_.logWriter_,
                ClientTypes.getTypeString(sourceType), "boolean");
        }
    }

    // This method is used in lieu of setObject(targetType, sourceObject) because we
    // don't support the BIT/BOOLEAN as underlying DERBY targetTypes.
    final byte setByteFromObject(Object source, int sourceType) throws SqlException {
        switch (sourceType) {
        case ClientTypes.SMALLINT:
            return getByteFromShort(((Short) source).shortValue());
        case ClientTypes.INTEGER:
            return getByteFromInt(((Integer) source).intValue());
        case ClientTypes.BIGINT:
            return getByteFromLong(((BigInteger) source).longValue());
        case ClientTypes.REAL:
            return getByteFromFloat(((Float) source).floatValue());
        case ClientTypes.DOUBLE:
            return getByteFromDouble(((Double) source).doubleValue());
        case ClientTypes.DECIMAL:
            return getByteFromLong(((BigDecimal) source).longValue());
        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return getByteFromString((String) source);
        default:
            throw new ColumnTypeConversionException(agent_.logWriter_,
                ClientTypes.getTypeString(sourceType), "byte");
        }
    }

    // Convert from long source to target type
    // In support of PS.setLong()
    final Object setObject(int targetType, long source) throws SqlException {
        switch (targetType) {
        case ClientTypes.BIT:
        case ClientTypes.BOOLEAN:
            return Boolean.valueOf(source != 0);

        case ClientTypes.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "SMALLINT");
            }
            return Short.valueOf((short) source);

        case ClientTypes.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "INTEGER");
            }
            return Integer.valueOf((int) source);

        case ClientTypes.BIGINT:
            return Long.valueOf(source);

        case ClientTypes.REAL:
            return Float.valueOf(source);

        case ClientTypes.DOUBLE:
            return Double.valueOf(source);

        case ClientTypes.DECIMAL:
            return BigDecimal.valueOf(source);

        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "long", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from floating point source to target type
    // In support of PS.setFloat()
    final Object setObject(int targetType, float source) throws SqlException {
        switch (targetType) {
        case ClientTypes.BIT:
        case ClientTypes.BOOLEAN:
            return Boolean.valueOf(source != 0);

        case ClientTypes.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "SMALLINT");
            }
            return Short.valueOf((short) source);

        case ClientTypes.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "INTEGER");
            }
            return Integer.valueOf((int) source);

        case ClientTypes.BIGINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Long.MAX_VALUE || source < Long.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "BIGINT");
            }
            return Long.valueOf((long) source);

        case ClientTypes.REAL:
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
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "REAL");
            }
            return Float.valueOf(source);

        case ClientTypes.DOUBLE:
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
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "DOUBLE");
            }
            // source passed in is a float, do we need to check if the source already contains "infinity"??
            // Convert to Double via String to avoid changing the precision,
            // which may happen if we cast float to double.
            return Double.valueOf(String.valueOf(source));

        case ClientTypes.DECIMAL:
            // Can't use the following commented out line because it changes precision of the result.
            //return new java.math.BigDecimal (source);
            // This matches derby semantics
            return new BigDecimal(String.valueOf(source));

        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "float", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from double floating point source to target type
    // In support of PS.setDouble()
    final Object setObject(int targetType, double source) throws SqlException {
        switch (targetType) {
        case ClientTypes.BIT:
        case ClientTypes.BOOLEAN:
            return Boolean.valueOf(source != 0);

        case ClientTypes.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "SMALLINT");
            }
            return Short.valueOf((short) source);

        case ClientTypes.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "INTEGER");
            }
            return Integer.valueOf((int) source);

        case ClientTypes.BIGINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Long.MAX_VALUE || source < Long.MIN_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "BIGINT");
            }
            return Long.valueOf((long) source);

        case ClientTypes.REAL:
            if (Configuration.rangeCheckCrossConverters &&
                    (source > Float.MAX_VALUE || source < -Float.MAX_VALUE)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "REAL");
            }
            return Float.valueOf((float) source);

        case ClientTypes.DOUBLE:
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
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "DOUBLE");
            }
            return Double.valueOf(source);

        case ClientTypes.DECIMAL:
            // Use BigDecimal.valueOf(source) instead of new BigDecimal(source),
            // as the latter may change the precision.
            return BigDecimal.valueOf(source);
        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "double", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from big decimal source to target type
    // In support of PS.setBigDecimal()
    final Object setObject(int targetType, BigDecimal source)
            throws SqlException {
        switch (targetType) {
        case ClientTypes.BIT:
        case ClientTypes.BOOLEAN:
            return Boolean.valueOf(
                    BigDecimal.valueOf(0L).compareTo(source) != 0);

        case ClientTypes.SMALLINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxShortValue__) == 1 || source.compareTo(bdMinShortValue__) == -1)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "SMALLINT");
            }
            return Short.valueOf(source.shortValue());

        case ClientTypes.INTEGER:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxIntValue__) == 1 || source.compareTo(bdMinIntValue__) == -1)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "INTEGER");
            }
            return Integer.valueOf(source.intValue());

        case ClientTypes.BIGINT:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxLongValue__) == 1 || source.compareTo(bdMinLongValue__) == -1)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "BIGINT");
            }
            return Long.valueOf(source.longValue());

        case ClientTypes.REAL:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxFloatValue__) == 1 || source.compareTo(bdMinFloatValue__) == -1)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "REAL");
            }
            return Float.valueOf(source.floatValue());

        case ClientTypes.DOUBLE:
            if (Configuration.rangeCheckCrossConverters &&
                    (source.compareTo(bdMaxDoubleValue__) == 1 || source.compareTo(bdMinDoubleValue__) == -1)) {
                throw new OutsideRangeForDataTypeException(
                    agent_.logWriter_, "DOUBLE");
            }
            return Double.valueOf(source.doubleValue());

        case ClientTypes.DECIMAL:
            return source;

        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.Math.BigDecimal", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from date source to target type
    // In support of PS.setDate()
    final Object setObject(int targetType, Date source) throws SqlException {
        switch (targetType) {

        case Types.DATE:
            return source;

        case Types.TIMESTAMP:
            return new Timestamp(source.getTime());

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Date", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from time source to target type
    // In support of PS.setTime()
    final Object setObject(int targetType, Time source) throws SqlException {
        switch (targetType) {

        case Types.TIME:
            return source;

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Time", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from date source to target type
    // In support of PS.setTimestamp()
    final Object setObject(int targetType, Timestamp source)
            throws SqlException {
        switch (targetType) {

        case Types.TIMESTAMP:
            return source;

        case Types.TIME:
            return new Time(source.getTime());

        case Types.DATE:
            return new Date(source.getTime());

        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return String.valueOf(source);

        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Timestamp", ClientTypes.getTypeString(targetType));
        }
    }

    // setString() against BINARY columns cannot be implemented consistently because w/out metadata, we'll send char encoding bytes.
    // So we refuse setString() requests altogether.
    // Convert from string source to target type.
    // In support of PS.setString()
    final Object setObject(int targetDriverType, String source) throws SqlException {
        try {
            switch (targetDriverType) {
            case ClientTypes.BIT:
            case ClientTypes.BOOLEAN:
            {
                String cleanSource = source.trim().toUpperCase(Locale.ENGLISH);
                if (cleanSource.equals("UNKNOWN")) {
                    return null;
                } else if (cleanSource.equals("TRUE")) {
                    return Boolean.TRUE;
                } else if (cleanSource.equals("FALSE")) {
                    return Boolean.FALSE;
                } else {
                    throw new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.LANG_FORMAT_EXCEPTION),
                        ClientTypes.getTypeString(targetDriverType));
                }
            }

            case ClientTypes.SMALLINT:
                return Short.valueOf(source);

            case ClientTypes.INTEGER:
                return Integer.valueOf(source);

            case ClientTypes.BIGINT:
                return Long.valueOf(source);

            case ClientTypes.REAL:
                return Float.valueOf(source);

            case ClientTypes.DOUBLE:
                return Double.valueOf(source);

            case ClientTypes.DECIMAL:
                return new BigDecimal(source);

            case Types.DATE:
                return date_valueOf(source, null);

            case Types.TIME:
                return time_valueOf(source, null);

            case Types.TIMESTAMP:
                return timestamp_valueOf(source, null);

            case ClientTypes.CHAR:
            case ClientTypes.VARCHAR:
            case ClientTypes.LONGVARCHAR:
                return source;

            case ClientTypes.CLOB:
                return new ClientClob(agent_, source);

                // setString() against BINARY columns is problematic because w/out metadata, we'll send char encoding bytes.
                // So we refuse setString() requests altogether.
            case ClientTypes.BINARY:
            case ClientTypes.VARBINARY:
            case ClientTypes.LONGVARBINARY:
            case ClientTypes.BLOB:
            default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "String", ClientTypes.getTypeString(targetDriverType));
            }
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId 
                    (SQLState.LANG_FORMAT_EXCEPTION), 
                    e,
                    ClientTypes.getTypeString(targetDriverType));
        }
    }

    // ------ method to convert to targetJdbcType ------
    /**
     * Convert the input targetJdbcType to the correct JdbcType used by CrossConverters.
     */
    public static int getInputJdbcType(int jdbcType) {
        switch (jdbcType) {
        case Types.TINYINT:
        case Types.SMALLINT:
            return Types.INTEGER;
        case Types.NUMERIC:
            return Types.DECIMAL;
        case Types.FLOAT:
            return Types.DOUBLE;
        default:
            return jdbcType;
        }

    }


    // -- methods in support of setObject(String)/getString() on BINARY columns---


    // Convert from byte[] source to target type
    // In support of PS.setBytes()
    final Object setObject(int targetType, byte[] source) throws SqlException {
        switch (targetType) {
        case ClientTypes.BINARY:
        case ClientTypes.VARBINARY:
        case ClientTypes.LONGVARBINARY:
            return source;
        case ClientTypes.BLOB:
            return new ClientBlob(source, agent_, 0);
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "byte[]", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from Reader source to target type
    // In support of PS.setCharacterStream()
    final Object setObject(int targetType, Reader source, int length)
            throws SqlException {
        switch (targetType) {
        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return setStringFromReader(source, length);
        case ClientTypes.CLOB:
            if (length == CrossConverters.UNKNOWN_LENGTH) {
                return new ClientClob(agent_, source);
            }
            return new ClientClob(agent_, source, length);
            // setCharacterStream() against BINARY columns is problematic because w/out metadata, we'll send char encoding bytes.
            // There's no clean solution except to just not support setObject(String/Reader/Stream)
        case ClientTypes.BINARY:
        case ClientTypes.VARBINARY:
        case ClientTypes.LONGVARBINARY:
        case ClientTypes.BLOB:
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.io.Reader", ClientTypes.getTypeString(targetType));
        }
    }

    // create a String by reading all of the bytes from reader
    private String setStringFromReader(Reader r, int length)
            throws SqlException {
        StringWriter sw = new StringWriter();
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
        } catch (IOException e) {
            throw SqlException.javaException(agent_.logWriter_, e);
        }
    }

    // Convert from InputStream source to target type.
    // In support of PS.setAsciiStream, PS.setUnicodeStream
    // Note: PS.setCharacterStream() is handled by setObject(Reader)
    final Object setObjectFromCharacterStream(
            int targetType,
            InputStream source,
            Charset encoding,
            int length) throws SqlException {

        switch (targetType) {
        case ClientTypes.CHAR:
        case ClientTypes.VARCHAR:
        case ClientTypes.LONGVARCHAR:
            return setStringFromStream(source, encoding, length);
        case ClientTypes.CLOB:
            if (length == CrossConverters.UNKNOWN_LENGTH) {
                return new ClientClob(agent_, source, encoding);
            }
            return new ClientClob(agent_, source, encoding, length);
        case ClientTypes.BINARY:
        case ClientTypes.VARBINARY:
        case ClientTypes.LONGVARBINARY:
        case ClientTypes.BLOB:
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.io.InputStream", ClientTypes.getTypeString(targetType));
        }
    }


    // create a String by reading all of the bytes from inputStream, applying encoding
    private String setStringFromStream(
            InputStream is,
            Charset encoding,
            int length) throws SqlException {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int totalRead = 0;

            try {
                int read = is.read();
                while (read != -1) {
                    totalRead++;
                    baos.write(read);
                    read = is.read();
                }
            } catch (IOException e) {
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.JAVA_EXCEPTION),
                    e, e.getClass().getName(), e.getMessage());
            }

            if (length != CrossConverters.UNKNOWN_LENGTH &&
                    length != totalRead) {
                throw new SqlException(agent_.logWriter_, 
                        new ClientMessageId (SQLState.READER_UNDER_RUN));
            }

            return new String(baos.toByteArray(), encoding);
    }

    // Convert from Blob source to target type
    // In support of PS.setBlob()
    final Object setObject(int targetType, Blob source) throws SqlException {
        switch (targetType) {
        case ClientTypes.BLOB:
            return source;
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Blob", ClientTypes.getTypeString(targetType));
        }
    }

    // Convert from InputStream source to target type
    // In support of PS.setBinaryStream()
    final Object setObjectFromBinaryStream(
            int targetType,
            InputStream source,
            int length) throws SqlException {

        switch (targetType) {
        case ClientTypes.BINARY:
        case ClientTypes.VARBINARY:
        case ClientTypes.LONGVARBINARY:
            return setBytesFromStream(source, length);
        case ClientTypes.BLOB:
            if (length == CrossConverters.UNKNOWN_LENGTH) {
                return new ClientBlob(agent_, source);
            }
            return new ClientBlob(agent_, source, length);
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.io.InputStream", ClientTypes.getTypeString(targetType));
        }
    }

    // create a byte[] by reading all of the bytes from inputStream
    private byte[] setBytesFromStream(InputStream is, int length)
            throws SqlException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
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
        } catch (IOException e) {
            throw SqlException.javaException(agent_.logWriter_, e);
        }
        return baos.toByteArray();
    }

    // Convert from Clob source to target type
    // In support of PS.setClob()
    final Object setObject(int targetType, Clob source) throws SqlException {
        switch (targetType) {
        case ClientTypes.CLOB:
            return source;
        default:
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId (SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                "java.sql.Clob", ClientTypes.getTypeString(targetType));
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
        } else if (source instanceof BigDecimal) {
            return setObject(targetType, (BigDecimal) source);
        } else if (source instanceof Date) {
            return setObject(targetType, (Date) source);
        } else if (source instanceof Time) {
            return setObject(targetType, (Time) source);
        } else if (source instanceof Timestamp) {
            return setObject(targetType, (Timestamp) source);
        } else if (source instanceof String) {
            return setObject(targetType, (String) source);
        } else if (source instanceof byte[]) {
            return setObject(targetType, (byte[]) source);
        } else if (source instanceof Blob) {
            return setObject(targetType, (Blob) source);
        } else if (source instanceof Clob) {
            return setObject(targetType, (Clob) source);
        } else if (source instanceof Array) {
            return setObject(targetType, (Array) source);
        } else if (source instanceof Ref) {
            return setObject(targetType, (Ref) source);
        } else if (source instanceof Short) {
            return setObject(targetType, ((Short) source).shortValue());
        } else if (source instanceof Byte) {
            return setObject(targetType, ((Byte) source).byteValue());
        } else if (source instanceof BigInteger) {
            return setObject(targetType,
                             new BigDecimal((BigInteger)source ));
        } else if (source instanceof java.util.Date) {
            return setObject(targetType,
                             new Timestamp(((java.util.Date)source).getTime()));
        } else if (source instanceof Calendar) {
            return setObject(targetType,
                             new Timestamp(((Calendar)source).getTime().
                                           getTime()));
        } else if (targetType == ClientTypes.JAVA_OBJECT) {
            return source;
        } else {
            throw new SqlException(
                agent_.logWriter_,
                new ClientMessageId(SQLState.LANG_DATA_TYPE_SET_MISMATCH),
                source.getClass().getName(),
                ClientTypes.getTypeString(targetType));
        }
    }

    // move all these to Cursor and rename to crossConvertFrom*To*()
    // ---------------------------------------------------------------------------
    // The following methods are used for output cross conversion.
    // ---------------------------------------------------------------------------

    //---------------------------- getBoolean*() methods -------------------------

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

    /**
     * <p>
     * Get a boolean value from a CHAR column. In order to match the embedded
     * driver and JCC we return false iff the CHAR value is "0" or "false".
     * </p>
     *
     * <p>
     * Leading and trailing whitespace is removed from the input string before
     * it's compared to "0" and "false". No other normalization is performed.
     * Specifically, no case conversion is performed, so the comparison is
     * case sensitive, and everything that doesn't exactly match "0" or "false"
     * will be considered true.
     * </p>
     *
     * @param source the value of a CHAR column
     * @return false if source is "0" or "false", true otherwise
     */
    final boolean getBooleanFromString(String source) {
        String trimmed = source.trim();
        return !(trimmed.equals("0") || trimmed.equals("false"));
    }

    //---------------------------- getByte*() methods ----------------------------

    final byte getByteFromShort(short source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Byte.MAX_VALUE || source < Byte.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "TINYINT");
        }

        return (byte) source;
    }

    final byte getByteFromInt(int source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Byte.MAX_VALUE || source < Byte.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "TINYINT");
        }

        return (byte) source;
    }

    final byte getByteFromLong(long source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Byte.MAX_VALUE || source < Byte.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "TINYINT");
        }

        return (byte) source;
    }

    final byte getByteFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Byte.MAX_VALUE || source < Byte.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "TINYINT");
        }

        return (byte) source;
    }

    final byte getByteFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Byte.MAX_VALUE || source < Byte.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "TINYINT");
        }

        return (byte) source;
    }

    final byte getByteFromBoolean(boolean source) throws SqlException {
        return source ? (byte) 1 : (byte) 0;
    }

    final byte getByteFromString(String source) throws SqlException {
        try {
            return parseByte(source);
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId 
                    (SQLState.LANG_FORMAT_EXCEPTION), e, "byte");
        }
    }

    //---------------------------- getShort*() methods ---------------------------

    final short getShortFromInt(int source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "SMALLINT");
        }

        return (short) source;
    }

    final short getShortFromLong(long source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "SMALLINT");
        }

        return (short) source;
    }

    final short getShortFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "SMALLINT");
        }

        return (short) source;
    }

    final short getShortFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Short.MAX_VALUE || source < Short.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "SMALLINT");
        }

        return (short) source;
    }

    final short getShortFromBoolean(boolean source) throws SqlException {
        return source ? (short) 1 : (short) 0;
    }

    final short getShortFromString(String source) throws SqlException {
        try {
            return parseShort(source);
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId 
                    (SQLState.LANG_FORMAT_EXCEPTION), 
                    e, "short");
        }
    }

    //---------------------------- getInt*() methods -----------------------------

    final int getIntFromLong(long source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "INTEGER");
        }

        return (int) source;
    }

    final int getIntFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "INTEGER");
        }

        return (int) source;
    }

    final int getIntFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Integer.MAX_VALUE || source < Integer.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "INTEGER");
        }

        return (int) source;
    }

    final int getIntFromBoolean(boolean source) throws SqlException {
        return source ? (int) 1 : (int) 0;
    }

    final int getIntFromString(String source) throws SqlException {
        try {
            return parseInt(source);
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    e, "int");
        }
    }

    //---------------------------- getLong*() methods ----------------------------

    final long getLongFromFloat(float source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Long.MAX_VALUE || source < Long.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "BIGINT");
        }

        return (long) source;
    }

    final long getLongFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                (source > Long.MAX_VALUE || source < Long.MIN_VALUE)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "BIGINT");
        }

        return (long) source;
    }

    final long getLongFromBoolean(boolean source) throws SqlException {
        return source ? (long) 1 : (long) 0;
    }

    final long getLongFromString(String source) throws SqlException {
        try {
            return parseLong(source);
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    e, "long");
        }
    }

    //---------------------------- getFloat*() methods ---------------------------

    final float getFloatFromDouble(double source) throws SqlException {
        if (Configuration.rangeCheckCrossConverters &&
                Float.isInfinite((float)source)) {
            throw new OutsideRangeForDataTypeException(
                agent_.logWriter_, "DOUBLE");
        }

        return (float) source;
    }

    final float getFloatFromBoolean(boolean source) throws SqlException {
        return source ? (float) 1 : (float) 0;
    }

    final float getFloatFromString(String source) throws SqlException {
        try {
            return Float.parseFloat(source.trim());
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    e, "float");
        }
    }

    //---------------------------- getDouble*() methods --------------------------

    final double getDoubleFromBoolean(boolean source) throws SqlException {
        return source ? (double) 1 : (double) 0;
    }

    final double getDoubleFromString(String source) throws SqlException {
        try {
            return Double.parseDouble(source.trim());
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    e, "double");
        }
    }

    //---------------------------- getBigDecimal*() methods ----------------------

    final BigDecimal getBigDecimalFromString(String source)
            throws SqlException {
        try {
            // Unfortunately, the big decimal constructor calls java.lang.Long.parseLong(),
            // which doesn't like spaces, so we have to call trim() to get rid of the spaces from CHAR columns.
            return new BigDecimal(source.trim());
        } catch (NumberFormatException e) {
            throw new SqlException(agent_.logWriter_,
                    new ClientMessageId (SQLState.LANG_FORMAT_EXCEPTION),
                    e, "java.math.BigDecimal");
        }
    }

    //---------------------------- getString*() methods --------------------------

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
        } catch (IllegalArgumentException e) { // subsumes NumberFormatException
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId (SQLState.LANG_DATE_SYNTAX_EXCEPTION), e);
        }
    }

    //---------------------------- getTime*() methods ----------------------------

    final Time getTimeFromString(String source, Calendar cal)
            throws SqlException {
        try {
            return time_valueOf(source, cal);
        } catch (IllegalArgumentException e) { // subsumes NumberFormatException
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId (SQLState.LANG_DATE_SYNTAX_EXCEPTION), e);
        }
    }

    //---------------------------- getTimestamp*() methods -----------------------

    final Timestamp getTimestampFromString(String source, Calendar cal)
            throws SqlException {
        try {
            return timestamp_valueOf(source, cal);
        } catch (IllegalArgumentException e) { // subsumes NumberFormatException
            throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId (SQLState.LANG_DATE_SYNTAX_EXCEPTION), e);
        }
    }

    /**
     * Convert a string to a date in the specified calendar. Accept the same
     * format as {@code Date.valueOf()}.
     *
     * @param s the string to parse
     * @param cal the calendar (or null to use the default calendar)
     * @return a {@code Date} value that represents the date in the
     * calendar {@code cal}
     * @throws IllegalArgumentException if the format of the string is invalid
     */
    private static Date date_valueOf(String s, Calendar cal) {
        String formatError = "JDBC Date format must be yyyy-mm-dd";
        if (s == null) {
            throw new IllegalArgumentException(formatError);
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
                digit(date.charAt(6)) - 1; // subtract one since
                                           // Calendar.JANUARY == 0

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
    private static Time time_valueOf(String s, Calendar cal) {
        if (s == null) {
            throw new IllegalArgumentException();
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
    private static void initTimePortion(Calendar cal, String time) {
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
    private static Timestamp timestamp_valueOf(String s, Calendar cal) {
        if (s == null) {
            throw new IllegalArgumentException();
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

    private static byte parseByte(String s) throws NumberFormatException {
        int i = parseInt(s);
        if (i < Byte.MIN_VALUE || i > Byte.MAX_VALUE) {
            throw new NumberFormatException();
        }
        return (byte) i;
    }

    private static short parseShort(String s) throws NumberFormatException {
        int i = parseInt(s);
        if (i < Short.MIN_VALUE || i > Short.MAX_VALUE) {
            throw new NumberFormatException();
        }
        return (short) i;
    }

    // Custom version of java.lang.parseInt() that allows for space padding of char fields.
    private static int parseInt(String s) throws NumberFormatException {
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

    private static long parseLong(String s) throws NumberFormatException {
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
            } else {    // Only got "-"
                throw new NumberFormatException(s);
            }
        } else {
            return -result;
        }
    }

    private static void skipPadding(String s, int i, int length)
            throws NumberFormatException {
        while (i < length) {
            if (s.charAt(i++) != ' ') {
                throw new NumberFormatException(s);
            }
        }
    }
}
