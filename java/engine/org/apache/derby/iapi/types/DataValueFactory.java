/*

   Derby - Class org.apache.derby.iapi.types.DataValueFactory

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import java.util.Locale;

/**
 * This interface is how we get constant data values of different types.
 */

public interface DataValueFactory
{
        /**
         * Get a SQL int with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        NumberDataValue         getDataValue(Integer value);
        NumberDataValue         getDataValue(Integer value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL int with a char value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        public NumberDataValue getDataValue(char value);
        public NumberDataValue getDataValue(char value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL smallint with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        NumberDataValue         getDataValue(Short value);
        NumberDataValue         getDataValue(Short value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL TINYINT with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        NumberDataValue         getDataValue(Byte value);
        NumberDataValue         getDataValue(Byte value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL bigint with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        NumberDataValue         getDataValue(Long value);
        NumberDataValue         getDataValue(Long value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL real with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        NumberDataValue         getDataValue(Float value) throws StandardException;
        NumberDataValue         getDataValue(Float value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL double precision with the given value.  A null argument means
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         * @exception StandardException         Thrown on error
         */
        NumberDataValue         getDataValue(Double value) throws StandardException;
        NumberDataValue         getDataValue(Double value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL boolean with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        BooleanDataValue        getDataValue(Boolean value);
        BooleanDataValue        getDataValue(Boolean value, BooleanDataValue previous)
                                                        throws StandardException;
        
        // ------ LONGVARBIT

        /**
         * Get a SQL Long Bit Varying with the given value.  A null argument means
         * get a SQL null value.  The second form uses the previous value (if
         * non-null) to hold the return value.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getLongVarbitDataValue(byte[] value) throws StandardException;
        BitDataValue            getLongVarbitDataValue(byte[] value,
                                                                                                BitDataValue previous)
                                                        throws StandardException;

        // ------ BLOB

        /**
         * Get a SQL Blob with the given value.  A null argument means
         * get a SQL null value.  The second form uses the previous value (if
         * non-null) to hold the return value.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getBlobDataValue(byte[] value) throws StandardException;
        BitDataValue            getBlobDataValue(byte[] value,
                                                                                                BitDataValue previous)
                                                        throws StandardException;
        // ------ BOOLEAN
        /**
         * Get a SQL boolean with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         * @exception StandardException         Thrown on error
         */
        BooleanDataValue        getDataValue(BooleanDataValue value) throws StandardException;

        /**
         * Get a SQL varchar with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        StringDataValue         getVarcharDataValue(String value);
        StringDataValue         getVarcharDataValue(String value,
                                                                                        StringDataValue previous)
                                                                                                        throws StandardException;

        /**
         * Get a SQL long varchar with the given value.  A null argument means
         * get a SQL null value.  The second form uses the previous value
         * (if non-null) to hold the return value.
         *
         */
        StringDataValue         getLongvarcharDataValue(String value);
        StringDataValue         getLongvarcharDataValue(String value, StringDataValue previous) throws StandardException;

        /**
         * Get a SQL Clob with the given value.  A null argument means
         * get a SQL null value.  The second form uses the previous value
         * (if non-null) to hold the return value.
         *
         */
        StringDataValue         getClobDataValue(String value);
        StringDataValue         getClobDataValue(String value, StringDataValue previous) throws StandardException;

        /**
         * Get a SQL national varchar with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         */
        StringDataValue         getNationalVarcharDataValue(String value);
        StringDataValue         getNationalVarcharDataValue(String value,
                                                                                        StringDataValue previous)
                                                                                                        throws StandardException;

        /**
         * Get a SQL national long varchar with the given value.  A null argument means
         * get a SQL null value.  The second form uses the previous value
         * (if non-null) to hold the return value.
         */
        StringDataValue         getNationalLongvarcharDataValue(String value);
        StringDataValue         getNationalLongvarcharDataValue(String value,
                                                                                                StringDataValue previous)
                                                                                                        throws StandardException;

        /**
         * Get a SQL national blob with the given value.  A null argument means
         * get a SQL null value.  The second form uses the previous value
         * (if non-null) to hold the return value.
         */
        StringDataValue         getNClobDataValue(String value);
        StringDataValue         getNClobDataValue(String value, StringDataValue previous) 
            throws StandardException;

        /**
         * Get a User-defined data value with the given value and type name.
         * A null argument means get a SQL null value.  The second form uses
         * the previous value (if non-null) hold the return value.
         *
         */
        UserDataValue           getDataValue(Object value);
        UserDataValue           getDataValue(Object value,
                                                                                UserDataValue previous);

        /**
         * Get a RefDataValue with the given value.  A null argument means get
         * a SQL null value.  The second form uses the previous value (if non-null)
         * to hold the return value.
         *
         */
        RefDataValue            getDataValue(RowLocation value);
        RefDataValue            getDataValue(RowLocation value, RefDataValue previous);

        /**
         * Get a SQL int with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         */
        NumberDataValue         getDataValue(int value);
        NumberDataValue         getDataValue(int value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL bigint with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         */
        NumberDataValue         getDataValue(long value);
        NumberDataValue         getDataValue(long value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL real with the given value.  The second form
         * re-uses the previous value, if non-null, as the data holder to return.
         *
         * @exception StandardException         Thrown on error
         */
        NumberDataValue         getDataValue(float value) throws StandardException;
        NumberDataValue         getDataValue(float value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL double precision with the given value.  The second form
         * re-uses the previous value, if non-null, as the data holder to return.
         *
         * @exception StandardException         Thrown on error
         */
        NumberDataValue         getDataValue(double value) throws StandardException;
        NumberDataValue         getDataValue(double value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL SMALLINT with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         */
        NumberDataValue         getDataValue(short value);
        NumberDataValue         getDataValue(short value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL TINYINT with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         */
        NumberDataValue         getDataValue(byte value);
        NumberDataValue         getDataValue(byte value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL DECIMAL with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         * @exception StandardException         Thrown on error
         */
        NumberDataValue         getDataValue(BigDecimal value) throws StandardException;
        NumberDataValue         getDataValue(BigDecimal value, NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL DECIMAL with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         * @exception StandardException         Thrown on error
         */
        NumberDataValue         getDecimalDataValue(String value) throws StandardException;
        NumberDataValue         getDecimalDataValue(String value,
                                                                                        NumberDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL boolean with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         */
        BooleanDataValue        getDataValue(boolean value);
        BooleanDataValue        getDataValue(boolean value, BooleanDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL bit with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getBitDataValue(byte[] value) throws StandardException;
        BitDataValue            getBitDataValue(byte[] value, BitDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL bit with the given value.  The second form re-uses the
         * previous value, if non-null, as the data holder to return.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getVarbitDataValue(byte[] value) throws StandardException;
        BitDataValue            getVarbitDataValue(byte[] value, BitDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL char with the given value.  A null argument means get
         * a SQL null value.  The second form re-uses the previous value,
         * if non-null, as the data holder to return.
         *
         */
        StringDataValue         getCharDataValue(String value);
        StringDataValue         getCharDataValue(String value, StringDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL national char with the given value.  A null argument means get
         * a SQL null value.  The second form re-uses the previous value,
         * if non-null, as the data holder to return.
         */
        StringDataValue         getNationalCharDataValue(String value);
        StringDataValue         getNationalCharDataValue(String value, StringDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL date with the given value.  A null argument means get
         * a SQL null value.  The second form re-uses the previous value,
         * if non-null, as the data holder to return.
         *
         */
        DateTimeDataValue       getDataValue(Date value) throws StandardException;
        DateTimeDataValue       getDataValue(Date value, DateTimeDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL time with the given value.  A null argument means get
         * a SQL null value.  The second form re-uses the previous value,
         * if non-null, as the data holder to return.
         *
         */
        DateTimeDataValue       getDataValue(Time value) throws StandardException;
        DateTimeDataValue       getDataValue(Time value, DateTimeDataValue previous)
                                                        throws StandardException;

        /**
         * Get a SQL timestamp with the given value.  A null argument means get
         * a SQL null value.  The second form re-uses the previous value,
         * if non-null, as the data holder to return.
         *
         */
        DateTimeDataValue       getDataValue(Timestamp value) throws StandardException;
        DateTimeDataValue       getDataValue(Timestamp value,
                                                                                DateTimeDataValue previous)
                                                        throws StandardException;
        /**
         * Construct a SQL timestamp from a date and time.
         *
         * @param date Must be convertible to a date.
         * @param time Must be convertible to a time.
         */
        DateTimeDataValue getTimestamp( DataValueDescriptor date, DataValueDescriptor time) throws StandardException;

        /**
         * @param dateStr A date in one of the DB2 standard date formats or the local format.
         * @param isJdbcEscape If true then the timestamp must be in the JDBC timestamp escape format, otherwise it must
         *                     be in the DB2 timestamp format.
         * @return A DateTimeDataValue
         *
         * @exception StandardException if the syntax is invalid or the date is out of range.
         */
        public DateTimeDataValue getDateValue( String dateStr, boolean isJdbcEscape) throws StandardException;

        /**
         * @param timeStr A date in one of the DB2 standard time formats or the local format.
         * @param isJdbcEscape If true then the timestamp must be in the JDBC time escape format, otherwise it must
         *                     be in the DB2 time format.
         * @return A DateTimeDataValue
         *
         * @exception StandardException if the syntax is invalid or the time is out of range.
         */
        public DateTimeDataValue getTimeValue( String timeStr, boolean isJdbcEscape) throws StandardException;

        /**
         * @param timeStr A time in string format.
         * @param isJdbcEscape If true then the time must be in the JDBC time escape format, otherwise it must
         *                     be in the DB2 time format.
         * @return An internal timestamp
         *
         * @exception StandardException if the syntax is invalid or the timestamp is out of range.
         */
        public DateTimeDataValue getTimestampValue( String timestampStr, boolean isJdbcEscape) throws StandardException;

        /**
         * Get a SQL int with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        NumberDataValue getNullInteger(NumberDataValue dataValue);

        /**
         * Get a SQL smallint with  a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         */
        NumberDataValue getNullShort(NumberDataValue dataValue);

        /**
         * Get a SQL tinyint with  a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        NumberDataValue getNullByte(NumberDataValue dataValue);

        /**
         * Get a SQL bigint with  a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        NumberDataValue getNullLong(NumberDataValue dataValue);

        /**
         * Get a SQL float with  a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        NumberDataValue getNullFloat(NumberDataValue dataValue);

        /**
         * Get a SQL double with  a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        NumberDataValue getNullDouble(NumberDataValue dataValue);

        /**
         * Get a SQL Decimal/Numeric with  a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        NumberDataValue getNullBigDecimal(NumberDataValue dataValue);

        /**
         * Get a SQL boolean with  a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         */
        BooleanDataValue getNullBoolean(BooleanDataValue dataValue);

        /**
         * Get a SQL Bit with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getNullBit(BitDataValue dataValue) throws StandardException;

        /**
         * Get a SQL Bit Varying with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getNullVarbit(BitDataValue dataValue) throws StandardException;

        // --- LONGVARBIT
        /**
         * Get a SQL Long Bit Varying with a SQL null value. If the supplied
         * value is null then get a new value, otherwise set it to null
         * and return that value.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getNullLongVarbit(
                                                                                                        BitDataValue dataValue)
                                                                throws StandardException;
        // --- BLOB
        /**
         * Get a SQL Blob with a SQL null value. If the supplied
         * value is null then get a new value, otherwise set it to null
         * and return that value.
         *
         * @exception StandardException         Thrown on error
         */
        BitDataValue            getNullBlob(BitDataValue dataValue)
                throws StandardException;
        
    // ------ CHAR
        /**
         * Get a SQL char with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         */
        StringDataValue         getNullChar(StringDataValue dataValue);

        /**
         * Get a SQL varchar with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        StringDataValue         getNullVarchar(StringDataValue dataValue);

        /**
         * Get a SQL long varchar with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        StringDataValue         getNullLongvarchar(StringDataValue dataValue);

        /**
         * Get a SQL long varchar with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        StringDataValue         getNullClob(StringDataValue dataValue);

        /**
         * Get a SQL national char with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         */
        StringDataValue         getNullNationalChar(StringDataValue dataValue);

        /**
         * Get a SQL national varchar with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        StringDataValue         getNullNationalVarchar(StringDataValue dataValue);

        /**
         * Get a SQL national long varchar with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        StringDataValue         getNullNationalLongvarchar(StringDataValue dataValue);

        /**
         * Get a SQL NCLOB with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        StringDataValue         getNullNClob(StringDataValue dataValue);

        /**
         * Get a User-defined data value with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        UserDataValue           getNullObject(UserDataValue dataValue);

        /**
         * Get a RefDataValue with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        RefDataValue            getNullRef(RefDataValue dataValue);

        /**
         * Get a SQL date with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        DateTimeDataValue       getNullDate(DateTimeDataValue dataValue);

        /**
         * Get a SQL time with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         *
         */
        DateTimeDataValue       getNullTime(DateTimeDataValue dataValue);

        /**
         * Get a SQL timestamp with a SQL null value. If the supplied value
         * is null then get a new value, otherwise set it to null and return 
         * that value.
         */
        DateTimeDataValue       getNullTimestamp(DateTimeDataValue dataValue);
}
