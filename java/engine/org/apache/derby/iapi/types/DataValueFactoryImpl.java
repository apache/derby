/*

   Derby - Class org.apache.derby.iapi.types.DataValueFactoryImpl

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

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.UserDataValue;
import org.apache.derby.iapi.types.RefDataValue;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.i18n.LocaleFinder;

import org.apache.derby.iapi.types.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Locale;

import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.iapi.services.context.ContextService;

/**
 * This class implements DataValueFactory.
 *
 * @see DataValueFactory
 */
public class DataValueFactoryImpl implements DataValueFactory
{
        LocaleFinder localeFinder;

        /**
          *     Make the constructor public.
          *
          */
    public      DataValueFactoryImpl(LocaleFinder localeFinder)
        {
                this.localeFinder = localeFinder;
        }

        /**
         * @see DataValueFactory#getDataValue
         *
         */
        public NumberDataValue getDataValue(int value)
        {
                return new SQLInteger(value);
        }

        public NumberDataValue getDataValue(int value, NumberDataValue previous)
                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLInteger(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Integer value)
        {
                if (value != null)
                        return new SQLInteger(value.intValue());
                else
                        return new SQLInteger();
        }

        public NumberDataValue getDataValue(Integer value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                {
                        return getDataValue(value);
                }

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(char value)
        {
                return new SQLInteger(value);
        }

        public NumberDataValue getDataValue(char value, NumberDataValue previous)
                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLInteger(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(short value)
        {
                return new SQLSmallint(value);
        }

        public NumberDataValue getDataValue(short value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLSmallint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Short value)
        {
                if (value != null)
                        return new SQLSmallint(value.shortValue());
                else
                        return new SQLSmallint();
        }

        public NumberDataValue getDataValue(Short value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(byte value)
        {
                return new SQLTinyint(value);
        }

        public NumberDataValue getDataValue(byte value, NumberDataValue previous)
                                throws StandardException
        {
                if (previous == null)
                        return new SQLTinyint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Byte value)
        {
                if (value != null)
                        return new SQLTinyint(value.byteValue());
                else
                        return new SQLTinyint();
        }

        public NumberDataValue getDataValue(Byte value, NumberDataValue previous)
                                throws StandardException
        {
                if (previous == null)
                        return getDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(long value)
        {
                return new SQLLongint(value);
        }

        public NumberDataValue getDataValue(long value, NumberDataValue previous)
                                throws StandardException
        {
                if (previous == null)
                        return new SQLLongint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Long value)
        {
                if (value != null)
                        return new SQLLongint(value.longValue());
                else
                        return new SQLLongint();
        }

        public NumberDataValue getDataValue(Long value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(float value)
                throws StandardException
        {
                return new SQLReal(value);
        }

        public NumberDataValue getDataValue(float value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLReal(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Float value)
                throws StandardException
        {
                if (value != null)
                        return new SQLReal(value.floatValue());
                else
                        return new SQLReal();
        }

        public NumberDataValue getDataValue(Float value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(double value) throws StandardException
        {
                return new SQLDouble(value);
        }

        public NumberDataValue getDataValue(double value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLDouble(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Double value) throws StandardException
        {
                if (value != null)
                        return new SQLDouble(value.doubleValue());
                else
                        return new SQLDouble();
        }

        public NumberDataValue getDataValue(Double value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(BigDecimal value)
        {
                if (value != null)
                        return new SQLDecimal(value);
                else
                        return new SQLDecimal();
        }

        public NumberDataValue getDataValue(BigDecimal value,
                                                                                NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDecimalDataValue(String value) throws StandardException
        {
                if (value != null)
                        return new SQLDecimal(value);
                else
                        return new SQLDecimal();
        }

        public NumberDataValue getDecimalDataValue(String value,
                                                                                                NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDecimalDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public BooleanDataValue getDataValue(boolean value)
        {
                return new SQLBoolean(value);
        }

        public BooleanDataValue getDataValue(boolean value,
                                                                                BooleanDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLBoolean(value);
        
                previous.setValue(value);
                return previous;
        }

        public BooleanDataValue getDataValue(Boolean value)
        {
                if (value != null)
                        return new SQLBoolean(value.booleanValue());
                else
                        return new SQLBoolean();
        }

        public BooleanDataValue getDataValue(Boolean value,
                                                                                        BooleanDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public BooleanDataValue getDataValue(BooleanDataValue value)
        {
                if (value != null)
                        return value;
                else
                        return new SQLBoolean();
        }

        public BitDataValue getBitDataValue(byte[] value) throws StandardException
        {
                return new SQLBit(value);
        }

        public BitDataValue getBitDataValue(byte[] value, BitDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLBit(value);
                previous.setValue(value);
                return previous;
        }

        public BitDataValue getVarbitDataValue(byte[] value)
        {
                return new SQLVarbit(value);
        }

        public BitDataValue getVarbitDataValue(byte[] value, BitDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLVarbit(value);
                previous.setValue(value);
                return previous;
        }


        // LONGVARBIT

        public BitDataValue getLongVarbitDataValue(byte[] value) throws StandardException
        {
                return new SQLLongVarbit(value);
        }

        public BitDataValue getLongVarbitDataValue(byte[] value, BitDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLLongVarbit(value);
                previous.setValue(value);
                return previous;
        }

        // BLOB
        public BitDataValue getBlobDataValue(byte[] value) throws StandardException
        {
                return new SQLBlob(value);
        }

        public BitDataValue getBlobDataValue(byte[] value, BitDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLBlob(value);
                previous.setValue(value);
                return previous;
        }

        // CHAR
        public StringDataValue getCharDataValue(String value)
        {
                return new SQLChar(value);
        }

        public StringDataValue getCharDataValue(String value,
                                                                                        StringDataValue previous)
                                                                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLChar(value);
                previous.setValue(value);
                return previous;
        }

        public StringDataValue getVarcharDataValue(String value)
        {
                return new SQLVarchar(value);
        }

        public StringDataValue getVarcharDataValue(String value,
                                                                                                StringDataValue previous)
                                                                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLVarchar(value);
                previous.setValue(value);
                return previous;
        }

        public StringDataValue getLongvarcharDataValue(String value)
        {
                return new SQLLongvarchar(value);
        }

        public StringDataValue getClobDataValue(String value)
        {
                return new SQLClob(value);
        }

        public StringDataValue getLongvarcharDataValue(String value,
                                                                                                        StringDataValue previous)
                                                                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLLongvarchar(value);
                previous.setValue(value);
                return previous;
        }

        public StringDataValue getClobDataValue(String value, StringDataValue previous) throws StandardException
        {
                if (previous == null)
                        return new SQLClob(value);
                previous.setValue(value);
                return previous;
        }

        //
        public StringDataValue getNationalCharDataValue(String value)
        {
                return new SQLNationalChar(value, getLocaleFinder());
        }

        public StringDataValue getNationalCharDataValue(String value,
                                                                                        StringDataValue previous)
                                                                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLNationalChar(value, getLocaleFinder());
                previous.setValue(value);
                return previous;
        }

        public StringDataValue getNationalVarcharDataValue(String value)
        {
                return new SQLNationalVarchar(value, getLocaleFinder());
        }

        public StringDataValue getNationalVarcharDataValue(String value,
                                                                                                StringDataValue previous)
                                                                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLNationalVarchar(value, getLocaleFinder());
                previous.setValue(value);
                return previous;
        }

        public StringDataValue getNationalLongvarcharDataValue(String value)
        {
                return new SQLNationalLongvarchar(value, getLocaleFinder());
        }

        public StringDataValue getNationalLongvarcharDataValue(String value,
                                                                                                        StringDataValue previous)
                                                                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLNationalLongvarchar(value, getLocaleFinder());
                previous.setValue(value);
                return previous;
        }

        public StringDataValue getNClobDataValue(String value)
        {
                return new SQLNClob(value, getLocaleFinder());
        }

        public StringDataValue getNClobDataValue(String value, StringDataValue previous)
            throws StandardException
        {
                if (previous == null)
                        return new SQLNClob(value, getLocaleFinder());
                previous.setValue(value);
                return previous;
        }

        public DateTimeDataValue getDataValue(Date value) throws StandardException
        {
                return new SQLDate(value);
        }

        public DateTimeDataValue getDataValue(Date value,
                                                                                        DateTimeDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLDate(value);
                previous.setValue(value);
                return previous;
        }

        public DateTimeDataValue getDataValue(Time value) throws StandardException
        {
                return new SQLTime(value);
        }

        public DateTimeDataValue getDataValue(Time value,
                                                                                        DateTimeDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLTime(value);
                previous.setValue(value);
                return previous;
        }

        public DateTimeDataValue getDataValue(Timestamp value) throws StandardException
        {
                return new SQLTimestamp(value);
        }

        public DateTimeDataValue getDataValue(Timestamp value,
                                                                                        DateTimeDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLTimestamp(value);
                previous.setValue(value);
                return previous;
        }

        public DateTimeDataValue getTimestamp( DataValueDescriptor date, DataValueDescriptor time) throws StandardException
        {
            return new SQLTimestamp( date, time);
        }

        public UserDataValue getDataValue(Object value)
        {
                return new UserType(value);
        }

        public UserDataValue getDataValue(Object value,
                                                                                UserDataValue previous)
        {
                if (previous == null)
                        return new UserType(value);
                ((UserType) previous).setValue(value);
                return previous;
        }

        public RefDataValue getDataValue(RowLocation value)
        {
                return new SQLRef(value);
        }

        public RefDataValue getDataValue(RowLocation value, RefDataValue previous)
        {
                if (previous == null)
                        return new SQLRef(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue          getNullInteger(NumberDataValue dataValue) 
        {
                if (dataValue == null)
                {
                        return new SQLInteger();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public NumberDataValue getNullShort(NumberDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLSmallint();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public NumberDataValue getNullLong(NumberDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLLongint();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public NumberDataValue getNullByte(NumberDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLTinyint();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public NumberDataValue getNullFloat(NumberDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLReal();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public NumberDataValue getNullDouble(NumberDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLDouble();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public NumberDataValue getNullBigDecimal(NumberDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLDecimal();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public BooleanDataValue getNullBoolean(BooleanDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLBoolean();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public BitDataValue             getNullBit(BitDataValue dataValue) throws StandardException
        {
                if (dataValue == null)
                {
                        return getBitDataValue((byte[]) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }


        public BitDataValue             getNullVarbit(BitDataValue dataValue) throws StandardException
        {
                if (dataValue == null)
                {
                        return getVarbitDataValue((byte[]) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        // LONGVARBIT
        public BitDataValue getNullLongVarbit(BitDataValue dataValue) throws StandardException
        {
                if (dataValue == null)
                {
                        return getLongVarbitDataValue((byte[]) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        /// BLOB
        public BitDataValue getNullBlob(BitDataValue dataValue) throws StandardException
        {
                if (dataValue == null)
                {
                        return getBlobDataValue((byte[]) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        // CHAR
        public StringDataValue          getNullChar(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getCharDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public StringDataValue          getNullVarchar(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getVarcharDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public StringDataValue          getNullLongvarchar(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getLongvarcharDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public StringDataValue          getNullClob(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getClobDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public StringDataValue          getNullNationalChar(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getNationalCharDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public StringDataValue          getNullNationalVarchar(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getNationalVarcharDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public StringDataValue          getNullNationalLongvarchar(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getNationalLongvarcharDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public StringDataValue          getNullNClob(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getNClobDataValue((String) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public UserDataValue            getNullObject(UserDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getDataValue((Object) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public RefDataValue             getNullRef(RefDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return getDataValue((RowLocation) null);
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public DateTimeDataValue        getNullDate(DateTimeDataValue dataValue)
        {
                if (dataValue == null)
                {
                    try
                    {
                        return getDataValue((Date) null);
                    }
                    catch( StandardException se)
                    {
                        if( SanityManager.DEBUG)
                        {
                            SanityManager.THROWASSERT( "Could not get a null date.", se);
                        }
                        return null;
                    }
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public DateTimeDataValue        getNullTime(DateTimeDataValue dataValue)
        {
                if (dataValue == null)
                {
                    try
                    {
                        return getDataValue((Time) null);
                    }
                    catch( StandardException se)
                    {
                        if( SanityManager.DEBUG)
                        {
                            SanityManager.THROWASSERT( "Could not get a null time.", se);
                        }
                        return null;
                    }
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

        public DateTimeDataValue        getNullTimestamp(DateTimeDataValue dataValue)
        {
                if (dataValue == null)
                {
                    try
                    {
                        return getDataValue((Timestamp) null);
                    }
                    catch( StandardException se)
                    {
                        if( SanityManager.DEBUG)
                        {
                            SanityManager.THROWASSERT( "Could not get a null timestamp.", se);
                        }
                        return null;
                    }
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }

    public DateTimeDataValue getDateValue( String dateStr, boolean isJdbcEscape) throws StandardException
    {
        return new SQLDate( dateStr, isJdbcEscape, getLocaleFinder());
    } // end of getDateValue( String dateStr)

    public DateTimeDataValue getTimeValue( String timeStr, boolean isJdbcEscape) throws StandardException
    {
        return new SQLTime( timeStr, isJdbcEscape, getLocaleFinder());
    } // end of getTimeValue( String timeStr)

    public DateTimeDataValue getTimestampValue( String timestampStr, boolean isJdbcEscape) throws StandardException
    {
        return new SQLTimestamp( timestampStr, isJdbcEscape, getLocaleFinder());
    } // end of getTimestampValue( String timestampStr)


        // RESOLVE: This is here to find the LocaleFinder (i.e. the Database)
        // on first access. This is necessary because the Monitor can't find
        // the Database at boot time, because the Database is not done booting.
        // See LanguageConnectionFactory.
        private LocaleFinder getLocaleFinder()
        {
                if (localeFinder == null)
                {
                        DatabaseContext dc = (DatabaseContext) ContextService.getContext(DatabaseContext.CONTEXT_ID);
                        if( dc != null)
                            localeFinder = dc.getDatabase();
                }

                return localeFinder;
        }
}
