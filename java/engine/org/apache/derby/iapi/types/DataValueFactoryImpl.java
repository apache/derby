/*

   Derby - Class org.apache.derby.iapi.types.DataValueFactoryImpl

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

import org.apache.derby.iapi.services.i18n.LocaleFinder;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.RegisteredFormatIds;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.loader.ClassInfo;
import org.apache.derby.iapi.services.loader.InstanceGetter;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import java.text.Collator;
import java.text.RuleBasedCollator;

import java.util.Properties;
import java.util.Locale;

import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.iapi.services.context.ContextService;

/**
 * Core implementation of DataValueFactory. Does not implement
 * methods required to generate DataValueDescriptor implementations
 * for the DECIMAL datatype. J2ME and J2SE require different implementations.
 *
 * @see DataValueFactory
 */
abstract class DataValueFactoryImpl implements DataValueFactory, ModuleControl
{
        LocaleFinder localeFinder;
        //BasicDatabase first boots DVF in it's boot method and then sets 
        //this databaseLocale in DVF.
    	private Locale databaseLocale;
    	//Following Collator object will be initialized using databaseLocale.  
    	private RuleBasedCollator collatorForCharacterTypes;

        DataValueFactoryImpl()
        {
        }
        
        /*
         ** ModuleControl methods.
         */
        
    	/* (non-Javadoc)
    	 * @see org.apache.derby.iapi.services.monitor.ModuleControl#boot(boolean, java.util.Properties)
    	 */
    	public void boot(boolean create, Properties properties) throws StandardException {
    		
    		DataValueDescriptor decimalImplementation = getNullDecimal(null);
    		
    		TypeId.decimalImplementation = decimalImplementation;
    		RegisteredFormatIds.TwoByte[StoredFormatIds.SQL_DECIMAL_ID]
    									= decimalImplementation.getClass().getName();
    		    		
    		// Generate a DECIMAL value represetentation of 0
    		decimalImplementation = decimalImplementation.getNewNull();
    		decimalImplementation.setValue(0L);
    		NumberDataType.ZERO_DECIMAL = decimalImplementation;    		

    		ModuleFactory monitor = Monitor.getMonitor();
    		//The Locale on monitor has already been set by the boot code in
    		//BasicDatabase so we can simply do a get here.
    		//This Locale will be either the Locale obtained from the territory
    		//attribute supplied by the user on the JDBC url at database create
    		//time or if user didn't provide the territory attribute at database
    		//create time, then it will be set to the default JVM locale. The
    		//Locale object will be used to construct the Collator object which
    		//will be used if user has requested territory based collation.
    		databaseLocale = monitor.getLocale(this);

    		//If we are here for database create time, verify that there is 
    		//Collator support for the database's locale. If not, then we 
    		//will throw an exception. 
    		//Notice that this Collator support check is happening only during 
    		//database creation time. This is because, during database create
    		//time, DVF has access to collation property of the database and
    		//hence it can do the Collator support check
    		//(collation property is available through JDBC url at the database
    		//create time, if user has asked for a particular collation) eg
    		//connect 'jdbc:derby:db;create=true;territory=no;collation=TERRITORY_BASED';
    		//Once the database is created, the collation property gets
    		//saved in the database and during susbsequent boots of the
    		//database, collation attribute of the database is only available
    		//once store has finished reading it. So, during subsequent 
    		//database boot up time, the collation attribute of the database 
    		//will be checked the first time a collation operation is done.
    		//And if the Collator support is not found at that point, user will 
    		//get an exception for Collator unavailability. This first 
    		//collation operation can happen if the database needs to be 
    		//recovered during boot time or otherwise it will happen when the
    		//user has executed a SQL which requires collation operation.
	    	if (create) {
	    		//Get the collation property from the JDBC url(this will be 
	    		//available only during database create time). It can only have 
	    		//one of the 2 possible values - UCS_BASIC or TERRITORY_BASED.
	    		//This property can only be specified at database create time.
	    		//If the user has requested for territory based database, then 
	    		//verify that JVM has Collator support for the database locale.
	    		String userDefinedCollation = 
	    			properties.getProperty(Attribute.COLLATION);
	    		if (userDefinedCollation != null) {//Invalid value handling
					int collationType = DataTypeDescriptor.getCollationType(userDefinedCollation);
					if (collationType != StringDataValue.COLLATION_TYPE_UCS_BASIC) {
						if (collationType >= StringDataValue.COLLATION_TYPE_TERRITORY_BASED
								&& collationType <  StringDataValue.COLLATION_TYPE_TERRITORY_BASED_IDENTICAL) {
							int strength = collationType - StringDataValue.COLLATION_TYPE_TERRITORY_BASED_PRIMARY;
							collatorForCharacterTypes = verifyCollatorSupport(strength);
						} else
							throw StandardException.newException(SQLState.INVALID_COLLATION, userDefinedCollation);
					}
	    		}    		
	    	}
    	}

    	/* (non-Javadoc)
    	 * @see org.apache.derby.iapi.services.monitor.ModuleControl#stop()
    	 */
    	public void stop() {
    	}
 
        /**
         * @see DataValueFactory#getDataValue
         *
         */
        public NumberDataValue getDataValue(int value, NumberDataValue previous)
                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLInteger(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Integer value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                {
                        return new SQLInteger(value);
                }

                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(char value, NumberDataValue previous)
                                                        throws StandardException
        {
                if (previous == null)
                        return new SQLInteger(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(short value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLSmallint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Short value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLSmallint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(byte value, NumberDataValue previous)
                                throws StandardException
        {
                if (previous == null)
                        return new SQLTinyint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Byte value, NumberDataValue previous)
                                throws StandardException
        {
                if (previous == null)
                        return new SQLTinyint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(long value, NumberDataValue previous)
                                throws StandardException
        {
                if (previous == null)
                        return new SQLLongint(value);
                previous.setValue(value);
                return previous;
        }


        public NumberDataValue getDataValue(Long value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLLongint(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(float value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLReal(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Float value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLReal(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(double value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLDouble(value);
                previous.setValue(value);
                return previous;
        }

        public NumberDataValue getDataValue(Double value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLDouble(value);
                previous.setValue(value);
                return previous;
        }
        public final NumberDataValue getDecimalDataValue(Number value)
			throws StandardException
        {
			NumberDataValue ndv = getNullDecimal((NumberDataValue) null);
			ndv.setValue(value);
			return ndv;
        }

        public final NumberDataValue getDecimalDataValue(Number value, NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDecimalDataValue(value);

                previous.setValue(value);
                return previous;
        }

        public final NumberDataValue getDecimalDataValue(String value,
                                                                                                NumberDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return getDecimalDataValue(value);

                previous.setValue(value);
                return previous;
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

        public BooleanDataValue getDataValue(Boolean value,
                                                                                        BooleanDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLBoolean(value);
                previous.setValue(value);
                return previous;
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

        public BitDataValue getVarbitDataValue(byte[] value, BitDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLVarbit(value);
                previous.setValue(value);
                return previous;
        }


        // LONGVARBIT

        public BitDataValue getLongVarbitDataValue(byte[] value, BitDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLLongVarbit(value);
                previous.setValue(value);
                return previous;
        }

        // BLOB

        public BitDataValue getBlobDataValue(byte[] value, BitDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLBlob(value);
                previous.setValue(value);
                return previous;
        }

        public BitDataValue getBlobDataValue(Blob value, BitDataValue previous)
                        throws StandardException
        {
            if (previous == null) { return new SQLBlob(value); }
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
        
        /**
         * Return a StringDataValue to represent a SQL CHAR
         * with the given collation re-using previous if not null.
         */
        public StringDataValue getCharDataValue(String value,
                StringDataValue previous, int collationType)
            throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getCharDataValue(value, previous);
            
            if (previous == null)
                return new CollatorSQLChar(value, getCharacterCollator(collationType));
            
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
        
        /**
         * Return a StringDataValue to represent a SQL VARCHAR
         * with the given collation re-using previous if not null.
         */
        public StringDataValue getVarcharDataValue(String value,
                StringDataValue previous, int collationType)
            throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getVarcharDataValue(value, previous);
            
            if (previous == null)
                return new CollatorSQLVarchar(value,
                        getCharacterCollator(collationType));
            
            previous.setValue(value);
            return previous;
        }

        public StringDataValue getLongvarcharDataValue(String value)
        {
                return new SQLLongvarchar(value);
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
        
        /**
         * Return a StringDataValue to represent a SQL LONG VARCHAR
         * with the given collation re-using previous if not null.
         */
        public StringDataValue getLongvarcharDataValue(String value,
                StringDataValue previous, int collationType)
            throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getLongvarcharDataValue(value, previous);
            
            if (previous == null)
                return new CollatorSQLLongvarchar(value,
                        getCharacterCollator(collationType));
            
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
        
        public StringDataValue getClobDataValue(Clob value, StringDataValue previous) throws StandardException
        {
            if (previous == null) { return new SQLClob(value); }
            
            previous.setValue(value);
            return previous;
        }

        public StringDataValue getClobDataValue(Clob value,
                StringDataValue previous, int collationType)
            throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getClobDataValue(value, previous);
            
            if (previous == null)
            {
                return new CollatorSQLClob(value,
                        getCharacterCollator(collationType));
            }
            
            previous.setValue(value);
            return previous;
        }
    
        /**
         * Return a StringDataValue to represent a SQL CLOB
         * with the given collation re-using previous if not null.
         */
        public StringDataValue getClobDataValue(String value,
                StringDataValue previous, int collationType)
            throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getClobDataValue(value, previous);
            
            if (previous == null)
                return new CollatorSQLClob(value,
                        getCharacterCollator(collationType));
            
            previous.setValue(value);
            return previous;
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

        public DateTimeDataValue getDataValue(Time value,
                                                                                        DateTimeDataValue previous)
                        throws StandardException
        {
                if (previous == null)
                        return new SQLTime(value);
                previous.setValue(value);
                return previous;
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

        /**
         * Implement the date SQL function: construct a SQL date from a string, number, or timestamp.
         *
         * @param operand Must be a date, a number, or a string convertible to a date.
         *
         * @exception StandardException standard error policy
         */
        public DateTimeDataValue getDate( DataValueDescriptor operand) throws StandardException
        {
                return SQLDate.computeDateFunction( operand, this);
        }

        /**
         * Implement the timestamp SQL function: construct a SQL timestamp from a string, or timestamp.
         *
         * @param operand Must be a timestamp or a string convertible to a timestamp.
         *
         * @exception StandardException standard error policy
         */
        public DateTimeDataValue getTimestamp( DataValueDescriptor operand) throws StandardException
        {
                return SQLTimestamp.computeTimestampFunction( operand, this);
        }

        public DateTimeDataValue getTimestamp( DataValueDescriptor date, DataValueDescriptor time) throws StandardException
        {
            return new SQLTimestamp( date, time);
        }

        public UserDataValue getDataValue(Object value,
                                                                                UserDataValue previous)
        {
                if (previous == null)
                        return new UserType(value);
                ((UserType) previous).setValue(value);
                return previous;
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
                        return new SQLVarbit();
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
                        return new SQLLongVarbit();
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
                        return new SQLBlob();
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
        /**
         * Get a SQL CHAR set to NULL with collation set to collationType.
         * If the supplied value is null then get a new value,
         * otherwise set it to null and return that value.
         */
        public StringDataValue         getNullChar(StringDataValue previous,
                int collationType)
        throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getNullChar(previous);
            
            if (previous == null)
                return new CollatorSQLChar(getCharacterCollator(collationType));
            
            previous.setToNull();
            return previous;
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
        
        /**
         * Get a SQL VARCHAR set to NULL with collation set to collationType.
         * If the supplied value is null then get a new value,
         * otherwise set it to null and return that value.
         */
        public StringDataValue         getNullVarchar(StringDataValue previous,
                int collationType)
        throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getNullChar(previous);
            
            if (previous == null)
                return new CollatorSQLVarchar(getCharacterCollator(collationType));
            
            previous.setToNull();
            return previous;
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
        
        /**
         * Get a SQL LONG VARCHAR set to NULL with collation set to collationType.
         * If the supplied value is null then get a new value,
         * otherwise set it to null and return that value.
         */
        public StringDataValue         getNullLongvarchar(StringDataValue previous,
                int collationType)
        throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getNullChar(previous);
            
            if (previous == null)
                return new CollatorSQLLongvarchar(getCharacterCollator(collationType));
            
            previous.setToNull();
            return previous;
        }

        public StringDataValue          getNullClob(StringDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new SQLClob();
                }
                else
                {
                        dataValue.setToNull();
                        return dataValue;
                }
        }
        
        /**
         * Get a SQL CLOB set to NULL with collation set to collationType.
         * If the supplied value is null then get a new value,
         * otherwise set it to null and return that value.
         */
        public StringDataValue         getNullClob(StringDataValue previous,
                int collationType)
        throws StandardException
        {
            if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
                return getNullChar(previous);
            
            if (previous == null)
                return new CollatorSQLClob(getCharacterCollator(collationType));
            
            previous.setToNull();
            return previous;
        }

        public UserDataValue            getNullObject(UserDataValue dataValue)
        {
                if (dataValue == null)
                {
                        return new UserType((Object) null);
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
                        return new SQLRef();
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
                        return new SQLDate((Date) null);
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
                        return new SQLTime((Time) null);
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
                        return new SQLTimestamp((Timestamp) null);
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

    /**
     * getXMLDataValue:
     * Get a null XML  value.  If a non-null XMLDataValue is
     * received then re-use that instance, otherwise create
	 * a new one.
     * @param previous An XMLDataValue instance to re-use.
     * @return An XMLDataValue instance corresponding to a
     *  NULL value.  If an XMLDataValue was received, the
     *  returned XMLDataValue is the same instance as the one
     *  received, but the actual data has been set to a
     *  SQL null value.
     * @exception StandardException Thrown on error
     */
    public XMLDataValue getXMLDataValue(XMLDataValue previous)
		throws StandardException
    {
		return getNullXML(previous);
    }

    /**
     * getNullXML:
     * Get an XML with a SQL null value. If the supplied value is
     * null then get a new value, otherwise set it to null and return 
     * that value.
     * @param dataValue An XMLDataValue instance to re-use.
     * @return An XMLDataValue instance corresponding to a
     *  NULL value.  If an XMLDataValue was received, the
     *  returned XMLDataValue is the same instance as the one
     *  received, but the actual data has been set to null.
     */
    public XMLDataValue getNullXML(XMLDataValue dataValue)
    {
        if (dataValue == null)
            return new XML();
        else {
            dataValue.setToNull();
            return dataValue;
        }
    }

    /** @see DataValueFactory#getCharacterCollator(int) */
    public RuleBasedCollator getCharacterCollator(int collationType) 
    throws StandardException {
    	if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
    		return (RuleBasedCollator)null;
    	else if (collatorForCharacterTypes == null) {
    		//This is the first access to Collator because otherwise
    		//it will not be null. Verify that JVM has support for
    		//the Collator for the database locale.
			//	Calculate the collator strength. COLLATION_TYPE_TERRITORY_BASED use strength -1, i e unspecified.
			int strength = collationType - StringDataValue.COLLATION_TYPE_TERRITORY_BASED_PRIMARY;
    		collatorForCharacterTypes = verifyCollatorSupport(strength);
    		return collatorForCharacterTypes;    	    		
    	} else
    		return collatorForCharacterTypes;    	
    }
    
    /**
     * Verify that JVM has support for the Collator for the database's locale.
     *
	 * @param strength Collator strength or -1 for locale default.
     * @return Collator for database's locale
     * @throws StandardException if JVM does not have support for Collator
     */
    private RuleBasedCollator verifyCollatorSupport(int strength)
    throws StandardException {
    	Locale[] availLocales =  Collator.getAvailableLocales();
    	//Verify that Collator can be instantiated for the given locale.
    	boolean localeFound = false;
    	for (int i=0; i<availLocales.length;i++)
    	{
    		if (availLocales[i].equals(databaseLocale)) {
    			localeFound = true;
    			break;
    		}
    	}
    	if (!localeFound)
			throw StandardException.newException(
					SQLState.COLLATOR_NOT_FOUND_FOR_LOCALE, 
					databaseLocale.toString());
    	
    	RuleBasedCollator collator = (RuleBasedCollator)Collator.getInstance(databaseLocale);

		if (strength != -1)
			collator.setStrength(strength);

		return collator;
    }
    /** 
     * @see DataValueFactory#getNull(int, int)
     */
    public DataValueDescriptor getNull(int formatId, int collationType) 
    throws StandardException {

    	//For StoredFormatIds.SQL_DECIMAL_ID, different implementations are 
    	//required for different VMs. getNullDecimal method is not static and 
    	//hence can't be called in the static getNullDVDWithUCS_BASICcollation
    	//method in this class. That is why StoredFormatIds.SQL_DECIMAL_ID is 
    	//getting handled here.
    	if (formatId == StoredFormatIds.SQL_DECIMAL_ID)
    		return getNullDecimal(null);
		else {
			DataValueDescriptor returnDVD = 
				DataValueFactoryImpl.getNullDVDWithUCS_BASICcollation(formatId);
			//If we are dealing with default collation, then we have got the
			//right DVD already. Just return it.
			if (collationType == StringDataValue.COLLATION_TYPE_UCS_BASIC)
				return returnDVD;			
			//If we are dealing with territory based collation and returnDVD is 
			//of type StringDataValue, then we need to return a StringDataValue   
			//with territory based collation.
			if (returnDVD instanceof StringDataValue) 
				return ((StringDataValue)returnDVD).getValue(getCharacterCollator(collationType));
			else
				return returnDVD;			
		}
    }
    
    /**
     * This method will return a DVD based on the formatId. It doesn't take
     * into account the collation that should be associated with collation
     * sensitive DVDs, which are all the character type DVDs. Such DVDs 
     * returned from this method have default UCS_BASIC collation associated
     * with them. If collation associated should be terriotry based, then that
     * needs to be handled by the caller of this method. An example of such 
     * code in the caller can be seen in DataValueFactory.getNull method.
     * 
     * Another thing to note is this method does not deal with format id
     * associated with decimal. This is because different implementation are
     * required for different VMs. This is again something that needs to be
     * handled by the caller. An example of such code in the caller can be 
     * seen in DataValueFactory.getNull method.
     *  
     * @param formatId Return a DVD based on the format id
     * @return DataValueDescriptor with default collation of UCS_BASIC 
     */
    public static DataValueDescriptor getNullDVDWithUCS_BASICcollation(
    int formatId) {

        switch (formatId) {
        /* Wrappers */
        case StoredFormatIds.SQL_BIT_ID: return new SQLBit();
        case StoredFormatIds.SQL_BOOLEAN_ID: return new SQLBoolean();
        case StoredFormatIds.SQL_CHAR_ID: return new SQLChar();
        case StoredFormatIds.SQL_DATE_ID: return new SQLDate();
        case StoredFormatIds.SQL_DOUBLE_ID: return new SQLDouble();
        case StoredFormatIds.SQL_INTEGER_ID: return new SQLInteger();
        case StoredFormatIds.SQL_LONGINT_ID: return new SQLLongint();
        case StoredFormatIds.SQL_REAL_ID: return new SQLReal();
        case StoredFormatIds.SQL_REF_ID: return new SQLRef();
        case StoredFormatIds.SQL_SMALLINT_ID: return new SQLSmallint();
        case StoredFormatIds.SQL_TIME_ID: return new SQLTime();
        case StoredFormatIds.SQL_TIMESTAMP_ID: return new SQLTimestamp();
        case StoredFormatIds.SQL_TINYINT_ID: return new SQLTinyint();
        case StoredFormatIds.SQL_VARCHAR_ID: return new SQLVarchar();
        case StoredFormatIds.SQL_LONGVARCHAR_ID: return new SQLLongvarchar();
        case StoredFormatIds.SQL_VARBIT_ID: return new SQLVarbit();
        case StoredFormatIds.SQL_LONGVARBIT_ID: return new SQLLongVarbit();
        case StoredFormatIds.SQL_USERTYPE_ID_V3: return new UserType();
        case StoredFormatIds.SQL_BLOB_ID: return new SQLBlob();
        case StoredFormatIds.SQL_CLOB_ID: return new SQLClob();
        case StoredFormatIds.XML_ID: return new XML();
        case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID: 
        // This is an specific implementation of RowLocation, known to be
        // a DTD.  
             return(
                 new org.apache.derby.impl.store.access.heap.HeapRowLocation());
        default:return null;
        }
    }

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
