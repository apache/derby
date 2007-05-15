/*

   Derby - Class org.apache.derby.iapi.types.SQLTime

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.db.DatabaseContext;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.NumberDataValue;

import org.apache.derby.iapi.types.DataType;
import org.apache.derby.iapi.services.i18n.LocaleFinder;
import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.util.StringUtil;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.PreparedStatement;

import java.util.Calendar;
import java.util.GregorianCalendar;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.text.DateFormat;
import java.text.ParseException;

/**
 * This contains an instance of a SQL Time
 * Our current implementation doesn't implement time precision so the fractional
 * seconds portion of the time is always 0.  The default when no time precision
 * is specified is 0 fractional seconds.  A SQL Time without timezone information
 * is assumed to be in the local time zone.  The local time is stored as is
 * and doesn't change if the timezone changes. This is in conformance with the
 * SQL99 standard.  The SQL92 standard indicates that the time is in GMT and
 * changes with the timezone.  The SQL99 standard clarifies this to allow time without
 * timezoned to be stored as the local time.
 * <p>
 * Time is stored as two ints.  The first int represents hour, minute, second 
 * and the second represents fractional seconds (currently 0 since we don't support
 * time precision)
 * 	encodedTime = -1 indicates null
 *
 * PERFORMANCE OPTIMIZATION:
 *	The java.sql.Time object is only instantiated on demand for performance
 * 	reasons.
 */

public final class SQLTime extends DataType
						implements DateTimeDataValue
{

	private int		encodedTime;
	private int		encodedTimeFraction; //currently always 0 since we don't
											 //support time precision

	// The cached value.toString()
	private String	valueString;

	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLTime.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE + ClassSize.estimateMemoryUsage( valueString);
    } // end of estimateMemoryUsage

	public String getString()
	{
		if (!isNull())
		{
			if (valueString == null)
			{
				valueString = encodedTimeToString(encodedTime);
			}
			return valueString;
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				if (valueString != null)
				{
					SanityManager.THROWASSERT(
						"valueString expected to be null, not " +
						valueString);
				}
			}
			return null;
		}
	}

    int getEncodedTime()
    {
        return encodedTime;
    }

	/**
     * Convert a SQL TIME to a JDBC java.sql.Timestamp.
     * 
     * Behaviour is to set the date portion of the Timestamp
     * to the actual current date, which may not match the
     * SQL CURRENT DATE, which remains fixed for the lifetime
     * of a SQL statement. JDBC drivers (especially network client drivers)
     * could not be expected to fetch the CURRENT_DATE SQL value
     * on every query that involved a TIME value, so the current
     * date as seen by the JDBC client was picked as the logical behaviour.
     * See DERBY-1811.
	 */
	public Timestamp getTimestamp( Calendar cal)
	{
		if (isNull())
			return null;
		else
		{
            if( cal == null)
            {
                // Calendar initialized to current date and time.
                cal = new GregorianCalendar(); 
            }
            else
            {
                cal.clear();
                // Set Calendar to current date and time
                // to pick up the current date. Time portion
                // will be overridden by this value's time.
                cal.setTimeInMillis(System.currentTimeMillis());
            }
            
            SQLTime.setTimeInCalendar(cal, encodedTime);
          
            // Derby's resolution for the TIME type is only seconds.
			cal.set(Calendar.MILLISECOND, 0);
            
			return new Timestamp(cal.getTimeInMillis());
		}
	}

	public Object getObject()
	{
		return getTime( (Calendar) null);
	}
		
	public int getLength()
	{
		return 8;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
		return "TIME";
	}


	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_TIME_ID;
	}

	/** 
		@exception IOException error writing data

	*/
	public void writeExternal(ObjectOutput out) throws IOException {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isNull(), "writeExternal() is not supposed to be called for null values.");

		out.writeInt(encodedTime);
		out.writeInt(encodedTimeFraction);
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on error reading the object
	 */
	public void readExternal(ObjectInput in) throws IOException
	{
		encodedTime = in.readInt();
		encodedTimeFraction = in.readInt();
		// reset cached values
		valueString = null;
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException
	{
		encodedTime = in.readInt();
		encodedTimeFraction = in.readInt();
		// reset cached values
		valueString = null;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
	{
		// Call constructor with all of our info
		return new SQLTime(encodedTime, encodedTimeFraction);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLTime();
	}
	/**
	 * @see org.apache.derby.iapi.services.io.Storable#restoreToNull
	 *
	 */

	public void restoreToNull()
	{
		encodedTime = -1;
		encodedTimeFraction = 0;

		// clear cached valueString
		valueString = null;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException, StandardException
	{
			restoreToNull();
			encodedTime = computeEncodedTime(resultSet.getTime(colNumber));
			//need to set encodedTimeFraction when we implement time precision
	}

	/**
	 * Orderable interface
	 *
	 *
	 * @see org.apache.derby.iapi.types.Orderable
	 *
	 * @exception StandardException thrown on failure
	 */
	public int compare(DataValueDescriptor other)
		throws StandardException
	{
		/* Use compare method from dominant type, negating result
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < other.typePrecedence())
		{
			return - (other.compare(this));
		}

		boolean thisNull, otherNull;

		thisNull = this.isNull();
		otherNull = other.isNull();

		/*
		 * thisNull otherNull	return
		 *	T		T		 	0	(this == other)
		 *	F		T		 	-1 	(this < other)
		 *	T		F		 	1	(this > other)
		 */
		if (thisNull || otherNull)
		{
			if (!thisNull)		// otherNull must be true
				return -1;
			if (!otherNull)		// thisNull must be true
				return 1;
			return 0;
		}

		/*
			Neither are null compare them 
		 */

		int comparison;

		/* get the comparison time values */
		int otherEncodedTime = 0;

		/* if the argument is another Time look up the value
		 * we have already taken care of Null
		 * ignoring encodedTimeFraction for now since it is always 0
		 * - need to change this when we support TIME(precision)
		 */
		if (other instanceof SQLTime)
		{
			otherEncodedTime=((SQLTime)other).encodedTime;
		}
		else 
		{
			/* O.K. have to do it the hard way and calculate the numeric value
			 * from the value
			 */
			otherEncodedTime = computeEncodedTime(other.getTime( (Calendar) null));
		}
		if (encodedTime < otherEncodedTime)
			comparison = -1;
		else if (encodedTime > otherEncodedTime)
			comparison = 1;
		else
			comparison = 0;

		return comparison;
	}

	/**
		@exception StandardException thrown on error
	 */
	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
		throws StandardException
	{
		if (!orderedNulls)		// nulls are unordered
		{
			if (this.isNull() || other.isNull())
				return unknownRV;
		}

		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}

	/*
	** Class interface
	*/

	/*
	** Constructors
	*/

	/** no-arg constructor required by Formattable */
	public SQLTime() 
	{ 
		encodedTime = -1;	//null value
	}

	public SQLTime(Time value) throws StandardException
	{
		parseTime(value);
	}

    private void parseTime(java.util.Date value) throws StandardException
	{
		encodedTime = computeEncodedTime(value);
	}

	private SQLTime(int encodedTime, int encodedTimeFraction) {
		this.encodedTime = encodedTime;
		this.encodedTimeFraction = encodedTimeFraction;
	}


    /**
     * Construct a time from a string. The allowed time formats are:
     *<ol>
     *<li>old ISO and IBM European standard: hh.mm[.ss]
     *<li>IBM USA standard: hh[:mm] {AM | PM}
     *<li>JIS & current ISO: hh:mm[:ss]
     *</ol>
     * 
     * @exception Standard exception if the syntax is invalid or the value is out of range.
     */
    public SQLTime( String timeStr, boolean isJdbcEscape, LocaleFinder localeFinder)
        throws StandardException
    {
        parseTime( timeStr, isJdbcEscape, localeFinder, (Calendar) null);
    }
    
    /**
     * Construct a time from a string. The allowed time formats are:
     *<ol>
     *<li>old ISO and IBM European standard: hh.mm[.ss]
     *<li>IBM USA standard: hh[:mm] {AM | PM}
     *<li>JIS & current ISO: hh:mm[:ss]
     *</ol>
     * 
     * @exception Standard exception if the syntax is invalid or the value is out of range.
     */
    public SQLTime( String timeStr, boolean isJdbcEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        parseTime( timeStr, isJdbcEscape, localeFinder, cal);
    }

    private static final char IBM_EUR_SEPARATOR = '.';
    private static final char[] IBM_EUR_SEPARATOR_OR_END = {IBM_EUR_SEPARATOR, (char) 0};
    static final char JIS_SEPARATOR = ':';
    private static final char[] US_OR_JIS_MINUTE_END = {JIS_SEPARATOR, ' ', (char) 0};
    private static final char[] ANY_SEPARATOR = { '.', ':', ' '};
    private static final String[] AM_PM = {"AM", "PM"};
    private static final char[] END_OF_STRING = {(char) 0};
    
    private void parseTime( String timeStr, boolean isJdbcEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        boolean validSyntax = true;
        DateTimeParser parser = new DateTimeParser( timeStr);
        StandardException thrownSE = null;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int amPm = -1;
        try
        {
            if( parser.nextSeparator() == SQLTimestamp.DATE_SEPARATOR)
            {
                    encodedTime = SQLTimestamp.parseDateOrTimestamp( parser, true)[1];
                    valueString = parser.getTrimmedString();
                    return;
            }
            hour = parser.parseInt( 2, true, ANY_SEPARATOR, false);
            switch( parser.getCurrentSeparator())
            {
            case IBM_EUR_SEPARATOR:
                if( isJdbcEscape)
                {
                    validSyntax = false;
                    break;
                }
                minute = parser.parseInt( 2, false, IBM_EUR_SEPARATOR_OR_END, false);
                if( parser.getCurrentSeparator() == IBM_EUR_SEPARATOR)
                    second = parser.parseInt( 2, false, END_OF_STRING, false);
                break;

            case ':':
                // IBM USA or JIS (new ISO)
                minute = parser.parseInt( 2, false, US_OR_JIS_MINUTE_END, false);
                switch( parser.getCurrentSeparator())
                {
                case ' ':
                    // IBM USA with minutes
                    if( isJdbcEscape)
                    {
                        validSyntax = false;
                        break;
                    }
                    amPm = parser.parseChoice( AM_PM);
                    parser.checkEnd();
                    break;

                case JIS_SEPARATOR:
                    second = parser.parseInt( 2, false, END_OF_STRING, false);
                    break;

                    // default is end of string, meaning that the seconds part is zero.
                }
                break;

            case ' ':
                // IBM USA with minutes omitted
                if( isJdbcEscape)
                {
                    validSyntax = false;
                    break;
                }
                amPm = parser.parseChoice( AM_PM);
                break;

            default:
                validSyntax = false;
            }
        }
        catch( StandardException se)
        {
            validSyntax = false;
            thrownSE = se;
        }
        if( validSyntax)
        {
            if( amPm == 0) // AM
            {
                if( hour == 12)
                {
                    if( minute == 0 && second == 0)
                        hour = 24;
                    else
                        hour = 0;
                }
                else if( hour > 12)
                    throw StandardException.newException( SQLState.LANG_DATE_RANGE_EXCEPTION);
            }
            else if( amPm == 1) // PM
            {
                if( hour < 12)
                    hour += 12;
                else if( hour > 12)
                    throw StandardException.newException( SQLState.LANG_DATE_RANGE_EXCEPTION);
            }
            valueString = parser.checkEnd();
            encodedTime = computeEncodedTime( hour, minute, second);
        }
        else
        {
            // See if it is a localized time or timestamp
            timeStr = StringUtil.trimTrailing( timeStr);
            DateFormat timeFormat = null;
            if(localeFinder == null)
                timeFormat = DateFormat.getTimeInstance();
            else if( cal == null)
                timeFormat = localeFinder.getTimeFormat();
            else
                timeFormat = (DateFormat) localeFinder.getTimeFormat().clone();
            if( cal != null)
                timeFormat.setCalendar( cal);
            try
            {
                encodedTime = computeEncodedTime( timeFormat.parse( timeStr), cal);
            }
            catch( ParseException pe)
            {
                // Maybe it is a localized timestamp
                try
                {
                    encodedTime = SQLTimestamp.parseLocalTimestamp( timeStr, localeFinder, cal)[1];
                }
                catch( ParseException pe2)
                {
                    if( thrownSE != null)
                        throw thrownSE;
                    throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
                }
            }
            valueString = timeStr;
        }
    } // end of parseTime

	/**
	 * Set the value from a correctly typed Time object.
	 * @throws StandardException 
	 */
	void setObject(Object theValue) throws StandardException
	{
		setValue((Time) theValue);
	}
    
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		if (theValue instanceof SQLTime) {
			restoreToNull();

			SQLTime tvst = (SQLTime) theValue;
			encodedTime = tvst.encodedTime;
			encodedTimeFraction = tvst.encodedTimeFraction;

		}
        else
        {
            Calendar cal = new GregorianCalendar();
			setValue(theValue.getTime( cal), cal);
        }
	}

	/**
		@see DateTimeDataValue#setValue

		@exception StandardException thrown on failure.
	 */
	public void setValue(Time value, Calendar cal) throws StandardException
	{
		restoreToNull();
		encodedTime = computeEncodedTime(value, cal);
	}

	/**
		@see DateTimeDataValue#setValue

		@exception StandardException thrown on failure.
	 */
	public void setValue(Timestamp value, Calendar cal) throws StandardException
	{
		restoreToNull();
		encodedTime = computeEncodedTime(value, cal);
	}


	public void setValue(String theValue)
	    throws StandardException
	{
		restoreToNull();
		if (theValue != null)
        {
            DatabaseContext databaseContext = (DatabaseContext) ContextService.getContext(DatabaseContext.CONTEXT_ID);
            parseTime( theValue,
                       false,
                       (databaseContext == null) ? null : databaseContext.getDatabase(),
                       (Calendar) null);
        }
	}

	/*
	** SQL Operators
	*/

    NumberDataValue nullValueInt() {
        return new SQLInteger();
    }

	/**
	 * @see DateTimeDataValue#getYear
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getYear(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getYear", "Time");
	}

	/**
	 * @see DateTimeDataValue#getMonth
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMonth(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getMonth", "Time");
	}

	/**
	 * @see DateTimeDataValue#getDate
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getDate(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getDate", "Time");
	}

	/**
	 * @see DateTimeDataValue#getHours
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getHours(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {    
            return SQLDate.setSource(getHour(encodedTime), result);
        }
	}

	/**
	 * @see DateTimeDataValue#getMinutes
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMinutes(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {    
            return SQLDate.setSource(getMinute(encodedTime), result);
        }
	}

	/**
	 * @see DateTimeDataValue#getSeconds
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getSeconds(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {    
            return SQLDate.setSource(getSecond(encodedTime), result);
        }
	}

	/*
	** String display of value
	*/

	public String toString()
	{
		if (isNull())
		{
			return "NULL";
		}
		else
		{
			return getTime( (Calendar) null).toString();
		}
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		if (isNull())
		{
			return 0;
		}
		// add 1 since 0 represents a valid time
		return encodedTime + encodedTimeFraction + 1;

	}

	/** @see DataValueDescriptor#typePrecedence */
	public int	typePrecedence()
	{
		return TypeId.TIME_PRECEDENCE;
	}

	/**
	 * Check if the value is null.  
	 *
	 * @return Whether or not value is logically null.
	 */
	public final boolean isNull()
	{
		return (encodedTime ==  -1);
	}

	/**
	 * Get the time value 
	 * Since this is a JDBC object we use the JDBC definition
	 * we use the JDBC definition, see JDBC API Tutorial and Reference
	 * section 47.3.12
	 * Date is set to Jan. 1, 1970
	 *
	 * @return	The localized time value.
	 */
	public Time getTime(java.util.Calendar cal)
	{
		if (isNull())
			return null;
        
        // Derby's SQL TIME type only has second resolution
        // so pass in 0 for nano-seconds
        return getTime(cal, encodedTime, 0);
	}
    
    /**
     * Set the time portion of a date-time value into
     * the passed in Calendar object from its encodedTime
     * value. Note that this is only the time down
     * to a resolution of one second. Only the HOUR_OF_DAY,
     * MINUTE and SECOND fields are modified. The remaining
     * state of the Calendar is not modified.
     */
    static void setTimeInCalendar(Calendar cal, int encodedTime)
    {
        cal.set(Calendar.HOUR_OF_DAY, getHour(encodedTime));
        cal.set(Calendar.MINUTE, getMinute(encodedTime));
        cal.set(Calendar.SECOND, getSecond(encodedTime));        
    }
    
    /**
     * Get a java.sql.Time object from an encoded time
     * and nano-second value. As required by JDBC the
     * date component of the Time object will be set to
     * Jan. 1, 1970
     * @param cal Calendar to use for conversion
     * @param encodedTime Derby encoded time value
     * @param nanos number of nano-seconds.
     * @return Valid Time object.
     */
    static Time getTime(Calendar cal, int encodedTime, int nanos)
    {
        if( cal == null)
            cal = new GregorianCalendar();
        
        cal.clear();
        
        cal.set(1970, Calendar.JANUARY, 1);

        SQLTime.setTimeInCalendar(cal, encodedTime);

        cal.set(Calendar.MILLISECOND, nanos/1000000);
        
        return new Time(cal.getTimeInMillis());
    }
    
    
	/**
	 * Get the encoded hour value (may be different than hour value for
	 *  	current timezone if value encoded in a different timezone)
	 *
	 * @return	hour value
	 */
	protected static int getHour(int encodedTime)
	{
		return (encodedTime >>> 16) & 0xff;
	}
	/**
	 * Get the encoded minute value (may be different than the minute value for
	 *  	current timezone if value encoded in a different timezone)
	 *
	 * @return	minute value
	 */
	protected static int getMinute(int encodedTime)
	{
		return ((encodedTime >>> 8) & 0xff);
	}
	/**
	 * Get the encoded second value (may be different than the second value for
	 *  	current timezone if value encoded in a different timezone)
	 *
	 * @return	second value
	 */
	protected static int getSecond(int encodedTime)
	{
		return (encodedTime & 0xff);
	}
	/**
	 *	Calculate the encoded time from a Calendar object
	 *	encoded time is hour << 16 + min << 8 + sec
	 *  this function is also used by SQLTimestamp 
	 *
	 * @param	cal calendar with time set
	 * @return	encoded time
     *
     * @exception StandardException if the time is not in the DB2 range
	 */
	static int computeEncodedTime(Calendar cal) throws StandardException
	{
		return computeEncodedTime(cal.get(Calendar.HOUR_OF_DAY),
                                  cal.get(Calendar.MINUTE),
                                  cal.get(Calendar.SECOND));
	}

    static int computeEncodedTime( int hour, int minute, int second) throws StandardException
    {
        if( hour == 24)
        {
            if( minute != 0 || second != 0)
                throw StandardException.newException( SQLState.LANG_DATE_RANGE_EXCEPTION);
        }
        else if( hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59)
            throw StandardException.newException( SQLState.LANG_DATE_RANGE_EXCEPTION);

        return (hour << 16) + (minute << 8) + second;
    }

    /**
     * Convert a time to a JDBC escape format string
     *
     * @param hour
     * @param minute
     * @param second
     * @param sb The resulting string is appended to this StringBuffer
     */
    static void timeToString( int hour, int minute, int second, StringBuffer sb)
    {
		String hourStr = Integer.toString( hour);
		String minStr = Integer.toString( minute);
		String secondStr = Integer.toString( second);
		if (hourStr.length() == 1)
			sb.append("0");
		sb.append( hourStr);
		sb.append( JIS_SEPARATOR);
		if (minStr.length() == 1)
			sb.append("0");
		sb.append(minStr);
		sb.append( JIS_SEPARATOR);
		if (secondStr.length() == 1)
			sb.append("0");
		sb.append(secondStr);
    } // end of timeToString

	/**
	 * Get the String version from the encodedTime.
	 *
	 * @return	 string value.
	 */
	protected static String encodedTimeToString(int encodedTime)
	{
		StringBuffer vstr = new StringBuffer();
        timeToString( SQLTime.getHour(encodedTime), SQLTime.getMinute(encodedTime), SQLTime.getSecond(encodedTime), vstr);
		return vstr.toString();
	}

	// International Support

	/**
	 * International version of getString(). Overrides getNationalString
	 * in DataType for date, time, and timestamp.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected String getNationalString(LocaleFinder localeFinder) throws StandardException
	{
		if (isNull())
		{
			return getString();
		}

		return localeFinder.getTimeFormat().format(getTime( (Calendar) null));
	}

	/**
	 * Compute encoded time value
	 * Time is represented by hour << 16 + minute << 8 + seconds
	 */
	private	int computeEncodedTime(java.util.Date value) throws StandardException
	{
        return computeEncodedTime( value, (Calendar) null);
    }

    static int computeEncodedTime(java.util.Date value, Calendar currentCal) throws StandardException
    {
        if (value == null)
			return -1;
        if( currentCal == null)
            currentCal = new GregorianCalendar();
		currentCal.setTime(value);
		return computeEncodedTime(currentCal);
	}

     /** Adding this method to ensure that super class' setInto method doesn't get called
      * that leads to the violation of JDBC spec( untyped nulls ) when batching is turned on.
      */
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {

		      ps.setTime(position, getTime((Calendar) null));
   }


    /**
     * Add a number of intervals to a datetime value. Implements the JDBC escape TIMESTAMPADD function.
     *
     * @param intervalType One of FRAC_SECOND_INTERVAL, SECOND_INTERVAL, MINUTE_INTERVAL, HOUR_INTERVAL,
     *                     DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, QUARTER_INTERVAL, or YEAR_INTERVAL
     * @param intervalCount The number of intervals to add
     * @param currentDate Used to convert time to timestamp
     * @param resultHolder If non-null a DateTimeDataValue that can be used to hold the result. If null then
     *                     generate a new holder
     *
     * @return startTime + intervalCount intervals, as a timestamp
     *
     * @exception StandardException
     */
    public DateTimeDataValue timestampAdd( int intervalType,
                                           NumberDataValue intervalCount,
                                           java.sql.Date currentDate,
                                           DateTimeDataValue resultHolder)
        throws StandardException
    {
        return toTimestamp( currentDate).timestampAdd( intervalType, intervalCount, currentDate, resultHolder);
    }

    private SQLTimestamp toTimestamp(java.sql.Date currentDate) throws StandardException
    {
        return new SQLTimestamp( SQLDate.computeEncodedDate( currentDate, (Calendar) null),
                                 getEncodedTime(),
                                 0 /* nanoseconds */);
    }
    
    /**
     * Finds the difference between two datetime values as a number of intervals. Implements the JDBC
     * TIMESTAMPDIFF escape function.
     *
     * @param intervalType One of FRAC_SECOND_INTERVAL, SECOND_INTERVAL, MINUTE_INTERVAL, HOUR_INTERVAL,
     *                     DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, QUARTER_INTERVAL, or YEAR_INTERVAL
     * @param time1
     * @param currentDate Used to convert time to timestamp
     * @param resultHolder If non-null a NumberDataValue that can be used to hold the result. If null then
     *                     generate a new holder
     *
     * @return the number of intervals by which this datetime is greater than time1
     *
     * @exception StandardException
     */
    public NumberDataValue timestampDiff( int intervalType,
                                          DateTimeDataValue time1,
                                          java.sql.Date currentDate,
                                          NumberDataValue resultHolder)
        throws StandardException
    {
        return toTimestamp( currentDate ).timestampDiff( intervalType, time1, currentDate, resultHolder);
    }
}

