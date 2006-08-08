/*

   Derby - Class org.apache.derby.iapi.types.SQLTimestamp

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.db.DatabaseContext;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.DateTimeDataValue;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.context.ContextService;
 
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.DataType;
import org.apache.derby.iapi.services.i18n.LocaleFinder;
import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.util.ReuseFactory;

import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLTime;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import java.util.Calendar;
import java.util.GregorianCalendar;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.text.DateFormat;
import java.text.ParseException;

/**
 * This contains an instance of a SQL Timestamp object.
 * <p>
 * SQLTimestamp is stored in 3 ints - an encoded date, an encoded time and
 *		nanoseconds
 * encodedDate = 0 indicates a null WSCTimestamp
 *
 * SQLTimestamp is similar to SQLTimestamp, but it does conserves space by not keeping a GregorianCalendar object
 *
 * PERFORMANCE OPTIMIZATION:
 *	We only instantiate the value field when required due to the overhead of the
 *	Date methods.
 *	Thus, use isNull() instead of "value == null" and
 *	getTimestamp() instead of using value directly.
 */

public final class SQLTimestamp extends DataType
						implements DateTimeDataValue
{

    static final int MAX_FRACTION_DIGITS = 6; // Only microsecond resolution on conversion to/from strings
    static final int FRACTION_TO_NANO = 1000; // 10**(9 - MAX_FRACTION_DIGITS)

    static final int ONE_BILLION = 1000000000;
    
	private int	encodedDate;
	private int	encodedTime;
	private int	nanos;

	// The cached value.toString()
	private String	valueString;

	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLTimestamp.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE + ClassSize.estimateMemoryUsage( valueString);
        return sz;
    } // end of estimateMemoryUsage

	public String getString()
	{
		if (!isNull())
		{
			if (valueString == null)
			{
				valueString = getTimestamp((Calendar) null).toString();
                /* The java.sql.Timestamp.toString() method is supposed to return a string in
                 * the JDBC escape format. However the JDK 1.3 libraries truncate leading zeros from
                 * the year. This is not acceptable to DB2. So add leading zeros if necessary.
                 */
                int separatorIdx = valueString.indexOf( '-');
                if( separatorIdx >= 0 && separatorIdx < 4)
                {
                    StringBuffer sb = new StringBuffer();
                    for( ; separatorIdx < 4; separatorIdx++)
                        sb.append('0');
                    sb.append( valueString);
                    valueString = sb.toString();
                }
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


	/**
		getDate returns the date portion of the timestamp
		Time is set to 00:00:00.0
		Since Date is a JDBC object we use the JDBC definition
		for the time portion.  See JDBC API Tutorial, 47.3.12.

		@exception StandardException thrown on failure
	 */
	public Date	getDate( Calendar cal) throws StandardException
	{
		if (isNull())
			return null;
        return newDate(cal);
    }

    private Date newDate(java.util.Calendar cal) throws StandardException
    {
        if( cal == null)
            cal = new GregorianCalendar();
		cal.set(Calendar.YEAR, SQLDate.getYear(encodedDate) );
		cal.set(Calendar.MONTH, SQLDate.getMonth(encodedDate)-1);
		cal.set(Calendar.DATE, SQLDate.getDay(encodedDate) );
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return new Date(cal.getTime().getTime());
	}

	/**
		getTime returns the time portion of the timestamp
		Date is set to 1970-01-01
		Since Time is a JDBC object we use the JDBC definition
		for the date portion.  See JDBC API Tutorial, 47.3.12.
		@exception StandardException thrown on failure
	 */
	public Time	getTime( Calendar cal) throws StandardException
	{
		if (isNull())
			return null;
        return newTime(cal);
    }

    private Time newTime(java.util.Calendar cal) throws StandardException
    {
        if( cal == null)
            cal = new GregorianCalendar();
		cal.set(Calendar.YEAR, 1970);
		cal.set(Calendar.MONTH, Calendar.JANUARY);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, SQLTime.getHour(encodedTime));
		cal.set(Calendar.MINUTE, SQLTime.getMinute(encodedTime));
		cal.set(Calendar.SECOND, SQLTime.getSecond(encodedTime));
		cal.set(Calendar.MILLISECOND, (int)(nanos/1E06));
		return new Time(cal.getTime().getTime());
	}

	public Object getObject()
	{
		return getTimestamp((Calendar) null);
	}
		
	/* get storage length */
	public int getLength()
	{
		return 12;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
		return "TIMESTAMP";
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_TIMESTAMP_ID;
	}

	/** 
		@exception IOException error writing data

	*/
	public void writeExternal(ObjectOutput out) throws IOException {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isNull(), "writeExternal() is not supposed to be called for null values.");

		/*
		** Timestamp is written out 3 ints, encoded date, encoded time, and
		** nanoseconds
		*/
		out.writeInt(encodedDate);
		out.writeInt(encodedTime);
		out.writeInt(nanos);
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on error reading the object
	 */
	public void readExternal(ObjectInput in) throws IOException
	{
		encodedDate = in.readInt();
		encodedTime = in.readInt();
		nanos = in.readInt();
		// reset cached values
		valueString = null;
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException
	{
		encodedDate = in.readInt();
		encodedTime = in.readInt();
		nanos = in.readInt();
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
		return new SQLTimestamp(encodedDate, encodedTime, nanos);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLTimestamp();
	}
	/**
	 * @see org.apache.derby.iapi.services.io.Storable#restoreToNull
	 *
	 */
	public void restoreToNull()
	{
		// clear numeric representation
		encodedDate = 0;
		encodedTime = 0;
		nanos = 0;

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
			setValue(resultSet.getTimestamp(colNumber), (Calendar) null);
	}

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
		/* get the comparison date values */
		int otherEncodedDate = 0;
		int otherEncodedTime = 0;
		int otherNanos = 0;

		/* if the argument is another SQLTimestamp, look up the value
		 */
		if (other instanceof SQLTimestamp)
		{
			SQLTimestamp st = (SQLTimestamp)other;
			otherEncodedDate= st.encodedDate;
			otherEncodedTime= st.encodedTime;
			otherNanos= st.nanos;
		}
		else 
		{
			/* O.K. have to do it the hard way and calculate the numeric value
			 * from the value
			 */
			Calendar cal = new GregorianCalendar();
			Timestamp otherts = other.getTimestamp(cal);
			otherEncodedDate = SQLTimestamp.computeEncodedDate(otherts, cal);
			otherEncodedTime = SQLTimestamp.computeEncodedTime(otherts, cal);
			otherNanos = otherts.getNanos();
		}
		if (encodedDate < otherEncodedDate)
			comparison = -1;
		else if (encodedDate > otherEncodedDate)
			comparison = 1;
		else if (encodedTime < otherEncodedTime)
			comparison = -1;
		else if (encodedTime > otherEncodedTime)
			comparison = 1;
		else if (nanos < otherNanos)
			comparison = -1;
		else if (nanos > otherNanos)
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
			if (this.isNull() || ((DataValueDescriptor)other).isNull())
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
	public SQLTimestamp() { }


	public SQLTimestamp(Timestamp value) throws StandardException
	{
		setValue(value, (Calendar) null);
	}

	SQLTimestamp(int encodedDate, int encodedTime, int nanos) {

		this.encodedDate = encodedDate;
		this.encodedTime = encodedTime;
		this.nanos = nanos;
	}

    public SQLTimestamp( DataValueDescriptor date, DataValueDescriptor time) throws StandardException
    {
        Calendar cal = null;
        if( date == null || date.isNull()
            || time == null || time.isNull())
            return;
        if( date instanceof SQLDate)
        {
            SQLDate sqlDate = (SQLDate) date;
            encodedDate = sqlDate.getEncodedDate();
        }
        else
        {
            cal = new GregorianCalendar();
            encodedDate = computeEncodedDate( date.getDate( cal), cal);
        }
        if( time instanceof SQLTime)
        {
            SQLTime sqlTime = (SQLTime) time;
            encodedTime = sqlTime.getEncodedTime();
        }
        else
        {
            if( cal == null)
                cal = new GregorianCalendar();
            encodedTime = computeEncodedTime( time.getTime( cal), cal);
        }
    }

    /**
     * Construct a timestamp from a string. The allowed formats are:
     *<ol>
     *<li>JDBC escape: yyyy-mm-dd hh:mm:ss[.fffff]
     *<li>IBM: yyyy-mm-dd-hh.mm.ss[.nnnnnn]
     *</ol>
     * The format is specified by a parameter to the constructor. Leading zeroes may be omitted from the month, day,
     * and hour part of the timestamp. The microsecond part may be omitted or truncated.
     */
    public SQLTimestamp( String timestampStr, boolean isJDBCEscape, LocaleFinder localeFinder)
        throws StandardException
    {
        parseTimestamp( timestampStr, isJDBCEscape,localeFinder, (Calendar) null);
    }
    
    /**
     * Construct a timestamp from a string. The allowed formats are:
     *<ol>
     *<li>JDBC escape: yyyy-mm-dd hh:mm:ss[.fffff]
     *<li>IBM: yyyy-mm-dd-hh.mm.ss[.nnnnnn]
     *</ol>
     * The format is specified by a parameter to the constructor. Leading zeroes may be omitted from the month, day,
     * and hour part of the timestamp. The microsecond part may be omitted or truncated.
     */
    public SQLTimestamp( String timestampStr, boolean isJDBCEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        parseTimestamp( timestampStr, isJDBCEscape, localeFinder, cal);
    }

    static final char DATE_SEPARATOR = '-';
    private static final char[] DATE_SEPARATORS = { DATE_SEPARATOR};
    private static final char IBM_DATE_TIME_SEPARATOR = '-';
    private static final char ODBC_DATE_TIME_SEPARATOR = ' ';
    private static final char[] DATE_TIME_SEPARATORS = {IBM_DATE_TIME_SEPARATOR, ODBC_DATE_TIME_SEPARATOR};
    private static final char[] DATE_TIME_SEPARATORS_OR_END
    = {IBM_DATE_TIME_SEPARATOR, ODBC_DATE_TIME_SEPARATOR, (char) 0};
    private static final char IBM_TIME_SEPARATOR = '.';
    private static final char ODBC_TIME_SEPARATOR = ':';
    private static final char[] TIME_SEPARATORS = {IBM_TIME_SEPARATOR, ODBC_TIME_SEPARATOR};
    private static final char[] TIME_SEPARATORS_OR_END = {IBM_TIME_SEPARATOR, ODBC_TIME_SEPARATOR, (char) 0};
    private static final char[] END_OF_STRING = {(char) 0};
    
    private void parseTimestamp( String timestampStr, boolean isJDBCEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        StandardException thrownSE = null;
        DateTimeParser parser = new DateTimeParser( timestampStr);
        try
        {
            int[] dateTimeNano = parseDateOrTimestamp( parser, true);
            encodedDate = dateTimeNano[0];
            encodedTime = dateTimeNano[1];
            nanos = dateTimeNano[2];
            valueString = parser.getTrimmedString();
            return;
        }
        catch( StandardException se)
        {
            thrownSE = se;
        }
        // see if it is a localized timestamp
        try
        {
            timestampStr = StringUtil.trimTrailing( timestampStr);
            int[] dateAndTime = parseLocalTimestamp( timestampStr, localeFinder, cal);
            encodedDate = dateAndTime[0];
            encodedTime = dateAndTime[1];
            valueString = timestampStr;
            return;
        }
        catch( ParseException pe){}
        catch( StandardException se){}
        if( thrownSE != null)
            throw thrownSE;
        throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
    } // end of parseTimestamp

    /**
     * Parse a localized timestamp.
     *
     * @param str the timestamp string, with trailing blanks removed.
     * @param localeFinder
     *
     * @return a {encodedDate, encodedTime} array.
     *
     * @exception ParseException If the string is not a valid timestamp.
     */
    static int[] parseLocalTimestamp( String str, LocaleFinder localeFinder, Calendar cal)
        throws StandardException, ParseException
    {
        DateFormat timestampFormat = null;
        if(localeFinder == null)
            timestampFormat = DateFormat.getDateTimeInstance();
        else if( cal == null)
            timestampFormat = localeFinder.getTimestampFormat();
        else
            timestampFormat = (DateFormat) localeFinder.getTimestampFormat().clone();
        if( cal == null)
            cal = new GregorianCalendar();
        else
            timestampFormat.setCalendar( cal);
        java.util.Date date = timestampFormat.parse( str);
            
        return new int[] { computeEncodedDate( date, cal), computeEncodedTime( date, cal)};
    } // end of parseLocalTimestamp

    /**
     * Parse a timestamp or a date. DB2 allows timestamps to be used as dates or times. So
     * date('2004-04-15-16.15.32') is valid, as is date('2004-04-15').
     *
     * This method does not handle localized timestamps.
     *
     * @param parser a DateTimeParser initialized with a string.
     * @param timeRequired If true then an error will be thrown if the time is missing. If false then the time may
     *                     be omitted.
     *
     * @return {encodedDate, encodedTime, nanosecond} array.
     *
     * @exception StandardException if the syntax is incorrect for an IBM standard timestamp.
     */
    static int[] parseDateOrTimestamp( DateTimeParser parser, boolean timeRequired)
        throws StandardException
    {
        int year = parser.parseInt( 4, false, DATE_SEPARATORS, false);
        int month = parser.parseInt( 2, true, DATE_SEPARATORS, false);
        int day = parser.parseInt( 2, true, timeRequired ? DATE_TIME_SEPARATORS : DATE_TIME_SEPARATORS_OR_END, false);
        int hour = 0;
        int minute = 0;
        int second = 0;
        int nano = 0;
        if( parser.getCurrentSeparator() != 0)
        {
            char timeSeparator = (parser.getCurrentSeparator() == ODBC_DATE_TIME_SEPARATOR)
              ? ODBC_TIME_SEPARATOR : IBM_TIME_SEPARATOR;
            hour = parser.parseInt( 2, true, TIME_SEPARATORS, false);
            if( timeSeparator == parser.getCurrentSeparator())
            {
                minute = parser.parseInt( 2, false, TIME_SEPARATORS, false);
                if( timeSeparator == parser.getCurrentSeparator())
                {
                    second = parser.parseInt( 2, false, TIME_SEPARATORS_OR_END, false);
                    if( parser.getCurrentSeparator() == '.')
                        nano = parser.parseInt( MAX_FRACTION_DIGITS, true, END_OF_STRING, true)*FRACTION_TO_NANO;
                }
            }
        }
        parser.checkEnd();
        return new int[] { SQLDate.computeEncodedDate( year, month, day),
                           SQLTime.computeEncodedTime( hour,minute,second),
                           nano};
    } // end of parseDateOrTimestamp

	/**
	 * Set the value from a correctly typed Timestamp object.
	 * @throws StandardException 
	 */
	void setObject(Object theValue) throws StandardException
	{
		setValue((Timestamp) theValue);
	}
	
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		if (theValue instanceof SQLTimestamp) {
			restoreToNull();
			SQLTimestamp tvst = (SQLTimestamp) theValue;
			encodedDate = tvst.encodedDate;
			encodedTime = tvst.encodedTime;
			nanos = tvst.nanos;
        }
		else
        {
            Calendar cal = new GregorianCalendar();
			setValue(theValue.getTimestamp( cal), cal);
        }
	}

	/**
		@see DateTimeDataValue#setValue
		When converting from a date to a timestamp, time is set to 00:00:00.0

	 */
	public void setValue(Date value, Calendar cal) throws StandardException
	{
		restoreToNull();
        if( value != null)
        {
            if( cal == null)
                cal = new GregorianCalendar();
            encodedDate = computeEncodedDate(value, cal);
        }
		/* encodedTime and nanos are already set to zero by restoreToNull() */
	}

	/**
		@see DateTimeDataValue#setValue

	 */
	public void setValue(Time value, Calendar cal) throws StandardException
	{
		restoreToNull();
		if (value != null)
		{
			/*
			** Create a new timestamp with today's date,
			** and 'value' time.  
			**
			** We create a new calendar to get today's date
			*/
			Calendar today = GregorianCalendar.getInstance();
			encodedDate = SQLDate.computeEncodedDate(today);
            if( cal == null)
                cal = today;
			encodedTime = computeEncodedTime(value, cal);
		}
	}

	/**
		@see DateTimeDataValue#setValue

	 */
	public void setValue(Timestamp value, Calendar cal) 
	    throws StandardException
	{
		restoreToNull();
		setNumericTimestamp(value, cal);
	}


	public void setValue(String theValue)
	    throws StandardException
	{
		restoreToNull();

		if (theValue != null)
		{
            DatabaseContext databaseContext = (DatabaseContext) ContextService.getContext(DatabaseContext.CONTEXT_ID);
            parseTimestamp( theValue,
                            false,
                            (databaseContext == null) ? null : databaseContext.getDatabase(),
                            (Calendar) null);
		}
		/* restoreToNull will have already set the encoded date to 0 (null value) */
	}

	/*
	** SQL Operators
	*/


	/**
	 * @see DateTimeDataValue#getYear
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getYear(NumberDataValue result)
							throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getYear called on a null");
		}
		return SQLDate.setSource(SQLDate.getYear(encodedDate), result);
	}

	/**
	 * @see DateTimeDataValue#getMonth
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMonth(NumberDataValue result)
							throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getMonth called on a null");
		}
		return SQLDate.setSource(SQLDate.getMonth(encodedDate), result);
	}

	/**
	 * @see DateTimeDataValue#getDate
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getDate(NumberDataValue result)
							throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getDate called on a null");
		}
		return SQLDate.setSource(SQLDate.getDay(encodedDate), result);
	}

	/**
	 * @see DateTimeDataValue#getHours
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getHours(NumberDataValue result)
							throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getHours called on a null");
		}
		return SQLDate.setSource(SQLTime.getHour(encodedTime), result);
	}

	/**
	 * @see DateTimeDataValue#getMinutes
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMinutes(NumberDataValue result)
							throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getMinute called on a null");
		}
		return SQLDate.setSource(SQLTime.getMinute(encodedTime), result);
	}

	/**
	 * @see DateTimeDataValue#getSeconds
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getSeconds(NumberDataValue source)
							throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getSeconds called on a null");
			SanityManager.ASSERT(source == null || source instanceof SQLDouble,
		"getSeconds for a timestamp was given a source other than a SQLDouble");
		}
		NumberDataValue result;

		if (source != null)
			result = source;
		else
			result = new SQLDouble();

		result.setValue((double)(SQLTime.getSecond(encodedTime))
				+ ((double)nanos)/1.0e9);

		return result;
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
			return getTimestamp( (Calendar) null).toString();
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
		
		return  encodedDate + encodedTime + nanos; //since 0 is null

	}

	/** @see DataValueDescriptor#typePrecedence */
	public int	typePrecedence()
	{
		return TypeId.TIMESTAMP_PRECEDENCE;
	}

	/**
	 * Check if the value is null.  encodedDate value of 0 is null
	 *
	 * @return Whether or not value is logically null.
	 */
	public final boolean isNull()
	{
		return (encodedDate == 0);
	}

	/**
	 * Get the value field.  We instantiate the field
	 * on demand.
	 *
	 * @return	The value field.
	 */
	public Timestamp getTimestamp(java.util.Calendar cal)
	{
		if (isNull())
			return null;
        return newTimestamp(cal);
    }

    protected Timestamp newTimestamp(Calendar currentCal)
    {
        if( currentCal == null)
            currentCal = new GregorianCalendar();
        setCalendar( currentCal);
		Timestamp t = new Timestamp(currentCal.getTime().getTime());
		t.setNanos(nanos);
		return t;
	}

    private void setCalendar( Calendar cal)
    {
		cal.set(Calendar.YEAR, SQLDate.getYear(encodedDate));
		/* Note calendar month is zero based so we subtract 1*/
		cal.set(Calendar.MONTH, (SQLDate.getMonth(encodedDate)-1));
		cal.set(Calendar.DATE, SQLDate.getDay(encodedDate));
		cal.set(Calendar.HOUR_OF_DAY, SQLTime.getHour(encodedTime));
		cal.set(Calendar.MINUTE, SQLTime.getMinute(encodedTime));
		cal.set(Calendar.SECOND, SQLTime.getSecond(encodedTime));
		cal.set(Calendar.MILLISECOND, 0);
    } // end of setCalendar
        
	/**
	 * Set the encoded values for the timestamp
	 *
	 */
	private void setNumericTimestamp(Timestamp value, Calendar cal) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isNull(), "setNumericTimestamp called when already set");
		}
		if (value != null)
		{
            if( cal == null)
                cal = new GregorianCalendar();
			encodedDate = computeEncodedDate(value, cal);
			encodedTime = computeEncodedTime(value, cal);
			nanos = value.getNanos();
		}
		/* encoded date should already be 0 for null */
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


		return localeFinder.getTimestampFormat().format(getTimestamp((Calendar) null));
	}

	/**
		computeEncodedDate sets the date in a Calendar object
		and then uses the SQLDate function to compute an encoded date
		The encoded date is
			year << 16 + month << 8 + date
		@param value	the value to convert
		@return 		the encodedDate

	 */
	private static int computeEncodedDate(java.util.Date value, Calendar currentCal) throws StandardException
	{
		if (value == null)
			return 0;

		currentCal.setTime(value);
		return SQLDate.computeEncodedDate(currentCal);
	}
	/**
		computeEncodedTime extracts the hour, minute and seconds from
		a java.util.Date value and encodes them as
			hour << 16 + minute << 8 + second
		using the SQLTime function for encoding the data
		@param value	the value to convert
		@return 		the encodedTime

	 */
	private static int computeEncodedTime(java.util.Date value, Calendar currentCal) throws StandardException
	{
		currentCal.setTime(value);
		return SQLTime.computeEncodedTime(currentCal);
	}

    
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {

                  ps.setTimestamp(position, getTimestamp((Calendar) null));
     }

    /**
     * Compute the SQL timestamp function.
     *
     * @exception StandardException
     */
    public static DateTimeDataValue computeTimestampFunction( DataValueDescriptor operand,
                                                              DataValueFactory dvf) throws StandardException
    {
        try
        {
            if( operand.isNull())
                return new SQLTimestamp();
            if( operand instanceof SQLTimestamp)
                return (SQLTimestamp) operand.getClone();

            String str = operand.getString();
            if( str.length() == 14)
            {
                int year = parseDateTimeInteger( str, 0, 4);
                int month = parseDateTimeInteger( str, 4, 2);
                int day = parseDateTimeInteger( str, 6, 2);
                int hour = parseDateTimeInteger( str, 8, 2);
                int minute = parseDateTimeInteger( str, 10, 2);
                int second = parseDateTimeInteger( str, 12, 2);
                return new SQLTimestamp( SQLDate.computeEncodedDate( year, month, day),
                                         SQLTime.computeEncodedTime( hour,minute,second),
                                         0);
            }
            // else use the standard cast
            return dvf.getTimestampValue( str, false);
        }
        catch( StandardException se)
        {
            if( SQLState.LANG_DATE_SYNTAX_EXCEPTION.startsWith( se.getSQLState()))
                throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                      operand.getString(), "timestamp");
            throw se;
        }
    } // end of computeTimestampFunction

    static int parseDateTimeInteger( String str, int start, int ndigits) throws StandardException
    {
        int end = start + ndigits;
        int retVal = 0;
        for( int i = start; i < end; i++)
        {
            char c = str.charAt( i);
            if( !Character.isDigit( c))
                throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
            retVal = 10*retVal + Character.digit( c, 10);
        }
        return retVal;
    } // end of parseDateTimeInteger

    /**
     * Add a number of intervals to a datetime value. Implements the JDBC escape TIMESTAMPADD function.
     *
     * @param intervalType One of FRAC_SECOND_INTERVAL, SECOND_INTERVAL, MINUTE_INTERVAL, HOUR_INTERVAL,
     *                     DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, QUARTER_INTERVAL, or YEAR_INTERVAL
     * @param count The number of intervals to add
     * @param currentDate Used to convert time to timestamp
     * @param resultHolder If non-null a DateTimeDataValue that can be used to hold the result. If null then
     *                     generate a new holder
     *
     * @return startTime + intervalCount intervals, as a timestamp
     *
     * @exception StandardException
     */
    public DateTimeDataValue timestampAdd( int intervalType,
                                           NumberDataValue count,
                                           java.sql.Date currentDate,
                                           DateTimeDataValue resultHolder)
        throws StandardException
    {
        if( resultHolder == null)
            resultHolder = new SQLTimestamp();
        SQLTimestamp tsResult = (SQLTimestamp) resultHolder;
        if( isNull() || count.isNull())
        {
            tsResult.restoreToNull();
            return resultHolder;
        }
        tsResult.setFrom( this);
        int intervalCount = count.getInt();
        
        switch( intervalType)
        {
        case FRAC_SECOND_INTERVAL:
            // The interval is nanoseconds. Do the computation in long to avoid overflow.
            long nanos = this.nanos + intervalCount;
            if( nanos >= 0 && nanos < ONE_BILLION)
                tsResult.nanos = (int) nanos;
            else
            {
                int secondsInc = (int)(nanos/ONE_BILLION);
                if( nanos >= 0)
                    tsResult.nanos = (int) (nanos % ONE_BILLION);
                else
                {
                    secondsInc--;
                    nanos -= secondsInc * (long)ONE_BILLION; // 0 <= nanos < ONE_BILLION
                    tsResult.nanos = (int) nanos;
                }
                addInternal( Calendar.SECOND, secondsInc, tsResult);
            }
            break;

        case SECOND_INTERVAL:
            addInternal( Calendar.SECOND, intervalCount, tsResult);
            break;

        case MINUTE_INTERVAL:
            addInternal( Calendar.MINUTE, intervalCount, tsResult);
            break;

        case HOUR_INTERVAL:
            addInternal( Calendar.HOUR, intervalCount, tsResult);
            break;

        case DAY_INTERVAL:
            addInternal( Calendar.DATE, intervalCount, tsResult);
            break;

        case WEEK_INTERVAL:
            addInternal( Calendar.DATE, intervalCount*7, tsResult);
            break;

        case MONTH_INTERVAL:
            addInternal( Calendar.MONTH, intervalCount, tsResult);
            break;

        case QUARTER_INTERVAL:
            addInternal( Calendar.MONTH, intervalCount*3, tsResult);
            break;

        case YEAR_INTERVAL:
            addInternal( Calendar.YEAR, intervalCount, tsResult);
            break;

        default:
            throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                  ReuseFactory.getInteger( intervalType),
                                                  "TIMESTAMPADD");
        }
        return tsResult;
    } // end of timestampAdd

    private void addInternal( int calIntervalType, int count, SQLTimestamp tsResult) throws StandardException
    {
        Calendar cal = new GregorianCalendar();
        setCalendar( cal);
        try
        {
            cal.add( calIntervalType, count);
            tsResult.encodedTime = SQLTime.computeEncodedTime( cal);
            tsResult.encodedDate = SQLDate.computeEncodedDate( cal);
        }
        catch( StandardException se)
        {
            String state = se.getSQLState();
            if( state != null && state.length() > 0 && SQLState.LANG_DATE_RANGE_EXCEPTION.startsWith( state))
            {
                throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TIMESTAMP");
            }
            throw se;
        }
    } // end of addInternal

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
        if( resultHolder == null)
            resultHolder = new SQLInteger();
 
       if( isNull() || time1.isNull())
        {
            resultHolder.setToNull();
            return resultHolder;
        }
        
        SQLTimestamp ts1 = promote( time1, currentDate);

        /* Years, months, and quarters are difficult because their lengths are not constant.
         * The other intervals are relatively easy (because we ignore leap seconds).
         */
        Calendar cal = new GregorianCalendar();
        setCalendar( cal);
        long thisInSeconds = cal.getTime().getTime()/1000;
        ts1.setCalendar( cal);
        long ts1InSeconds = cal.getTime().getTime()/1000;
        long secondsDiff = thisInSeconds - ts1InSeconds;
        int nanosDiff = nanos - ts1.nanos;
        // Normalize secondsDiff and nanosDiff so that they are both <= 0 or both >= 0.
        if( nanosDiff < 0 && secondsDiff > 0)
        {
            secondsDiff--;
            nanosDiff += ONE_BILLION;
        }
        else if( nanosDiff > 0 && secondsDiff < 0)
        {
            secondsDiff++;
            nanosDiff -= ONE_BILLION;
        }
        long ldiff = 0;
        
        switch( intervalType)
        {
        case FRAC_SECOND_INTERVAL:
            if( secondsDiff > Integer.MAX_VALUE/ONE_BILLION || secondsDiff < Integer.MIN_VALUE/ONE_BILLION)
                throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
            ldiff = secondsDiff*ONE_BILLION + nanosDiff;
            break;
            
        case SECOND_INTERVAL:
            ldiff = secondsDiff;
            break;
            
        case MINUTE_INTERVAL:
            ldiff = secondsDiff/60;
            break;

        case HOUR_INTERVAL:
            ldiff = secondsDiff/(60*60);
            break;
            
        case DAY_INTERVAL:
            ldiff = secondsDiff/(24*60*60);
            break;
            
        case WEEK_INTERVAL:
            ldiff = secondsDiff/(7*24*60*60);
            break;

        case QUARTER_INTERVAL:
        case MONTH_INTERVAL:
            // Make a conservative guess and increment until we overshoot.
            if( Math.abs( secondsDiff) > 366*24*60*60) // Certainly more than a year
                ldiff = 12*(secondsDiff/(366*24*60*60));
            else
                ldiff = secondsDiff/(31*24*60*60);
            if( secondsDiff >= 0)
            {
                if (ldiff >= Integer.MAX_VALUE)
                    throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
                // cal holds the time for time1
                cal.add( Calendar.MONTH, (int) (ldiff + 1));
                for(;;)
                {
                    if( cal.getTime().getTime()/1000 > thisInSeconds)
                        break;
                    cal.add( Calendar.MONTH, 1);
                    ldiff++;
                }
            }
            else
            {
                if (ldiff <= Integer.MIN_VALUE)
                    throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
                // cal holds the time for time1
                cal.add( Calendar.MONTH, (int) (ldiff - 1));
                for(;;)
                {
                    if( cal.getTime().getTime()/1000 < thisInSeconds)
                        break;
                    cal.add( Calendar.MONTH, -1);
                    ldiff--;
                }
            }
            if( intervalType == QUARTER_INTERVAL)
                ldiff = ldiff/3;
            break;

        case YEAR_INTERVAL:
            // Make a conservative guess and increment until we overshoot.
            ldiff = secondsDiff/(366*24*60*60);
            if( secondsDiff >= 0)
            {
                if (ldiff >= Integer.MAX_VALUE)
                    throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
                // cal holds the time for time1
                cal.add( Calendar.YEAR, (int) (ldiff + 1));
                for(;;)
                {
                    if( cal.getTime().getTime()/1000 > thisInSeconds)
                        break;
                    cal.add( Calendar.YEAR, 1);
                    ldiff++;
                }
            }
            else
            {
                if (ldiff <= Integer.MIN_VALUE)
                    throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
                // cal holds the time for time1
                cal.add( Calendar.YEAR, (int) (ldiff - 1));
                for(;;)
                {
                    if( cal.getTime().getTime()/1000 < thisInSeconds)
                        break;
                    cal.add( Calendar.YEAR, -1);
                    ldiff--;
                }
            }
            break;

        default:
            throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                  ReuseFactory.getInteger( intervalType),
                                                  "TIMESTAMPDIFF");
        }
		if (ldiff > Integer.MAX_VALUE || ldiff < Integer.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
        resultHolder.setValue( (int) ldiff);
        return resultHolder;
    } // end of timestampDiff

    /**
     * Promotes a DateTimeDataValue to a timestamp.
     *
     *
     * @return the corresponding timestamp, using the current date if datetime is a time,
     *         or time 00:00:00 if datetime is a date.
     *
     * @exception StandardException
     */
    static SQLTimestamp promote( DateTimeDataValue dateTime, java.sql.Date currentDate) throws StandardException
    {
        if( dateTime instanceof SQLTimestamp)
            return (SQLTimestamp) dateTime;
        else if( dateTime instanceof SQLTime)
            return new SQLTimestamp( SQLDate.computeEncodedDate( currentDate, (Calendar) null),
                                    ((SQLTime) dateTime).getEncodedTime(),
                                    0 /* nanoseconds */);
        else if( dateTime instanceof SQLDate)
            return new SQLTimestamp( ((SQLDate) dateTime).getEncodedDate(), 0, 0);
        else
            return new SQLTimestamp( dateTime.getTimestamp( new GregorianCalendar()));
    } // end of promote
}
