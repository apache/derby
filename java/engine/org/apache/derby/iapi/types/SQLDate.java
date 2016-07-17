/*

   Derby - Class org.apache.derby.iapi.types.SQLDate

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.db.DatabaseContext;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.io.StoredFormatIds;
 
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.i18n.LocaleFinder;
import org.apache.derby.iapi.util.StringUtil;

import java.sql.Date;
import java.sql.Timestamp;
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
 * This contains an instance of a SQL Date.
 * <p>
 * The date is stored as int (year &lt;&lt; 16 + month &lt;&lt; 8 + day)
 * Null is represented by an encodedDate value of 0.
 * Some of the static methods in this class are also used by SQLTime and SQLTimestamp
 * so check those classes if you change the date encoding
 *
 * PERFORMANCE OPTIMIZATION:
 * The java.sql.Date object is only instantiated when needed
 * do to the overhead of Date.valueOf(), etc. methods.
 */

public final class SQLDate extends DataType
						implements DateTimeDataValue
{

	private int	encodedDate;	//year << 16 + month << 8 + day

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLDate.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    } // end of estimateMemoryUsage

    int getEncodedDate()
    {
        return encodedDate;
    }
    
	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

	public String getString()
	{
		//format is [yyy]y-mm-dd e.g. 1-01-01, 9999-99-99
		if (!isNull())
		{
			return encodedDateToString(encodedDate);
		}
		else
		{
			return null;
		}
	}

	/**
		getTimestamp returns a timestamp with the date value 
		time is set to 00:00:00.0
	*/
	public Timestamp getTimestamp( Calendar cal) 
	{
		if (isNull())
		{
			return null;
		}
        
        return new Timestamp(getTimeInMillis(cal));
    }

    /**
     * Convert the date into a milli-seconds since the epoch
     * with the time set to 00:00 based upon the passed in Calendar.
     */
    private long getTimeInMillis(Calendar cal)
    {
        if( cal == null)
            cal = new GregorianCalendar();
        cal.clear();
        
        SQLDate.setDateInCalendar(cal, encodedDate);
        
        return cal.getTimeInMillis();
    }
    
    /**
     * Set the date portion of a date-time value into
     * the passed in Calendar object from its encodedDate
     * value. Only the YEAR, MONTH and DAY_OF_MONTH
     * fields are modified. The remaining
     * state of the Calendar is not modified.
     */
    static void setDateInCalendar(Calendar cal, int encodedDate)
    {
        // Note Calendar uses 0 for January, Derby uses 1.
        cal.set(getYear(encodedDate),
                getMonth(encodedDate)-1, getDay(encodedDate));     
    }
    
	/**
		getObject returns the date value

	 */
	public Object getObject()
	{
		return getDate( (Calendar) null);
	}
		
	public int getLength()
	{
		return 4;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
		return "DATE";
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_DATE_ID;
	}

	/** 
		@exception IOException error writing data

	*/
	public void writeExternal(ObjectOutput out) throws IOException {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isNull(), "writeExternal() is not supposed to be called for null values.");

		out.writeInt(encodedDate);
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on error reading the object
	 */
	public void readExternal(ObjectInput in) throws IOException
	{
		encodedDate = in.readInt();
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#cloneValue */
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		// Call constructor with all of our info
		return new SQLDate(encodedDate);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLDate();
	}
	/**
	 * @see org.apache.derby.iapi.services.io.Storable#restoreToNull
	 *
	 */

	public void restoreToNull()
	{
		// clear encodedDate
		encodedDate = 0;

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
        setValue(resultSet.getDate(colNumber), (Calendar) null);
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
		/* get the comparison date values */
		int otherVal = 0;

		/* if the argument is another SQLDate
		 * get the encodedDate
		 */
		if (other instanceof SQLDate)
		{
			otherVal = ((SQLDate)other).encodedDate; 
		}
		else 
		{
			/* O.K. have to do it the hard way and calculate the numeric value
			 * from the value
			 */
			otherVal = SQLDate.computeEncodedDate(other.getDate(new GregorianCalendar()));
		}
		if (encodedDate > otherVal)
			comparison = 1;
		else if (encodedDate < otherVal)
			comparison = -1;
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
	public SQLDate() {
	}

	public SQLDate(Date value) throws StandardException
	{
		parseDate(value);
	}
    
    private void parseDate( java.util.Date value) throws StandardException
	{
		encodedDate = computeEncodedDate(value);
	}

	private SQLDate(int encodedDate) {
		this.encodedDate = encodedDate;
	}

    /**
     * Construct a date from a string. The allowed date formats are:
     *<ol>
     *<li>ISO: yyyy-mm-dd
     *<li>IBM USA standard: mm/dd/yyyy
     *<li>IBM European standard: dd.mm.yyyy
     *</ol>
     * Trailing blanks may be included; leading zeros may be omitted from the month and day portions.
     *
     * @param dateStr
     * @param isJdbcEscape if true then only the JDBC date escape syntax is allowed
     * @param localeFinder
     *
     * @exception StandardException if the syntax is invalid or the value is
     * out of range
     */
    public SQLDate( String dateStr, boolean isJdbcEscape, LocaleFinder localeFinder)
        throws StandardException
    {
        parseDate( dateStr, isJdbcEscape, localeFinder, (Calendar) null);
    }

    /**
     * Construct a date from a string. The allowed date formats are:
     *<ol>
     *<li>ISO: yyyy-mm-dd
     *<li>IBM USA standard: mm/dd/yyyy
     *<li>IBM European standard: dd.mm.yyyy
     *</ol>
     * Trailing blanks may be included; leading zeros may be omitted from the month and day portions.
     *
     * @param dateStr
     * @param isJdbcEscape if true then only the JDBC date escape syntax is allowed
     * @param localeFinder
     *
     * @exception StandardException if the syntax is invalid or the value is
     * out of range
     */
    public SQLDate( String dateStr, boolean isJdbcEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        parseDate( dateStr, isJdbcEscape, localeFinder, cal);
    }

    static final char ISO_SEPARATOR = '-';
    private static final char[] ISO_SEPARATOR_ONLY = {ISO_SEPARATOR};
    private static final char IBM_USA_SEPARATOR = '/';
    private static final char[] IBM_USA_SEPARATOR_ONLY = {IBM_USA_SEPARATOR};
    private static final char IBM_EUR_SEPARATOR = '.';
    private static final char[] IBM_EUR_SEPARATOR_ONLY = {IBM_EUR_SEPARATOR};
    private static final char[] END_OF_STRING = {(char) 0};
    
    private void parseDate( String dateStr, boolean isJdbcEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        boolean validSyntax = true;
        DateTimeParser parser = new DateTimeParser( dateStr);
        int year = 0;
        int month = 0;
        int day = 0;
        StandardException thrownSE = null;

        try
        {
            switch( parser.nextSeparator())
            {
            case ISO_SEPARATOR:
                encodedDate = SQLTimestamp.parseDateOrTimestamp( parser, false)[0];
                return;

            case IBM_USA_SEPARATOR:
                if( isJdbcEscape)
                {
                    validSyntax = false;
                    break;
                }
                month = parser.parseInt( 2, true, IBM_USA_SEPARATOR_ONLY, false);
                day = parser.parseInt( 2, true, IBM_USA_SEPARATOR_ONLY, false);
                year = parser.parseInt( 4, false, END_OF_STRING, false);
                break;

            case IBM_EUR_SEPARATOR:
                if( isJdbcEscape)
                {
                    validSyntax = false;
                    break;
                }
                day = parser.parseInt( 2, true, IBM_EUR_SEPARATOR_ONLY, false);
                month = parser.parseInt( 2, true, IBM_EUR_SEPARATOR_ONLY, false);
                year = parser.parseInt( 4, false, END_OF_STRING, false);
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
            encodedDate = computeEncodedDate( year, month, day);
        }
        else
        {
            // See if it is a localized date or timestamp.
            dateStr = StringUtil.trimTrailing( dateStr);
            DateFormat dateFormat = null;
            if( localeFinder == null)
                dateFormat = DateFormat.getDateInstance();
            else if( cal == null)
                dateFormat = localeFinder.getDateFormat();
            else
                dateFormat = (DateFormat) localeFinder.getDateFormat().clone();
            if( cal != null)
                dateFormat.setCalendar( cal);
            try
            {
                encodedDate = computeEncodedDate( dateFormat.parse( dateStr), cal);
            }
            catch( ParseException pe)
            {
                // Maybe it is a localized timestamp
                try
                {
                    encodedDate = SQLTimestamp.parseLocalTimestamp( dateStr, localeFinder, cal)[0];
                }
                catch( ParseException pe2)
                {
                    if( thrownSE != null)
                        throw thrownSE;
                    throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
                }
            }
        }
    } // end of parseDate

	/**
	 * Set the value from a correctly typed Date object.
	 * @throws StandardException 
	 */
	void setObject(Object theValue) throws StandardException
	{
		setValue((Date) theValue);
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		// Same format means same type SQLDate
		if (theValue instanceof SQLDate) {
			restoreToNull();
			encodedDate = ((SQLDate) theValue).encodedDate;
		}
        else
        {
            Calendar cal = new GregorianCalendar();
			setValue(theValue.getDate( cal), cal);
        }
	}

	/**
		@see DateTimeDataValue#setValue

	 */
	public void setValue(Date value, Calendar cal) throws StandardException
	{
		restoreToNull();
		encodedDate = computeEncodedDate((java.util.Date) value, cal);
	}

	/**
		@see DateTimeDataValue#setValue

	 */
	public void setValue(Timestamp value, Calendar cal) throws StandardException
	{
		restoreToNull();
		encodedDate = computeEncodedDate((java.util.Date) value, cal);
	}


	public void setValue(String theValue)
	    throws StandardException
	{
		restoreToNull();

		if (theValue != null)
		{
            DatabaseContext databaseContext = (DatabaseContext) DataValueFactoryImpl.getContext(DatabaseContext.CONTEXT_ID);
            parseDate( theValue,
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
        if (isNull()) {
            return nullValueInt();
        } else {    
            return SQLDate.setSource(getYear(encodedDate), result);
        }
    }

	/**
	 * @see DateTimeDataValue#getMonth
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMonth(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getMonth(encodedDate), result);
        }
	}

	/**
	 * @see DateTimeDataValue#getDate
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getDate(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getDay(encodedDate), result);
        }
	}

	/**
	 * @see DateTimeDataValue#getHours
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getHours(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getHours", "Date");
	}

	/**
	 * @see DateTimeDataValue#getMinutes
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMinutes(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getMinutes", "Date");
	}

	/**
	 * @see DateTimeDataValue#getSeconds
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getSeconds(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getSeconds", "Date");
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
			return getDate( (Calendar) null).toString();
		}
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		return encodedDate;
	}

	/** @see DataValueDescriptor#typePrecedence */
	public int	typePrecedence()
	{
		return TypeId.DATE_PRECEDENCE;
	}

	/**
	 * Check if the value is null.  
	 * encodedDate is 0 if the value is null
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
	public Date getDate( Calendar cal)
	{
        if (isNull())
            return null;
        
        return new Date(getTimeInMillis(cal));
	}

	/**
	 * Get the year from the encodedDate.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			year value.
	 */
	static int getYear(int encodedDate)
	{
		return (encodedDate >>> 16);
	}

	/**
	 * Get the month from the encodedDate,
     * January is one.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			month value.
	 */
	static int getMonth(int encodedDate)
	{
		return ((encodedDate >>> 8) & 0x00ff);
	}

	/**
	 * Get the day from the encodedDate.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			day value.
	 */
	static int getDay(int encodedDate)
	{
		return (encodedDate & 0x00ff);
	}
	/**
	 *	computeEncodedDate extracts the year, month and date from
	 *	a Calendar value and encodes them as
	 *		year &lt;&lt; 16 + month &lt;&lt; 8 + date
	 *	Use this function will help to remember to add 1 to month
	 *  which is 0 based in the Calendar class
	 *	@param cal	the Calendar 
	 *	@return 		the encodedDate
     *
     *  @exception StandardException if the value is out of the DB2 date range
	 */
	static int computeEncodedDate(Calendar cal) throws StandardException
	{
		return computeEncodedDate(cal.get(Calendar.YEAR),
                                  cal.get(Calendar.MONTH) + 1,
                                  cal.get(Calendar.DATE));
	}

    static int computeEncodedDate( int y, int m, int d) throws StandardException
    {
        int maxDay = 31;
        switch( m)
        {
        case 4:
        case 6:
        case 9:
        case 11:
            maxDay = 30;
            break;
                
        case 2:
            // leap years are every 4 years except for century years not divisble by 400.
            maxDay = ((y % 4) == 0 && ((y % 100) != 0 || (y % 400) == 0)) ? 29 : 28;
            break;
        }
        if( y < 1 || y > 9999
            || m < 1 || m > 12
            || d < 1 || d > maxDay)
            throw StandardException.newException( SQLState.LANG_DATE_RANGE_EXCEPTION);
        return (y << 16) + (m << 8) + d;
    }

    /**
     * Convert a date to the JDBC representation and append it to a string buffer.
     *
     * @param year
     * @param month 1 based (January == 1)
     * @param day
     * @param sb The string representation is appended to this StringBuffer
     */
    static void dateToString( int year, int month, int day, StringBuffer sb)
    {
        String yearStr = Integer.toString( year);
        for( int i = yearStr.length(); i < 4; i++)
            sb.append( '0');
		sb.append(yearStr);
		sb.append(ISO_SEPARATOR);

		String monthStr = Integer.toString( month);
		String dayStr = Integer.toString( day);
		if (monthStr.length() == 1)
			sb.append('0');
		sb.append(monthStr);
		sb.append(ISO_SEPARATOR);
		if (dayStr.length() == 1)
			sb.append('0');
		sb.append(dayStr);
    } // end of dateToString
    
	/**
	 * Get the String version from the encodedDate.
	 *
	 * @return	 string value.
	 */
	static String encodedDateToString(int encodedDate)
	{
		StringBuffer vstr = new StringBuffer();
        dateToString( getYear(encodedDate), getMonth(encodedDate), getDay(encodedDate), vstr);
		return vstr.toString();
	}

	/**
		This helper routine tests the nullability of various parameters
		and sets up the result appropriately.

		If source is null, a new NumberDataValue is built. 

		@exception StandardException	Thrown on error
	 */
	static NumberDataValue setSource(int value,
										NumberDataValue source)
									throws StandardException {
		/*
		** NOTE: Most extract operations return int, so the generation of
		** a SQLInteger is here.  Those extract operations that return
		** something other than int must allocate the source NumberDataValue
		** themselves, so that we do not allocate a SQLInteger here.
		*/
		if (source == null)
			source = new SQLInteger();

		source.setValue(value);

		return source;
	}
	/**
     * Compute the encoded date given a date
	 *
	 */
	private static int computeEncodedDate(java.util.Date value) throws StandardException
	{
        return computeEncodedDate( value, null);
    }

    static int computeEncodedDate(java.util.Date value, Calendar currentCal) throws StandardException
    {
		if (value == null)
			return 0;			//encoded dates have a 0 value for null
        if( currentCal == null)
            currentCal = new GregorianCalendar();
		currentCal.setTime(value);
		return SQLDate.computeEncodedDate(currentCal);
	}


        /**
         * Implement the date SQL function: construct a SQL date from a string, number, or timestamp.
         *
         * @param operand Must be a date or a string convertible to a date.
         * @param dvf the DataValueFactory
         *
         * @exception StandardException standard error policy
         */
    public static DateTimeDataValue computeDateFunction( DataValueDescriptor operand,
                                                         DataValueFactory dvf) throws StandardException
    {
        try
        {
            if( operand.isNull())
                return new SQLDate();
            if( operand instanceof SQLDate)
                return (SQLDate) operand.cloneValue(false);

            if( operand instanceof SQLTimestamp)
            {
                DateTimeDataValue retVal = new SQLDate();
                retVal.setValue( operand);
                return retVal;
            }
            if( operand instanceof NumberDataValue)
            {
                int daysSinceEpoch = operand.getInt();
                if( daysSinceEpoch <= 0 || daysSinceEpoch > 3652059)
                    throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                          operand.getString(), "date");
                Calendar cal = new GregorianCalendar( 1970, 0, 1, 12, 0, 0);
                cal.add( Calendar.DATE, daysSinceEpoch - 1);
                return new SQLDate( computeEncodedDate( cal.get( Calendar.YEAR),
                                                        cal.get( Calendar.MONTH) + 1,
                                                        cal.get( Calendar.DATE)));
            }
            String str = operand.getString();
            if( str.length() == 7)
            {
                // yyyyddd where ddd is the day of the year
                int year = SQLTimestamp.parseDateTimeInteger( str, 0, 4);
                int dayOfYear = SQLTimestamp.parseDateTimeInteger( str, 4, 3);
                if( dayOfYear < 1 || dayOfYear > 366)
                    throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                          operand.getString(), "date");
                Calendar cal = new GregorianCalendar( year, 0, 1, 2, 0, 0);
                cal.add( Calendar.DAY_OF_YEAR, dayOfYear - 1);
                int y = cal.get( Calendar.YEAR);
                if( y != year)
                    throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                          operand.getString(), "date");
                return new SQLDate( computeEncodedDate( year,
                                                        cal.get( Calendar.MONTH) + 1,
                                                        cal.get( Calendar.DATE)));
            }
            // Else use the standard cast.
            return dvf.getDateValue( str, false);
        }
        catch( StandardException se)
        {
            if( SQLState.LANG_DATE_SYNTAX_EXCEPTION.startsWith( se.getSQLState()))
                throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                      operand.getString(), "date");
            throw se;
        }
    } // end of computeDateFunction

    /** Adding this method to ensure that super class' setInto method doesn't get called
      * that leads to the violation of JDBC spec( untyped nulls ) when batching is turned on.
      */     
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {

                  ps.setDate(position, getDate((Calendar) null));
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
        return toTimestamp().timestampAdd( intervalType, intervalCount, currentDate, resultHolder);
    }

    private SQLTimestamp toTimestamp() throws StandardException
    {
        return new SQLTimestamp( getEncodedDate(), 0, 0);
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
        return toTimestamp().timestampDiff( intervalType, time1, currentDate, resultHolder);
    }
}
