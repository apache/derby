/*

   Derby - Class org.apache.derby.iapi.types.SQLDate

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.types.SQLInteger;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.DateTimeDataValue;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.io.StoredFormatIds;
 
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.DataType;

import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.i18n.LocaleFinder;
import org.apache.derby.iapi.util.StringUtil;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

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
 * The date is stored as int (year << 16 + month << 8 + day)
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

	// The cached value.toString()
	private String	valueString;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLDate.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE + ClassSize.estimateMemoryUsage( valueString);
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
			if (valueString == null)
			{
				valueString = encodedDateToString(encodedDate);
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
		getTimestamp returns a timestamp with the date value 
		time is set to 00:00:00.0
	*/
	public Timestamp getTimestamp( Calendar cal) 
	{
		if (isNull())
		{
			return null;
		}
		else 
			// date is converted to a timestamp filling the time in with 00:00:00
            return newTimestamp(cal);
    }

    private long getTimeInMillis( Calendar cal)
    {
        if( cal == null)
            cal = new GregorianCalendar();
        cal.clear();
        cal.set( getYear( encodedDate), getMonth( encodedDate)-1, getDay( encodedDate));
        return cal.getTime().getTime();
    }
    
    private Timestamp newTimestamp(java.util.Calendar cal)
    {
        return new Timestamp(getTimeInMillis( cal));
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

		// reset cached string values
		valueString = null;
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException
	{
		encodedDate = in.readInt();

		// reset cached string values
		valueString = null;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#getClone */
	public DataValueDescriptor getClone()
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
     * @return the internal DataValueDescriptor for the value
     *
     * @exception Standard exception if the syntax is invalid or the value is out of range.
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
     * @return the internal DataValueDescriptor for the value
     *
     * @exception Standard exception if the syntax is invalid or the value is out of range.
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
                valueString = parser.getTrimmedString();
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
            valueString = parser.checkEnd();
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
            valueString = dateStr;
        }
    } // end of parseDate

	public void setValue(Object theValue) throws StandardException
	{
		if (theValue == null)
		{
			setToNull();
		}
		else if (theValue instanceof Date)
		{
			setValue((Date)theValue, (Calendar) null);
		}
		else if (theValue instanceof Timestamp)
		{
			setValue((Timestamp)theValue, (Calendar) null);
		}
		else
		{
			genericSetObject(theValue);
		}

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
            DatabaseContext databaseContext = (DatabaseContext) ContextService.getContext(DatabaseContext.CONTEXT_ID);
            parseDate( theValue,
                       false,
                       (databaseContext == null) ? null : databaseContext.getDatabase(),
                       (Calendar) null);
        }
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
		return SQLDate.setSource(getYear(encodedDate), result);
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
		return SQLDate.setSource(getMonth(encodedDate), result);
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
		return SQLDate.setSource(getDay(encodedDate), result);
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
			SanityManager.ASSERT(!isNull(), "getHours called on null.");
		}
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
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getMinutes called on null.");
		}
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
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isNull(), "getSeconds called on null.");
		}
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
		if (encodedDate != 0)
            return new Date( getTimeInMillis( cal));

		return null;
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
	 * Get the month from the encodedDate.
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
	 *		year << 16 + month << 8 + date
	 *	Use this function will help to remember to add 1 to month
	 *  which is 0 based in the Calendar class
	 *	@param value	the Calendar 
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

		return localeFinder.getDateFormat().format(getDate(new GregorianCalendar()));
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
}

