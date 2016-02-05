/*

   Derby - Class org.apache.derby.impl.jdbc.Util

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

package org.apache.derby.impl.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.derby.iapi.error.ErrorStringBuilder;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.ExceptionFactory;
import org.apache.derby.iapi.reference.JDBC40Translation;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.types.TypeId;

/**
	This class understands the message protocol and looks up
	SQLExceptions based on keys, so that the Local JDBC driver's
	messages can be localized.

	REMIND: May want to investigate putting some of this in the protocol
	side, for the errors that any Derby JDBC driver might return.

	The ASSERT mechanism is a wrapper of the basic services,
	to ensure that failed asserts at this level will behave
	well in a JDBC environment.

*/
//In the past, this class was sent on the wire to the client and because it
//has the message protcol stuff and also the detailed stack trace as one
//of it's member variable, the client.jar files was really big. To get
//around this problem, now we have added EmbedSQLException which is
//just a java sql exception with the stack trace information variable
//transient so it doesn't get transported to the client side and thus
//reducing the size of client.jar The bug for this fix was 1850. The
//p4 number for it will have the details of all the files impacted and
//the actual changes made.
public abstract class Util  {

	private static int logSeverityLevel = PropertyUtil.getSystemInt(Property.LOG_SEVERITY_LEVEL,
		SanityManager.DEBUG ? 0 : ExceptionSeverity.SESSION_SEVERITY);
	/*
	** Methods of Throwable
	*/

	// class implementation

    /**
     * Log SQLException to the error log if the severity exceeds the 
     * logSeverityLevel  and then throw it.  This method can be used for 
     * logging JDBC exceptions to derby.log DERBY-1191.
     * 
     * @param se SQLException to log and throw
     * @throws SQLException
     */
    public static void logAndThrowSQLException(SQLException se) throws SQLException {
    	if (se.getErrorCode() >= logSeverityLevel){
    	logSQLException(se);
    	}
    	throw se;
    }
    
	/**
	 * Log an SQLException to the error log or to the console if there is no
	 * error log available.
	 * This method could perhaps be optimized to have a static shared
	 * ErrorStringBuilder and synchronize the method, but this works for now.
	 * 
	 * @param se SQLException to log
	 */
	public static void logSQLException(SQLException se) {
    	if (se == null)
    		return;
    	String message = se.getMessage();
    	String sqlstate = se.getSQLState();
    	if ((sqlstate != null) && (sqlstate.equals(SQLState.LOGIN_FAILED)) && 
    			(message != null) && (message.equals("Connection refused : java.lang.OutOfMemoryError")))				
    		return;

        logError( "\nERROR " +  se.getSQLState() + ": "  + se.getMessage() + "\n", se );
    }
    private static  void    logError( String errorMessage, Throwable t )
    {
    	HeaderPrintWriter errorStream = Monitor.getStream();
    	if (errorStream == null) {
    		t.printStackTrace();
    		return;
    	}
    	ErrorStringBuilder	errorStringBuilder = new ErrorStringBuilder(errorStream.getHeader());
    	errorStringBuilder.append( errorMessage );
    	errorStringBuilder.stackTrace( t );
    	errorStream.print(errorStringBuilder.get().toString());
    	errorStream.flush();
    	errorStringBuilder.reset();
    }

	// class interface


	/**
		Mimic SanityManager.ASSERT in a JDBC-friendly way,
		and providing system cleanup for JDBC failures.
		We need the connection to do cleanup...

		@exception SQLException the exception
	 */
	public static void ASSERT(EmbedConnection conn, boolean mustBeTrue, String msg) throws SQLException {
		if (SanityManager.DEBUG) {
			try {
				SanityManager.ASSERT(mustBeTrue, msg);
			} catch (Throwable t) {
                throw conn.handleException(t);
			}
		}
	}

    /**
     * Checks whether a data type is supported and raises a SQLException
     * if it isn't.
     */
    public static   void checkForSupportedDataType(int dataType) throws SQLException
    {
        if ( !isSupportedType( dataType ) )
        {
            throw generateCsSQLException( SQLState.DATA_TYPE_NOT_SUPPORTED, typeName( dataType ) );
        }
    }

    /**
     * Checks whether a data type is supported and raises a StandardException
     * if it isn't.
     */
    public static   void checkSupportedRaiseStandard(int dataType) throws StandardException
    {
        if ( !isSupportedType( dataType ) )
        {
            throw StandardException.newException( SQLState.DATA_TYPE_NOT_SUPPORTED, typeName( dataType ) );
        }
    }

    /**
     * Returns false if a data type is not supported for:
     * <code>setObject(int, Object, int)</code> and
     * <code>setObject(int, Object, int, int)</code>.
     *
     * @param dataType the data type to check
     */
    private static   boolean isSupportedType(int dataType)
    {
        // JDBC 4.0 javadoc for setObject() says:
        //
        // Throws: (...) SQLFeatureNotSupportedException - if
        // targetSqlType is a ARRAY, BLOB, CLOB, DATALINK,
        // JAVA_OBJECT, NCHAR, NCLOB, NVARCHAR, LONGNVARCHAR, REF,
        // ROWID, SQLXML or STRUCT data type and the JDBC driver does
        // not support this data type
        //
        // Of these types, we only support BLOB, CLOB and
        // (sort of) JAVA_OBJECT.

        switch (dataType) {
        case Types.ARRAY:
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.NCHAR:
        case Types.NCLOB:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
        case Types.NULL:
        case Types.OTHER:
        case Types.REF:
        case JDBC40Translation.REF_CURSOR:
        case Types.ROWID:
        case Types.SQLXML:
        case Types.STRUCT:
            return false;
        }

        return true;
    }

	/*
	** There is at least one static method for each message id.
	** Its parameters are specific to its message.
	** These will throw SQLException when the message repository
	** cannot be located.
    ** Note that these methods use the SQL exception factory,
	** they don't directly do a new Util.
	*/

    public static SQLException generateCsSQLException(
            String error, Object... args) {
        return generateCsSQLException(error, null, args);
	}

    static SQLException generateCsSQLException(
                    String error, Throwable t, Object... args) {
        return ExceptionFactory.getInstance().getSQLException(
                error, (SQLException) null, t, args);
	}

	public static SQLException generateCsSQLException(StandardException se) {
        return ExceptionFactory.getInstance().getSQLException(
                se.getMessage(), se.getMessageId(), (SQLException) null,
                se.getSeverity(), se, se.getArguments());
    }

	public static SQLException noCurrentConnection() {
        return generateCsSQLException(SQLState.NO_CURRENT_CONNECTION);
	}

    /**
     * Generate an <code>SQLException</code> which points to another
     * <code>SQLException</code> nested within it with
     * <code>setNextException()</code>.
     *
     * @param messageId message id
     * @param next the next SQLException, possibly null
     * @param cause the underlying exception, possibly null
     * @param args the arguments to the message creation
     * @return an SQLException wrapping another SQLException
     */
    static SQLException seeNextException(String messageId, SQLException next,
                                         Throwable cause, Object... args) {
        return ExceptionFactory.getInstance().getSQLException(
                messageId, next, cause, args);
    }

	public static SQLException javaException(Throwable t) {
		String name, msg;

		msg = t.getMessage();
		if (msg == null) msg = "";
		name = t.getClass().getName();
        SQLException next = null;
        Throwable cause = t.getCause();
        if (cause != null) {
            if (cause instanceof SQLException) {
                next = (SQLException) cause;
            } else if (cause instanceof StandardException) {
                next = generateCsSQLException((StandardException) cause);
            } else {
                next = javaException(cause);
            }
        }

        SQLException result = seeNextException(
                SQLState.JAVA_EXCEPTION, next, t, name, msg);

    	if ( result.getErrorCode() >= logSeverityLevel ) { logSQLException( result ); }
        
        return result;
	}


	public static SQLException policyNotReloaded( Throwable t ) {
        return generateCsSQLException(
                SQLState.POLICY_NOT_RELOADED, t, t.getMessage());
	}

	public static SQLException notImplemented() {

		return notImplemented( MessageService.getTextMessage(MessageId.CONN_NO_DETAILS) );
	}

	public static SQLException notImplemented(String feature) {
        return generateCsSQLException(SQLState.NOT_IMPLEMENTED, feature);
	}

	static SQLException setStreamFailure(IOException e) {
		String msg;

		msg = e.getMessage();
		if (msg == null) 
			msg = e.getClass().getName();
        return generateCsSQLException(SQLState.SET_STREAM_FAILURE, e, msg);
	}

	static SQLException typeMisMatch(int targetSQLType) {
        return generateCsSQLException(
                SQLState.TYPE_MISMATCH, typeName(targetSQLType));
	}

    /** Squash an array of longs into an array of ints */
    public static  int[]   squashLongs( long[] longs )
    {
        int count = (longs == null) ? 0 : longs.length;
        int[]   ints = new int[ count ];
        for ( int i = 0; i < count; i++ ) { ints[ i ] = (int) longs[ i ]; }

        return ints;
    }

    /**
     * Create an {@code IOException} that wraps another {@code Throwable}.
     *
     * @param cause the underlying cause of the error
     * @return an {@code IOException} linked to {@code cause}
     */
    static IOException newIOException(Throwable cause) {
        return new IOException(cause);
    }

	public static String typeName(int jdbcType) {
		switch (jdbcType) {
			case Types.ARRAY: return TypeId.ARRAY_NAME;
			case Types.BIT 		:  return TypeId.BIT_NAME;
			case Types.BOOLEAN  : return TypeId.BOOLEAN_NAME;
			case Types.DATALINK: return TypeId.DATALINK_NAME;
			case Types.TINYINT 	:  return TypeId.TINYINT_NAME;
			case Types.SMALLINT	:  return TypeId.SMALLINT_NAME;
			case Types.INTEGER 	:  return TypeId.INTEGER_NAME;
            case Types.BIGINT   :  return TypeId.BIGINT_NAME;

			case Types.FLOAT 	:  return TypeId.FLOAT_NAME;
			case Types.REAL 	:  return TypeId.REAL_NAME;
			case Types.DOUBLE 	:  return TypeId.DOUBLE_NAME;

			case Types.NUMERIC 	:  return TypeId.NUMERIC_NAME;
			case Types.DECIMAL	:  return TypeId.DECIMAL_NAME;

			case Types.CHAR		:  return TypeId.CHAR_NAME;
			case Types.VARCHAR 	:  return TypeId.VARCHAR_NAME;
			case Types.LONGVARCHAR 	:  return "LONGVARCHAR";
            case Types.CLOB     :  return TypeId.CLOB_NAME;

			case Types.DATE 		:  return TypeId.DATE_NAME;
			case Types.TIME 		:  return TypeId.TIME_NAME;
			case Types.TIMESTAMP 	:  return TypeId.TIMESTAMP_NAME;

			case Types.BINARY			:  return TypeId.BINARY_NAME;
			case Types.VARBINARY	 	:  return TypeId.VARBINARY_NAME;
			case Types.LONGVARBINARY 	:  return TypeId.LONGVARBINARY_NAME;
            case Types.BLOB             :  return TypeId.BLOB_NAME;

			case Types.OTHER		:  return "OTHER";
			case Types.JAVA_OBJECT	:  return "Types.JAVA_OBJECT";
			case Types.REF : return TypeId.REF_NAME;
			case JDBC40Translation.REF_CURSOR: return TypeId.REF_CURSOR;
            case Types.ROWID: return TypeId.ROWID_NAME;
			case Types.STRUCT: return TypeId.STRUCT_NAME;
			case StoredFormatIds.XML_TYPE_ID :  return TypeId.XML_NAME;
            case Types.SQLXML: return TypeId.SQLXML_NAME;
			default : return String.valueOf(jdbcType);
		}
	}
}
