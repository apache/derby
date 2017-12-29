/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedCallableStatement

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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.reference.SQLState;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;
import org.apache.derby.iapi.jdbc.EngineCallableStatement;
import org.apache.derby.iapi.types.StringDataValue;

/**
 * Local implementation.
 *
 */
public class EmbedCallableStatement extends EmbedPreparedStatement
    implements EngineCallableStatement
{
	/*
	** True if we are of the form ? = CALL() -- i.e. true
	** if we have a return output parameter.
	*/
	private boolean hasReturnOutputParameter;

	protected boolean	wasNull;

	/**
	 * @exception SQLException thrown on failure
	 */
	public EmbedCallableStatement (EmbedConnection conn, String sql,
								   int resultSetType,
								   int resultSetConcurrency,
								   int resultSetHoldability)
		throws SQLException
	{
	    super(conn, sql, false, 
			  resultSetType,
			  resultSetConcurrency,
			  resultSetHoldability,
			  Statement.NO_GENERATED_KEYS,
			  null,
			  null);

		// mark our parameters as for a callable statement 
		ParameterValueSet pvs = getParms();

		// do we have a return parameter?
		hasReturnOutputParameter = pvs.hasReturnOutputParameter();
	}

	protected void checkRequiresCallableStatement(Activation activation) {
	}

	protected final boolean executeStatement(Activation a,
                     boolean executeQuery, boolean executeUpdate)
		throws SQLException
	{
		// need this additional check (it's also in the super.executeStatement
		// to ensure we have an activation for the getParams
		checkExecStatus();
		synchronized (getConnectionSynchronization())
		{
			wasNull = false;
			//Don't fetch the getParms into a local varibale
			//at this point because it is possible that the activation
			//associated with this callable statement may have become
			//stale. If the current activation is invalid, a new activation 
			//will be created for it in executeStatement call below. 
			//We should be using the ParameterValueSet associated with
			//the activation associated to the CallableStatement after
			//the executeStatement below. That ParameterValueSet is the
			//right object to hold the return value from the CallableStatement.
			try
			{
				getParms().validate();
			} catch (StandardException e)
			{
				throw EmbedResultSet.noStateChangeException(e);
			}

			/* KLUDGE - ? = CALL ... returns a ResultSet().  We
			 * need executeUpdate to be false in that case.
			 */
			boolean execResult = super.executeStatement(a, executeQuery,
				(executeUpdate && (! hasReturnOutputParameter)));

			//Fetch the getParms into a local variable now because the
			//activation associated with a CallableStatement at this 
			//point(after the executStatement) is the current activation. 
			//We can now safely stuff the return value of the 
			//CallableStatement into the following ParameterValueSet object.
			ParameterValueSet pvs = getParms();

			/*
			** If we have a return parameter, then we
			** consume it from the returned ResultSet
			** reset the ResultSet set to null.
			*/
			if (hasReturnOutputParameter)
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(results!=null, "null results even though we are supposed to have a return parameter");
				}
				boolean gotRow = results.next();
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(gotRow, "the return resultSet didn't have any rows");
				}

				try
				{
					DataValueDescriptor returnValue = pvs.getReturnValueForSet();
					returnValue.setValueFromResultSet(results, 1, true);
				} catch (StandardException e)
				{
					throw EmbedResultSet.noStateChangeException(e);
				}
				finally {
					results.close();
					results = null;
				}

				// This is a form of ? = CALL which current is not a procedure call.
				// Thus there cannot be any user result sets, so return false. execResult
				// is set to true since a result set was returned, for the return parameter.
				execResult = false;
			}
			return execResult;
		}
	}

	/*
	* CallableStatement interface
	* (the PreparedStatement part implemented by EmbedPreparedStatement)
	*/

	/**
	 * @see CallableStatement#registerOutParameter
	 * @exception SQLException NoOutputParameters thrown.
	 */
	public final void registerOutParameter(int parameterIndex, int sqlType)
		throws SQLException 
	{
		checkStatus();

		try {
			getParms().registerOutParameter(parameterIndex-1, sqlType, -1);
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#registerOutParameter
     * @exception SQLException NoOutputParameters thrown.
     */
    public final void registerOutParameter(int parameterIndex, int sqlType, int scale)
	    throws SQLException 
	{
		checkStatus();

		if (scale < 0)
			throw newSQLException(SQLState.BAD_SCALE_VALUE, scale);
		try {
			getParms().registerOutParameter(parameterIndex-1, sqlType, scale);
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}


	/**
	 * JDBC 2.0
	 *
	 * Derby ignores the typeName argument because UDTs don't need it.
	 *
	 * @exception SQLException if a database-access error occurs.
	 */
 	public void registerOutParameter(int parameterIndex, int sqlType, 
 									 String typeName) 
 		 throws SQLException
 	{
 		registerOutParameter( parameterIndex, sqlType );
 	}
 		 
 

    /**
	 * @see CallableStatement#wasNull
     * @exception SQLException NoOutputParameters thrown.
     */
    public boolean wasNull() throws SQLException 
	{
		checkStatus();
		return wasNull;
	}

    /**
	 * @see CallableStatement#getString
     * @exception SQLException NoOutputParameters thrown.
     */
    public String getString(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			String v =  getParms().getParameterForGet(parameterIndex-1).getString();
			wasNull = (v == null);
			return v;

		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getBoolean
     * @exception SQLException NoOutputParameters thrown.
     */
    public boolean getBoolean(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			boolean v = param.getBoolean();
			wasNull = (!v) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getByte
     * @exception SQLException NoOutputParameters thrown.
     */
    public byte getByte(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			byte b = param.getByte();
			wasNull = (b == 0) && param.isNull();
			return b;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getShort
     * @exception SQLException NoOutputParameters thrown.
     */
    public short getShort(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			short s = param.getShort();
			wasNull = (s == 0) && param.isNull();
			return s;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getInt
     * @exception SQLException NoOutputParameters thrown.
     */
    public int getInt(int parameterIndex) throws SQLException 
	{
		checkStatus();

		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			int v = param.getInt();
			wasNull = (v == 0) && param.isNull();
			return v;

		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getLong
     * @exception SQLException NoOutputParameters thrown.
     */
    public long getLong(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			long v = param.getLong();
			wasNull = (v == 0L) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
     * JDBC 2.0
     *
     * Get the value of a NUMERIC parameter as a java.math.BigDecimal object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value (full precision); if the value is SQL NULL, 
     * the result is null 
     * @exception SQLException if a database-access error occurs.
     */
    public final BigDecimal getBigDecimal(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor dvd = getParms().getParameterForGet(parameterIndex-1);
			if (wasNull = dvd.isNull())
				return null;
			
			return org.apache.derby.iapi.types.SQLDecimal.getBigDecimal(dvd);
			
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getBigDecimal
     * @exception SQLException NoOutputParameters thrown.
     * @deprecated
     */
    public final BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
	{
    	BigDecimal v = getBigDecimal(parameterIndex);
    	if (v != null)
    		v = v.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
    	return v;
	}

    /**
	 * @see CallableStatement#getFloat
     * @exception SQLException NoOutputParameters thrown.
     */
    public float getFloat(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			float v = param.getFloat();
			wasNull = (v == 0.0) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getDouble
     * @exception SQLException NoOutputParameters thrown.
     */
    public double getDouble(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			double v = param.getDouble();
			wasNull = (v == 0.0) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getBytes
     * @exception SQLException NoOutputParameters thrown.
     */
    public byte[] getBytes(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			byte[] v =  getParms().getParameterForGet(parameterIndex-1).getBytes();
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getDate
     * @exception SQLException NoOutputParameters thrown.
     */
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException
	{
		checkStatus();
		try {
            Date v = getParms().
                    getParameterForGet(parameterIndex-1).getDate(cal);
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getTime
     * @exception SQLException NoOutputParameters thrown.
     */
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException
	{
		checkStatus();
		try {
            Time v = getParms().
                    getParameterForGet(parameterIndex-1).getTime(cal);
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getTimestamp
     * @exception SQLException NoOutputParameters thrown.
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal)
	    throws SQLException 
	{
		checkStatus();
		try {
            Timestamp v = getParms().
                    getParameterForGet(parameterIndex-1).getTimestamp(cal);
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}
    /**
     * Get the value of a SQL DATE parameter as a java.sql.Date object
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value; if the value is SQL NULL, the result is 
     * null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Date getDate(int parameterIndex)
      throws SQLException 
	{
        return getDate(parameterIndex, getCal());
	}

    /**
     * Get the value of a SQL TIME parameter as a java.sql.Time object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value; if the value is SQL NULL, the result is 
	 * null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Time getTime(int parameterIndex)
      throws SQLException 
	{
        return getTime(parameterIndex, getCal());
	}

    /**
     * Get the value of a SQL TIMESTAMP parameter as a java.sql.Timestamp 
     * object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value; if the value is SQL NULL, the result is 
     * null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Timestamp getTimestamp(int parameterIndex)
      throws SQLException 
	{
        return getTimestamp(parameterIndex, getCal());
	}

    /**
	 * @see CallableStatement#getObject
     * @exception SQLException NoOutputParameters thrown.
     */
	public final Object getObject(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			Object v = getParms().getParameterForGet(parameterIndex-1).getObject();
			wasNull = (v == null);
			return v;

		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}
	/**
	    * JDBC 3.0
	    *
	    * Retrieve the value of the designated JDBC DATALINK parameter as a java.net.URL object
	    *
	    * @param parameterIndex - the first parameter is 1, the second is 2
	    * @return a java.net.URL object that represents the JDBC DATALINK value used as
	    * the designated parameter
	    * @exception SQLException Feature not implemented for now.
		*/
		public URL getURL(int parameterIndex)
	    throws SQLException
		{
			throw Util.notImplemented();
		}

		/**
	    * JDBC 3.0
	    *
	    * Sets the designated parameter to the given java.net.URL object. The driver
	    * converts this to an SQL DATALINK value when it sends it to the database.
	    *
	    * @param parameterName - the name of the parameter
	    * @param val - the parameter value
	    * @exception SQLException Feature not implemented for now.
		*/
		public void setURL(String parameterName, URL val)
	    throws SQLException
		{
			throw Util.notImplemented();
		}

		/**
	    * JDBC 3.0
	    *
	    * Retrieves the value of a JDBC DATALINK parameter as a java.net.URL object
	    *
	    * @param parameterName - the name of the parameter
	    * @return the parameter value. If the value is SQL NULL, the result is null.
	    * @exception SQLException Feature not implemented for now.
		*/
		public URL getURL(String parameterName)
	    throws SQLException
		{
			throw Util.notImplemented();
		}

    /**
     * JDBC 2.0
     *
     * Get a BLOB OUT parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return an object representing a BLOB
     * @exception SQLException if a database-access error occurs.
     */
    public Blob getBlob (int parameterIndex) throws SQLException {
        Object o = getObject(parameterIndex);
        if (o == null || o instanceof Blob) {
            return (Blob) o;
        }
        throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                Blob.class.getName(),
                Util.typeName(getParameterJDBCType(parameterIndex)));
    }

    /**
     * JDBC 2.0
     *
     * Get a CLOB OUT parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return an object representing a CLOB
     * @exception SQLException if a database-access error occurs.
     */
    public Clob getClob (int parameterIndex) throws SQLException {
        Object o = getObject(parameterIndex);
        if (o == null || o instanceof Clob) {
            return (Clob) o;
        }
        throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                Clob.class.getName(),
                Util.typeName(getParameterJDBCType(parameterIndex)));
    }
    
	public void addBatch() throws SQLException {

		checkStatus();
		ParameterValueSet pvs = getParms();

		int numberOfParameters = pvs.getParameterCount();

		for (int j=1; j<=numberOfParameters; j++) {

			switch (pvs.getParameterMode(j)) {
            case (ParameterMetaData.parameterModeIn):
            case (ParameterMetaData.parameterModeUnknown):
				break;
            case (ParameterMetaData.parameterModeOut):
            case (ParameterMetaData.parameterModeInOut):
				throw newSQLException(SQLState.OUTPUT_PARAMS_NOT_ALLOWED);
			}
		}

		super.addBatch();
	}

    /**
     * JDBC 2.0
     *
     * Returns an object representing the value of OUT parameter {@code i}.
     * Use the map to determine the class from which to construct data of SQL
     * structured and distinct types.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param map the mapping from SQL type names to Java classes
     * @return a java.lang.Object holding the OUT parameter value.
     * @exception SQLException if a database-access error occurs.
     */
    public final Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        checkStatus();
        if (map == null) {
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER, map, "map",
                    "java.sql.CallableStatement.getObject");
        }
        if (!map.isEmpty()) {
            throw Util.notImplemented();
        }
        // Map is empty call the normal getObject method.
        return getObject(i);
    }

    /**
     * JDBC 2.0
     *
     * Get a REF(&lt;structured-type&gt;) OUT parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @return an object representing data of an SQL REF Type
     * @exception SQLException if a database-access error occurs.
     */
    public final Ref getRef(int i) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 2.0
     *
     * Get an Array OUT parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @return an object representing an SQL array
     * @exception SQLException if a database-access error occurs.
     */
    public final Array getArray(int i) throws SQLException {
        throw Util.notImplemented();
    }

    // JDBC 3.0 methods

    /**
     * JDBC 3.0
     *
     * Registers the OUT parameter named parameterName to the JDBC type sqlType.
     * All OUT parameters must be registered before a stored procedure is
     * executed.
     *
     * @param parameterName - the name of the parameter
     * @param sqlType - the JDBC type code defined by java.sql.Types. If the
     * parameter is of JDBC type NUMERIC or DECIMAL, the version of
     * registerOutParameter that accepts a scale value should be used.
     * @exception SQLException Feature not implemented for now.
     */
    public final void registerOutParameter(String parameterName, int sqlType)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Registers the designated output parameter. This version of the method
     * registerOutParameter should be used for a user-named or REF output
     * parameter.
     *
     * @param parameterName - the name of the parameter
     * @param sqlType - the SQL type code defined by java.sql.Types.
     * @param typeName - the fully-qualified name of an SQL structure type
     * @exception SQLException Feature not implemented for now.
     */
    public final void registerOutParameter(String parameterName,
            int sqlType, String typeName)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Registers the parameter named parameterName to the JDBC type sqlType.
     * This method must be called before a stored procedure is executed.
     *
     * @param parameterName - the name of the parameter
     * @param sqlType - the SQL type code defined by java.sql.Types.
     * @param scale - the desired number of digits to the right of the decimal
     * point. It must be greater than or equal to zero.
     * @exception SQLException Feature not implemented for now.
     */
    public final void registerOutParameter(String parameterName,
            int sqlType, int scale) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC REF (structured-type) parameter as a Ref
     * object in the Java programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value as a Ref object in the Java Programming
     * language. If the value is SQL NULL, the result is null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Ref getRef(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC BLOB parameter as a Blob object in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value as a Blob object in the Java Programming
     * language. If the value is SQL NULL, the result is null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Blob getBlob(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC CLOB parameter as a Clob object in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value as a Clob object in the Java Programming
     * language. If the value is SQL NULL, the result is null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Clob getClob(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC ARRAY parameter as an Array object in the
     * Java programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value as a Array object in the Java Programming
     * language. If the value is SQL NULL, the result is null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Array getArray(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to SQL NULL.
     *
     * @param parameterName - the name of the parameter
     * @param sqlType - the SQL type code defined in java.sql.Types
     * @exception SQLException Feature not implemented for now.
     */
    public final void setNull(String parameterName, int sqlType)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to SQL NULL.
     *
     * @param parameterName - the name of the parameter
     * @param sqlType - the SQL type code defined in java.sql.Types
     * @param typeName - the fully-qualified name of an SQL user-defined type
     * @exception SQLException Feature not implemented for now.
     */
    public final void setNull(String parameterName, int sqlType, String typeName)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java boolean value. The driver
     * converts this to an SQL BIT value when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setBoolean(String parameterName, boolean x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC BIT parameter as a boolean in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is
     * false.
     * @exception SQLException Feature not implemented for now.
     */
    public final boolean getBoolean(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java byte value. The driver
     * converts this to an SQL TINYINT value when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setByte(String parameterName, byte x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC TINYINT parameter as a byte in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is 0.
     * @exception SQLException Feature not implemented for now.
     */
    public final byte getByte(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java short value. The driver
     * converts this to an SQL SMALLINT value when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setShort(String parameterName, short x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC SMALLINT parameter as a short in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is 0.
     * @exception SQLException Feature not implemented for now.
     */
    public final short getShort(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java int value. The driver
     * converts this to an SQL INTEGER value when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setInt(String parameterName, int x) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC INTEGER parameter as a int in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is 0.
     * @exception SQLException Feature not implemented for now.
     */
    public final int getInt(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java long value. The driver
     * converts this to an SQL BIGINT value when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setLong(String parameterName, long x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC BIGINT parameter as a long in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is 0.
     * @exception SQLException Feature not implemented for now.
     */
    public final long getLong(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java float value. The driver
     * converts this to an SQL FLOAT value when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setFloat(String parameterName, float x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC FLOAT parameter as a float in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is 0.
     * @exception SQLException Feature not implemented for now.
     */
    public final float getFloat(String parameterName)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java double value. The driver
     * converts this to an SQL DOUBLE value when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setDouble(String parameterName, double x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC DOUBLE parameter as a double in the Java
     * programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is 0.
     * @exception SQLException Feature not implemented for now.
     */
    public final double getDouble(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given java.math.BigDecimal value.
     * The driver converts this to an SQL NUMERIC value when it sends it to the
     * database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setBigDecimal(String parameterName, BigDecimal x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC NUMERIC parameter as a java.math.BigDecimal
     * object with as many digits to the right of the decimal point as the value
     * contains
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is 0.
     * @exception SQLException Feature not implemented for now.
     */
    public final BigDecimal getBigDecimal(String parameterName)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java String value. The driver
     * converts this to an SQL VARCHAR OR LONGVARCHAR value (depending on the
     * argument's size relative the driver's limits on VARCHAR values) when it
     * sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setString(String parameterName, String x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC CHAR, VARCHAR, or LONGVARCHAR parameter as
     * a String in the Java programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final String getString(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Java array of bytes. The
     * driver converts this to an SQL VARBINARY OR LONGVARBINARY (depending on
     * the argument's size relative to the driver's limits on VARBINARY
     * values)when it sends it to the database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setBytes(String parameterName, byte[] x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC BINARY or VARBINARY parameter as an array
     * of byte values in the Java programming language.
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final byte[] getBytes(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given java.sql.Date value. The
     * driver converts this to an SQL DATE value when it sends it to the
     * database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setDate(String parameterName, Date x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given java.sql.Date value, using the
     * given Calendar object.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @param cal - the Calendar object the driver will use to construct the
     * date
     * @exception SQLException Feature not implemented for now.
     */
    public final void setDate(String parameterName, Date x, Calendar cal)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC DATE parameter as a java.sql.Date object
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Date getDate(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC DATE parameter as a java.sql.Date object,
     * using the given Calendar object to construct the date object.
     *
     * @param parameterName - the name of the parameter
     * @param cal - the Calendar object the driver will use to construct the
     * date
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Date getDate(String parameterName, Calendar cal)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given java.sql.Time value. The
     * driver converts this to an SQL TIME value when it sends it to the
     * database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setTime(String parameterName, Time x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC TIME parameter as ajava.sql.Time object
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Time getTime(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC TIME parameter as a java.sql.Time object,
     * using the given Calendar object to construct the time object.
     *
     * @param parameterName - the name of the parameter
     * @param cal - the Calendar object the driver will use to construct the
     * time
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Time getTime(String parameterName, Calendar cal)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given java.sql.Time value using the
     * Calendar object
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @param cal - the Calendar object the driver will use to construct the
     * time
     * @exception SQLException Feature not implemented for now.
     */
    public final void setTime(String parameterName, Time x, Calendar cal)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given java.sql.Timestamp value. The
     * driver converts this to an SQL TIMESTAMP value when it sends it to the
     * database.
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setTimestamp(String parameterName, Timestamp x)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given java.sql.Timestamp value,
     * using the given Calendar object
     *
     * @param parameterName - the name of the parameter
     * @param x - the parameter value
     * @param cal - the Calendar object the driver will use to construct the
     * timestamp.
     * @exception SQLException Feature not implemented for now.
     */
    public final void setTimestamp(String parameterName, Timestamp x, Calendar cal)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC TIMESTAMP parameter as a java.sql.Timestamp
     * object
     *
     * @param parameterName - the name of the parameter
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Timestamp getTimestamp(String parameterName)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a JDBC TIMESTAMP parameter as a java.sql.Timestamp
     * object, using the given Calendar object to construct the Timestamp
     * object.
     *
     * @param parameterName - the name of the parameter
     * @param cal - the Calendar object the driver will use to construct the
     * Timestamp
     * @return the parameter value. If the value is SQL NULL, the result is
     * null.
     * @exception SQLException Feature not implemented for now.
     */
    public final Timestamp getTimestamp(String parameterName, Calendar cal)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName - the name of the parameter
     * @param x - the Java input stream that contains the ASCII parameter value
     * @param length - the number of bytes in the stream
     * @exception SQLException Feature not implemented for now.
     */
    public final void setAsciiStream(String parameterName, InputStream x, int length)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName - the name of the parameter
     * @param x - the Java input stream that contains the binary parameter value
     * @param length - the number of bytes in the stream
     * @exception SQLException Feature not implemented for now.
     */
    public final void setBinaryStream(String parameterName, InputStream x, int length)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the designated parameter to the given Reader object, which is the
     * given number of characters long.
     *
     * @param parameterName - the name of the parameter
     * @param reader - the java.io.Reader object that contains the UNICODE data
     * @param length - the number of characters in the stream
     * @exception SQLException Feature not implemented for now.
     */
    public final void setCharacterStream(String parameterName, Reader reader, int length)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the value of the designated parameter with the given object. The
     * second argument must be an object type; for integral values, the
     * java.lang equivalent objects should be used.
     *
     * @param parameterName - the name of the parameter
     * @param x - the object containing the input parameter value
     * @param targetSqlType - the SQL type (as defined in java.sql.Types) to be
     * sent to the database. The scale argument may further qualify this type.
     * @param scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC
     * types, this is the number of digits after the decimal point. For all
     * other types, this value will be ignored.
     * @exception SQLException Feature not implemented for now.
     */
    public final void setObject(String parameterName, Object x, int targetSqlType, int scale)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Retrieves the value of a parameter as an Object in the java programming
     * language.
     *
     * @param parameterName - the name of the parameter
     * @return a java.lang.Object holding the OUT parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final Object getObject(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Returns an object representing the value of OUT parameter i and uses map
     * for the custom mapping of the parameter value.
     *
     * @param parameterName - the name of the parameter
     * @param map - the mapping from SQL type names to Java classes
     * @return a java.lang.Object holding the OUT parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final Object getObject(String parameterName, Map<String, Class<?>> map)
            throws SQLException {
        checkStatus();
        if (map == null) {
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER, map, "map",
                    "java.sql.CallableStatement.getObject");
        }
        if (!(map.isEmpty())) {
            throw Util.notImplemented();
        }

        // Map is empty so call the normal getObject method.
        return getObject(parameterName);
    }

    /**
     * JDBC 3.0
     *
     * Sets the value of the designated parameter with the given object. This
     * method is like the method setObject above, except that it assumes a scale
     * of zero.
     *
     * @param parameterName - the name of the parameter
     * @param x - the object containing the input parameter value
     * @param targetSqlType - the SQL type (as defined in java.sql.Types) to be
     * sent to the database.
     * @exception SQLException Feature not implemented for now.
     */
    public final void setObject(String parameterName, Object x, int targetSqlType)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * JDBC 3.0
     *
     * Sets the value of the designated parameter with the given object. The
     * second parameter must be of type Object; therefore, the java.lang
     * equivalent objects should be used for built-in types.
     *
     * @param parameterName - the name of the parameter
     * @param x - the object containing the input parameter value
     * @exception SQLException Feature not implemented for now.
     */
    public final void setObject(String parameterName, Object x)
            throws SQLException {
        throw Util.notImplemented();
    }

    // JDBC 4.0 methods

    /**
     * Retrieves the value of the designated parameter as a
     * <code>java.io.Reader</code> object in the Java programming language.
     * Introduced in JDBC 4.0.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return a <code>java.io.Reader</code> object that contains the parameter
     * value; if the value is SQL <code>NULL</code>, the value returned
     * is <code>null</code> in the Java programming language.
     * @throws SQLException if a database access error occurs or this method is
     * called on a closed <code>CallableStatement</code>
     */
    public final Reader getCharacterStream(int parameterIndex)
            throws SQLException {
        checkStatus();
        // Make sure the specified parameter has mode OUT or IN/OUT.
        switch (getParms().getParameterMode(parameterIndex)) {
            case (ParameterMetaData.parameterModeIn):
            case (ParameterMetaData.parameterModeUnknown):
                throw newSQLException(SQLState.LANG_NOT_OUTPUT_PARAMETER,
                        Integer.toString(parameterIndex));
        }
        Reader reader = null;
        int paramType = getParameterJDBCType(parameterIndex);
        switch (paramType) {
            // Handle character/string types.
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                boolean pushStack = false;
                Object syncObject = getConnectionSynchronization();
                synchronized (syncObject) {
                    try {
                        StringDataValue param = (StringDataValue) getParms().getParameterForGet(parameterIndex - 1);
                        if (param.isNull()) {
                            break;
                        }
                        pushStack = true;
                        setupContextStack();

                        if (param.hasStream()) {
                            CharacterStreamDescriptor csd =
                                    param.getStreamWithDescriptor();
                            reader = new UTF8Reader(csd, this, syncObject);
                        } else {
                            reader = new StringReader(param.getString());
                        }
                    } catch (Throwable t) {
                        throw EmbedResultSet.noStateChangeException(t);
                    } finally {
                        if (pushStack) {
                            restoreContextStack();
                        }
                    }
                } // End synchronized block
                break;

            // Handle binary types.
            // JDBC says to support these, but no defintion exists for the output.
            // Match JCC which treats the bytes as a UTF-16BE stream.
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                try {
                    InputStream is = getBinaryStream(parameterIndex);
                    if (is != null) {
                        reader = new InputStreamReader(is, "UTF-16BE");
                    }
                    break;
                } catch (UnsupportedEncodingException uee) {
                    throw newSQLException(uee.getMessage());
                }

            default:
                throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                        "java.io.Reader", Util.typeName(paramType));
        }
        // Update wasNull.
        wasNull = (reader == null);
        return reader;
    }

    /**
     * Get binary stream for a parameter.
     *
     * @param parameterIndex first parameter is 1, second is 2 etc.
     * @return a stream for the binary parameter, or <code>null</code>.
     *
     * @throws SQLException if a database access error occurs.
     */
    private InputStream getBinaryStream(int parameterIndex)
            throws SQLException {
        int paramType = getParameterJDBCType(parameterIndex);
        switch (paramType) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                break;
            default:
                throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                        "java.io.InputStream", Util.typeName(paramType));
        }

        boolean pushStack = false;
        synchronized (getConnectionSynchronization()) {
            try {
                DataValueDescriptor param =
                        getParms().getParameterForGet(parameterIndex - 1);
                wasNull = param.isNull();
                if (wasNull) {
                    return null;
                }
                pushStack = true;
                setupContextStack();

                InputStream stream; // The stream we will return to the user
                if (param.hasStream()) {
                    stream = new BinaryToRawStream(param.getStream(), param);
                } else {
                    stream = new ByteArrayInputStream(param.getBytes());
                }
                return stream;
            } catch (Throwable t) {
                throw EmbedResultSet.noStateChangeException(t);
            } finally {
                if (pushStack) {
                    restoreContextStack();
                }
            }
        } // End synchronized block
    }

    public final Reader getCharacterStream(String parameterName)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final Reader getNCharacterStream(int parameterIndex)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final Reader getNCharacterStream(String parameterName)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final String getNString(int parameterIndex)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final String getNString(String parameterName)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final void setBlob(String parameterName, Blob x)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final void setClob(String parameterName, Clob x)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final RowId getRowId(int parameterIndex) throws SQLException {
        throw Util.notImplemented();
    }

    public final RowId getRowId(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    public final void setRowId(String parameterName, RowId x) throws SQLException {
        throw Util.notImplemented();
    }

    public final void setNString(String parameterName, String value)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final void setNCharacterStream(String parameterName, Reader value, long length)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final void setNClob(String parameterName, NClob value) throws SQLException {
        throw Util.notImplemented();
    }

    public final void setClob(String parameterName, Reader reader, long length)
            throws SQLException {
        throw Util.notImplemented();

    }

    public final void setBlob(String parameterName, InputStream inputStream, long length)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final void setNClob(String parameterName, Reader reader, long length)
            throws SQLException {
        throw Util.notImplemented();
    }

    public final NClob getNClob(int i) throws SQLException {
        throw Util.notImplemented();
    }

    public final NClob getNClob(String parameterName) throws SQLException {
        throw Util.notImplemented();
    }

    public final void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw Util.notImplemented();

    }

    public final SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw Util.notImplemented();
    }

    public final SQLXML getSQLXML(String parametername) throws SQLException {
        throw Util.notImplemented();
    }

    public final void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        throw Util.notImplemented("setAsciiStream(String,InputStream)");
    }

    public final void setBinaryStream(String parameterName, InputStream x)
            throws SQLException {
        throw Util.notImplemented("setBinaryStream(String,InputStream)");
    }

    public final void setBlob(String parameterName, InputStream inputStream)
            throws SQLException {
        throw Util.notImplemented("setBlob(String,InputStream)");
    }

    public final void setCharacterStream(String parameterName, Reader reader)
            throws SQLException {
        throw Util.notImplemented("setCharacterStream(String,Reader)");
    }

    public final void setClob(String parameterName, Reader reader)
            throws SQLException {
        throw Util.notImplemented("setClob(String,Reader)");
    }

    public final void setNCharacterStream(String parameterName, Reader value)
            throws SQLException {
        throw Util.notImplemented("setNCharacterStream(String,Reader)");
    }

    public final void setNClob(String parameterName, Reader reader)
            throws SQLException {
        throw Util.notImplemented("setNClob(String,Reader)");
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */
    public final void setAsciiStream(String parameterName, InputStream x, long length)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */
    public final void setBinaryStream(String parameterName, InputStream x, long length)
            throws SQLException {
        throw Util.notImplemented();
    }

    /**
     * Sets the designated parameter to the given Reader, which will have the
     * specified number of bytes.
     *
     * @param parameterName the name of the first parameter
     * @param x the java Reader which contains the UNICODE value
     * @param length the number of bytes in the stream
     * @exception SQLException thrown on failure.
     *
     */
    public final void setCharacterStream(String parameterName, Reader x, long length)
            throws SQLException {
        throw Util.notImplemented();
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////
    public final <T> T getObject(int parameterIndex, Class<T> type)
            throws SQLException {
        checkStatus();

        if (type == null) {
            throw mismatchException("NULL", parameterIndex);
        }

        Object retval;

        if (String.class.equals(type)) {
            retval = getString(parameterIndex);
        } else if (BigDecimal.class.equals(type)) {
            retval = getBigDecimal(parameterIndex);
        } else if (Boolean.class.equals(type)) {
            retval = getBoolean(parameterIndex);
        } else if (Byte.class.equals(type)) {
            retval = getByte(parameterIndex);
        } else if (Short.class.equals(type)) {
            retval = getShort(parameterIndex);
        } else if (Integer.class.equals(type)) {
            retval = getInt(parameterIndex);
        } else if (Long.class.equals(type)) {
            retval = getLong(parameterIndex);
        } else if (Float.class.equals(type)) {
            retval = getFloat(parameterIndex);
        } else if (Double.class.equals(type)) {
            retval = getDouble(parameterIndex);
        } else if (Date.class.equals(type)) {
            retval = getDate(parameterIndex);
        } else if (Time.class.equals(type)) {
            retval = getTime(parameterIndex);
        } else if (Timestamp.class.equals(type)) {
            retval = getTimestamp(parameterIndex);
        } else if (Blob.class.equals(type)) {
            retval = getBlob(parameterIndex);
        } else if (Clob.class.equals(type)) {
            retval = getClob(parameterIndex);
        } else if (type.isArray() && type.getComponentType().equals(byte.class)) {
            retval = getBytes(parameterIndex);
        } else {
            retval = getObject(parameterIndex);
        }

        if (wasNull()) {
            retval = null;
        }

        if ((retval == null) || (type.isInstance(retval))) {
            return type.cast(retval);
        }

        throw mismatchException(type.getName(), parameterIndex);
    }

    private SQLException mismatchException(String targetTypeName, int parameterIndex)
            throws SQLException {
        String sourceTypeName = getParameterMetaData().getParameterTypeName(parameterIndex);
        return newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH, targetTypeName, sourceTypeName);
    }

    public final <T> T getObject(String parameterName, Class<T> type)
            throws SQLException {
        throw Util.notImplemented();
    }

}

