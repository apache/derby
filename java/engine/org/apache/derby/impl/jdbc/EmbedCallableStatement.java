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

import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.SQLState;

import java.net.URL;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Local implementation.
 *
 */
public abstract class EmbedCallableStatement extends EmbedPreparedStatement
	implements CallableStatement
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
			throw newSQLException(SQLState.BAD_SCALE_VALUE, new Integer(scale));
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
	 * Registers the designated output parameter
	 *
	 * @exception SQLException if a database-access error occurs.
	 */
 	public void registerOutParameter(int parameterIndex, int sqlType, 
 									 String typeName) 
 		 throws SQLException
 	{
 		throw Util.notImplemented("registerOutParameter");
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
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			Blob v = (Blob) param.getObject();
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
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
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			Clob v = (Clob) param.getObject();
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
    }
    
	public void addBatch() throws SQLException {

		checkStatus();
		ParameterValueSet pvs = getParms();

		int numberOfParameters = pvs.getParameterCount();

		for (int j=1; j<=numberOfParameters; j++) {

			switch (pvs.getParameterMode(j)) {
			case JDBC30Translation.PARAMETER_MODE_IN:
			case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
				break;
			case JDBC30Translation.PARAMETER_MODE_OUT:
			case JDBC30Translation.PARAMETER_MODE_IN_OUT:
				throw newSQLException(SQLState.OUTPUT_PARAMS_NOT_ALLOWED);
			}
		}

		super.addBatch();
	}
}

