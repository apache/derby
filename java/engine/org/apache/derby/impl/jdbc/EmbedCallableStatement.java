/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedCallableStatement

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.SQLState;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Local implementation.
 *
 * @author ames
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
			  JDBC30Translation.NO_GENERATED_KEYS,
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
			ParameterValueSet pvs = getParms();
			try
			{
				pvs.validate();

			} catch (StandardException e)
			{
				throw EmbedResultSet.noStateChangeException(e);
			}

			/* KLUDGE - ? = CALL ... returns a ResultSet().  We
			 * need executeUpdate to be false in that case.
			 */
			boolean execResult = super.executeStatement(a, executeQuery,
				(executeUpdate && (! hasReturnOutputParameter)));

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
					pvs.setReturnValue(results.getObject(1));

				} catch (StandardException e)
				{
					throw EmbedResultSet.noStateChangeException(e);
				}
				finally {
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
	 * @see CallableStatement#getBigDecimal
     * @exception SQLException NoOutputParameters thrown.
     */
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
	{
		checkStatus();
		try {
			BigDecimal v =  getParms().getParameterForGet(parameterIndex-1).getBigDecimal();
			wasNull = (v == null);
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
    public Date getDate(int parameterIndex) throws SQLException
	{
		checkStatus();
		try {
			Date v =  getParms().getParameterForGet(parameterIndex-1).getDate(getCal());
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
	public Time getTime(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			Time v =  getParms().getParameterForGet(parameterIndex-1).getTime(getCal());
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
    public Timestamp getTimestamp(int parameterIndex)
	    throws SQLException 
	{
		checkStatus();
		try {
			Timestamp v =  getParms().getParameterForGet(parameterIndex-1).getTimestamp(getCal());
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getObject
     * @exception SQLException NoOutputParameters thrown.
     */
	public Object getObject(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			Object v = getParms().getObject(parameterIndex-1);
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

