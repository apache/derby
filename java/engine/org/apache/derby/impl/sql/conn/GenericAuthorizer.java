/*

   Derby - Class org.apache.derby.impl.sql.conn.GenericAuthorizer

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.conn;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import java.util.Properties;

class GenericAuthorizer
implements Authorizer
{
	//
	//Enumerations for user access levels.
	private static final int NO_ACCESS = 0;
	private static final int READ_ACCESS = 1;
	private static final int FULL_ACCESS = 2;
	
	//
	//Configurable userAccessLevel - derived from Database level
	//access control lists + database boot time controls.
	private int userAccessLevel;

	//
	//Connection's readOnly status
    boolean readOnlyConnection;

	private final LanguageConnectionContext lcc;
	
	private final String authorizationId; //the userName after parsing by IdUtil 
	
	GenericAuthorizer(String authorizationId, 
						     LanguageConnectionContext lcc,
					         boolean sqlConnection)
		 throws StandardException
	{
		this.lcc = lcc;
		this.authorizationId = authorizationId;

		//we check the access level only if this is coming from a sql
		//connection, not internal logSniffer or StageTrunc db connection
		if(sqlConnection)
			refresh();
	}

	/*
	  Return true if the connection must remain readOnly
	  */
	private boolean connectionMustRemainReadOnly()
	{
		if (lcc.getDatabase().isReadOnly() ||
			(userAccessLevel==READ_ACCESS))
			return true;
		else
			return false;
	}

	/**
	  @see Authorizer#authorize
	  @exception StandardException Thrown if the operation is not allowed
	 */
	public void authorize(int operation) throws StandardException
	{
		int sqlAllowed = lcc.getStatementContext().getSQLAllowed();

		switch (operation)
		{
		case Authorizer.SQL_ARBITARY_OP:
		case Authorizer.SQL_CALL_OP:
			if (sqlAllowed == RoutineAliasInfo.NO_SQL)
				throw externalRoutineException(operation, sqlAllowed);
			break;
		case Authorizer.SQL_SELECT_OP:
			if (sqlAllowed > RoutineAliasInfo.READS_SQL_DATA)
				throw externalRoutineException(operation, sqlAllowed);
			break;

		// SQL write operations
		case Authorizer.SQL_WRITE_OP:
		case Authorizer.PROPERTY_WRITE_OP:
			if (isReadOnlyConnection())
				throw StandardException.newException(SQLState.AUTH_WRITE_WITH_READ_ONLY_CONNECTION);
			if (sqlAllowed > RoutineAliasInfo.MODIFIES_SQL_DATA)
				throw externalRoutineException(operation, sqlAllowed);
			break;

		// SQL DDL operations
		case Authorizer.JAR_WRITE_OP:
		case Authorizer.SQL_DDL_OP:
 			if (isReadOnlyConnection())
				throw StandardException.newException(SQLState.AUTH_DDL_WITH_READ_ONLY_CONNECTION);

			if (sqlAllowed > RoutineAliasInfo.MODIFIES_SQL_DATA)
				throw externalRoutineException(operation, sqlAllowed);
			break;

		default:
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Bad operation code "+operation);
		}
	}

	private static StandardException externalRoutineException(int operation, int sqlAllowed) {

		String sqlState;
		if (sqlAllowed == RoutineAliasInfo.READS_SQL_DATA)
			sqlState = SQLState.EXTERNAL_ROUTINE_NO_MODIFIES_SQL;
		else if (sqlAllowed == RoutineAliasInfo.CONTAINS_SQL)
		{
			switch (operation)
			{
			case Authorizer.SQL_WRITE_OP:
			case Authorizer.PROPERTY_WRITE_OP:
			case Authorizer.JAR_WRITE_OP:
			case Authorizer.SQL_DDL_OP:
				sqlState = SQLState.EXTERNAL_ROUTINE_NO_MODIFIES_SQL;
				break;
			default:
				sqlState = SQLState.EXTERNAL_ROUTINE_NO_READS_SQL;
				break;
			}
		}
		else
			sqlState = SQLState.EXTERNAL_ROUTINE_NO_SQL;

		return StandardException.newException(sqlState);
	}
	

	/**
	  @see Authorizer#getAuthorizationId
	  */
	public String getAuthorizationId()
	{
		return authorizationId;
	}

	private void getUserAccessLevel() throws StandardException
	{
		userAccessLevel = NO_ACCESS;
		if (userOnAccessList(Property.FULL_ACCESS_USERS_PROPERTY))
			userAccessLevel = FULL_ACCESS;

		if (userAccessLevel == NO_ACCESS &&
			userOnAccessList(Property.READ_ONLY_ACCESS_USERS_PROPERTY))
			userAccessLevel = READ_ACCESS;

		if (userAccessLevel == NO_ACCESS)
			userAccessLevel = getDefaultAccessLevel();
	}

	private int getDefaultAccessLevel() throws StandardException
	{
		PersistentSet tc = lcc.getTransactionExecute();
		String modeS = (String)
			PropertyUtil.getServiceProperty(
									tc,
									Property.DEFAULT_CONNECTION_MODE_PROPERTY);
		if (modeS == null)
			return FULL_ACCESS;
		else if(StringUtil.SQLEqualsIgnoreCase(modeS, Property.NO_ACCESS))
			return NO_ACCESS;
		else if(StringUtil.SQLEqualsIgnoreCase(modeS, Property.READ_ONLY_ACCESS))
			return READ_ACCESS;
		else if(StringUtil.SQLEqualsIgnoreCase(modeS, Property.FULL_ACCESS))
			return FULL_ACCESS;
		else
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Invalid value for property "+
										  Property.DEFAULT_CONNECTION_MODE_PROPERTY+
										  " "+
										  modeS);
 			return FULL_ACCESS;
		}
	}

	private boolean userOnAccessList(String listName) throws StandardException
	{
		PersistentSet tc = lcc.getTransactionExecute();
		String listS = (String)
			PropertyUtil.getServiceProperty(tc, listName);
		return IdUtil.idOnList(authorizationId,listS);
	}

	/**
	  @see Authorizer#isReadOnlyConnection
	 */
	public boolean isReadOnlyConnection()
	{
		return readOnlyConnection;
	}

	/**
	  @see Authorizer#isReadOnlyConnection
	  @exception StandardException Thrown if the operation is not allowed
	 */
	public void setReadOnlyConnection(boolean on, boolean authorize)
		 throws StandardException
	{
		if (authorize && !on) {
			if (connectionMustRemainReadOnly())
				throw StandardException.newException(SQLState.AUTH_CANNOT_SET_READ_WRITE);
		}
		readOnlyConnection = on;
	}

	/**
	  @see Authorizer#refresh
	  @exception StandardException Thrown if the operation is not allowed
	  */
	public void refresh() throws StandardException
	{
		getUserAccessLevel();
		if (!readOnlyConnection)
			readOnlyConnection = connectionMustRemainReadOnly();

		// Is a connection allowed.
		if (userAccessLevel == NO_ACCESS)
			throw StandardException.newException(SQLState.AUTH_DATABASE_CONNECTION_REFUSED);
	}
}
