/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedSavepoint30

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.ConnectionChild;
import org.apache.derby.impl.jdbc.Util;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

import java.sql.Savepoint;
import java.sql.SQLException;

/**
 * This class implements the Savepoint interface from JDBC3.0
 * This allows to set, release, or rollback a transaction to
 * designated Savepoints. Savepoints provide finer-grained
 * control of transactions by marking intermediate points within
 * a transaction. Once a savepoint has been set, the transaction
 * can be rolled back to that savepoint without affecting preceding work.
 *
 * @see java.sql.Savepoint
 *
 */
final class EmbedSavepoint30 extends ConnectionChild
    implements Savepoint {

    //In order to avoid name conflict, the external names are prepanded
    //with "e." and internal names always start with "i." This is for bug 4467
    private final String savepointName;
    private final int savepointID;

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
	/*
		Constructor assumes caller will setup context stack
		and restore it.
	    @exception SQLException on error
	 */
    EmbedSavepoint30(EmbedConnection conn, String name)
    throws StandardException {
   		super(conn);
   		if (name == null) //this is an unnamed savepoint
   		{
				//Generating a unique internal name for unnamed savepoints
				savepointName = "i." + conn.getLanguageConnection().getUniqueSavepointName();
				savepointID = conn.getLanguageConnection().getUniqueSavepointID();
   		} else
   		{
				savepointName = "e." + name;
				savepointID = -1;
   		}
   		conn.getLanguageConnection().languageSetSavePoint(savepointName, this);
    }

	/**
    *
    * Retrieves the generated ID for the savepoint that this Savepoint object
    * represents.
    *
    * @return the numeric ID of this savepoint
    * @exception SQLException if this is a named savepoint
    */
    public int getSavepointId() throws SQLException {
   		if (savepointID == -1)
			throw newSQLException(SQLState.NO_ID_FOR_NAMED_SAVEPOINT);
   		return savepointID;
    }

	/**
    *
    * Retrieves the name of the savepoint that this Savepoint object
    * represents.
    *
    * @return the name of this savepoint
    * @exception SQLException if this is an un-named savepoint
    */
    public String getSavepointName() throws SQLException {
   		if (savepointID != -1)
			throw newSQLException(SQLState.NO_NAME_FOR_UNNAMED_SAVEPOINT);
   		return savepointName.substring(2);
    }

    //Cloudscape internally keeps name for both named and unnamed savepoints
    String getInternalName() {
   		return savepointName;
    }


    //bug 4468 - verify that savepoint rollback/release is for a savepoint from
    //the current connection
    boolean sameConnection(EmbedConnection con) {
   		return (getEmbedConnection().getLanguageConnection() == con.getLanguageConnection());
    }
}

