/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedSavepoint

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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.sql.Savepoint;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
 * This class implements the Savepoint interface from JDBC 3.0.
 * This allows to set, release, or rollback a transaction to
 * designated Savepoints. Savepoints provide finer-grained
 * control of transactions by marking intermediate points within
 * a transaction. Once a savepoint has been set, the transaction
 * can be rolled back to that savepoint without affecting preceding work.
   <P><B>Supports</B>
   <UL>
   <LI> JSR169 - no subsetting for java.sql.Savepoint
   <LI> JDBC 3.0 - class introduced in JDBC 3.0
   </UL>
 *
 * @see java.sql.Savepoint
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
final class EmbedSavepoint extends ConnectionChild
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
    EmbedSavepoint(EmbedConnection conn, String name)
    throws StandardException {
   		super(conn);
   		if (name == null) //this is an unnamed savepoint
   		{
            //Generating a unique internal name for unnamed savepoints
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
            savepointName = "i." + getLanguageConnectionContext( conn ).getUniqueSavepointName();
            savepointID = getLanguageConnectionContext( conn ).getUniqueSavepointID();
   		} else
   		{
				savepointName = "e." + name;
				savepointID = -1;
   		}
   		getLanguageConnectionContext( conn ).languageSetSavePoint(savepointName, this);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
   		if (savepointID == -1) {
			throw newSQLException(SQLState.NO_ID_FOR_NAMED_SAVEPOINT);
        }
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
   		if (savepointID != -1) {
			throw newSQLException(SQLState.NO_NAME_FOR_UNNAMED_SAVEPOINT);
        }
   		return savepointName.substring(2);
    }

    // Derby internally keeps name for both named and unnamed savepoints
    String getInternalName() {
   		return savepointName;
    }


    //bug 4468 - verify that savepoint rollback/release is for a savepoint from
    //the current connection
    boolean sameConnection(EmbedConnection con) {
   		return
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
             getLCC( getEmbedConnection() ) ==
             getLCC( con )
             );
    }

}
