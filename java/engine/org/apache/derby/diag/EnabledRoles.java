/*

   Derby - Class org.apache.derby.diag.EnabledRoles

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.diag;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.RoleClosureIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.vti.VTITemplate;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;


/**
 * EnabledRoles shows all enabled roles for the current session.
 *
 * <p>To use it, query it as follows:
 * </p>
 * <pre> SELECT * FROM SYSCS_DIAG.ENABLED_ROLES; </pre>
 [
 * <p>The following columns will be returned:
 *    <ul><li>ROLEID -- VARCHAR(128) NOT NULL
 *        </li>
 *    </ul>
 *
 */
public final class EnabledRoles extends VTITemplate {

    RoleClosureIterator rci;
    String nextRole;
    boolean initialized;

    public EnabledRoles() {
    }

    /**
     * @see java.sql.ResultSet#next
     */
    public boolean next() throws SQLException {
        try {
			// Need to defer initialization here to make sure we have an
			// activation.
            if (!initialized) {
                initialized = true;
                LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
                String role = lcc.getCurrentRoleId(lcc.getLastActivation());

                if (role != null) {
                    DataDictionary dd = lcc.getDataDictionary();
                    lcc.beginNestedTransaction(true);
                    try {
                        int mode = dd.startReading(lcc);
                        try {
                            rci = dd.createRoleClosureIterator
                                (lcc.getLastActivation().
                                     getTransactionController(),
                                 role, true);
                        } finally {
                            dd.doneReading(mode, lcc);
                        }
                    } finally {
                        // make sure we commit; otherwise, we will end up with
                        // mismatch nested level in the language connection
                        // context.
                        lcc.commitNestedTransaction();
                    }
                }
            }

            return rci != null && ((nextRole = rci.next()) != null);

        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }


    /**
     * @see java.sql.ResultSet#close
     */
    public void close() {
    }


    /**
     * @see java.sql.ResultSet#getMetaData
     */
    public ResultSetMetaData getMetaData() {
        return metadata;
    }

    /**
     * @see java.sql.ResultSet#getString
     */
    public String getString(int columnIndex) throws SQLException {
        return nextRole;
    }

    /*
     * Metadata
     */
    private static final ResultColumnDescriptor[] columnInfo = {
        EmbedResultSetMetaData.getResultColumnDescriptor
        ("ROLEID", Types.VARCHAR, false, Limits.MAX_IDENTIFIER_LENGTH)
    };

    private static final ResultSetMetaData metadata =
		new EmbedResultSetMetaData(columnInfo);

}
