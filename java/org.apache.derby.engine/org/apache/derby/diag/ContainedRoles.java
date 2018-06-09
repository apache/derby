/*

   Derby - Class org.apache.derby.diag.ContainedRoles

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
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.shared.common.error.PublicAPI;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.vti.VTITemplate;

import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;


/**
 * Contained roles shows all roles contained in the given identifier, or if the
 * second argument, if given, is not 0, the inverse relation; all roles who
 * contain the given role identifier.
 *
 * <p>To use it, query it as follows:
 * </p>
 * <pre> SELECT * FROM TABLE(SUSCS_DIAG.CONTAINED_ROLES('FOO')) t; </pre>
 * <pre> SELECT * FROM TABLE(CONTAINED_ROLES('FOO', 1)) t; </pre>
 *
 * <p>The following columns will be returned:
 *    <ul><li>ROLEID -- VARCHAR(128) NOT NULL
 *    </ul>
 * </p>
 */
public class ContainedRoles extends VTITemplate {

    RoleClosureIterator rci;
    String nextRole;
    boolean initialized;
    String role;
    boolean inverse;

    /**
     * Constructor.
     *
     * @param roleid The role identifier for which we want to find the set of
     *               contained roles (inclusive). The identifier is expected to
     *               be in SQL form (not case normal form).
     * @param inverse If != 0, use the inverse relation: find those roles which
     *                all contain roleid (inclusive).
     * @throws SQLException This is a public API, so the internal exception is
     *                      wrapped in SQLException.
     */
    public ContainedRoles(String roleid, int inverse) throws SQLException {
        try {
            if (roleid != null) {
                role = IdUtil.parseSQLIdentifier(roleid);
            }

            this.inverse = (inverse != 0);
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    /**
     * Constructor.
     *
     * @param roleid The role identifier for which we want to find the set of
     *               contained roles (inclusive). The identifier is expected to
     *               be in SQL form (not case normal form).
     * @throws SQLException This is a public API, so the internal exception is
     *                      wrapped in SQLException.
     */
    public ContainedRoles(String roleid)  throws SQLException {
        this(roleid, 0);
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
                DataDictionary dd = lcc.getDataDictionary();
                RoleGrantDescriptor rdDef =
                    dd.getRoleDefinitionDescriptor(role);

                if (rdDef != null) {
                    lcc.beginNestedTransaction(true);
                    try {
                        int mode = dd.startReading(lcc);
                        try {
                            rci = dd.createRoleClosureIterator
                                (lcc.getLastActivation().
                                     getTransactionController(),
                                 role, !inverse);
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
