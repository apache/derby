/*

   Derby - Class org.apache.derby.iapi.sql.conn.SQLSessionContext

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

package org.apache.derby.iapi.sql.conn;

import java.lang.String;
import java.util.HashMap;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

/**
 * An implementation of this interface encapsulates some of the SQL
 * session context's state variables, cf. SQL 2003, section 4.37.3,
 * notably those which we need to save and restore when entering a
 * stored procedure or function (which can contain SQL and thus a
 * nested connection), cf. 4.37.3, 4.27.3 and 4.34.1.1.  <p> Presently
 * this set contains the following properties: <ul> <li>current
 * role</li> <li>current schema</li> </ul>
 *
 * The standard specifies that the authorization stack be copied onto
 * the new SQL session context before it is pushed (and possibly
 * modifed) with a new cell for the authorization ids (user, role). In
 * our implementation we merge these two stacks for now. Also, the
 * authorization id of current user is not represented yet, since it
 * can not be modified in a session; Derby can not run routines with
 * definer's rights yet.
 * <p>
 * SQL session context is implemented as follows: Statements at root
 * connection level use the instance held by the the lcc, nested
 * connections maintain instances of SQLSessionContext, held by the
 * activation of the calling statement. This forms a logical stack as
 * required by the standard. The statement context also holds a
 * reference to the current SQLSessionContext.
 * <p>
 * When a dynamic result set references e.g. current role, the value
 * retrieved will always be that of the current role when the
 * statement is logically executed (inside procedure/function), not
 * the current value when the result set is accessed outside the
 * stored procedure/function.  This works since the nested SQL session
 * context is kept by the caller activation, so even though the
 * statement context of the call has been popped, we can get at the
 * final state of the nested SQL session context since the caller's
 * activation is alive as long as dynamic result sets need it).
 * <p>
 * If more than one nested connection is used inside a shared
 * procedure, they will share the same nested SQL session
 * context. Since the same dynamic call context is involved, this
 * seems correct.
 *
 * @see LanguageConnectionContext#pushNestedSessionContext
 */

public interface SQLSessionContext {

    /**
     * Set the SQL role of this SQL connection context
     */
    public void setRole(String role);

    /**
     * Get the SQL role of this SQL connection context
     */
    public String getRole();

    /**
     * Set the SQL current user of this SQL connection context
     */
    public void setUser(String user);

    /**
     * Get the SQL current user of this SQL connection context
     */
    public String getCurrentUser();

    /**
     * Set the schema of this SQL connection context
     */
    public void setDefaultSchema(SchemaDescriptor sd);

    /**
     * Get the schema of this SQL connection context
     */
    public SchemaDescriptor getDefaultSchema();

    /**
     * Get a handle to the session's constraint modes.
     * The caller is responsible for any cloning needed.
     * @return constraint modes map
     */
    public HashMap<Long, Boolean> getUniquePKConstraintModes();

    /**
     * Get a handle to the session's check constraint modes.
     * The caller is responsible for any cloning needed.
     * @return constraint modes map
     */
    public HashMap<UUID, Boolean> getCheckConstraintModes();

    /**
     * Initialize a inferior session context with the constraint mode map
     * of the parent session context.
     * @param hm constraint mode map
     */
    public void setConstraintModes(HashMap<Long, Boolean> hm);

    /**
     * Initialize a inferior session context with the check constraint mode map
     * of the parent session context.
     * @param hm constraint mode map
     */
    public void setCheckConstraintModes(HashMap<UUID, Boolean> hm);

    /**
     * Set the constraint mode for this constraint/index to {@code deferred}.
     * If {@code deferred} is {@code false}, to immediate checking,
     * if {@code true} to deferred checking.
     *
     * @param conglomId The conglomerate id of the backing index for the
     *                  constraint .
     * @param deferred  The new constraint mode
     */
    public void setDeferred(long conglomId, boolean deferred);

    /**
     * Set the constraint mode for this constraint to {@code deferred}.
     * If {@code deferred} is {@code false}, to immediate checking,
     * if {@code true} to deferred checking.
     *
     * @param constraintId The constraint id
     * @param deferred  The new constraint mode
     */
    public void setDeferred(UUID constraintId, boolean deferred);

    /**
     * Return {@code Boolean.TRUE} if the constraint mode for this
     * constraint/index has been set to deferred, {@code Boolean.FALSE} if
     * it has been set to immediate.  Any ALL setting is considered also.
     * If the constraint mode hasn't been set for this constraint,
     * return {@code null}. The constraint mode is the effectively the initial
     * constraint mode in this case.
     */
    public Boolean isDeferred(long conglomId);

    /**
     * Return {@code Boolean.TRUE} if the constraint mode for this
     * constraint/index has been set to deferred, {@code Boolean.FALSE} if
     * it has been set to immediate.  Any ALL setting is considered also.
     * If the constraint mode hasn't been set for this constraint,
     * return {@code null}. The constraint mode is the effectively the initial
     * constraint mode in this case.
     *
     * @param constraintId the constraint id
     * @return {@code Boolean.TRUE} if the constraint mode for this
     * constraint/index has been set to deferred, {@code Boolean.FALSE} if
     * it has been set to immediate.
     */
    public Boolean isDeferred(UUID constraintId);

    /**
     * Clear deferred information for this transaction.
     */
    public void resetConstraintModes();

    /**
     * Set the constraint mode for all deferrable constraints to
     * {@code deferred}.
     * If {@code deferred} is {@code false}, set to immediate checking,
     * if {@code true} to deferred checking.
     * {@code null} is allowed: it means no ALL setting exists.
     *
     * @param deferred the mode to set
     */
    public void setDeferredAll(Boolean deferred);

    /**
     * Get state of DEFERRED ALL setting.
     *
     * @return {@code True} is deferred all constraint mode has been
     *         set for this session context.
     *         {@code False} is deferred immediate has been set for this
     *         session context.
     *         {@code null} means no ALL setting has been made for this context
     */
    public Boolean getDeferredAll();

}
