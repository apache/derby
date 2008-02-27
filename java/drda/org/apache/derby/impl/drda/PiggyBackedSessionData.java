/*

   Derby - Class org.apache.derby.impl.drda.PiggyBackedSessionData

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

package org.apache.derby.impl.drda;

import java.sql.SQLException;
import org.apache.derby.iapi.jdbc.EngineConnection;


/**
 * Tracks the most recently piggy-backed session attributes, and provides
 * methods to determine if they have been modified and need to be re-sent
 * to the client.
 */
class PiggyBackedSessionData {
    private int iso_;
    private boolean isoMod_;

    private String schema_;
    private boolean schemaMod_;

    private final EngineConnection conn_;

    /**
     * Get a reference (handle) to the PiggyBackedSessionData object. Null will
     * be returned either if the conn argument is not valid, or if the
     * createOnDemand argument is false and the existing argument is null.
     * @param existing the PBSD object from the previous piggybacking or null if
     * none has yet taken place
     * @param conn the current EngineConnection
     * @param createOnDemand if true; create the instance when needed
     * @return a reference to the PBSD object or null
     * @throws java.sql.SQLException
     */
    public static PiggyBackedSessionData getInstance(
            PiggyBackedSessionData existing, EngineConnection conn,
            boolean createOnDemand) throws SQLException {
        if (conn == null || conn.isClosed() ||
                (existing != null && existing.conn_ != conn)) {
            return null;
        }
        if (existing == null && createOnDemand) {
            return new PiggyBackedSessionData(conn);
        }
        return existing;
    }

    /**
     * Constructs a new instance with an associated EngineConnection.
     * A newly constructed instance is invalid. refresh() must be called before
     * the xModified() methods can be used.
     * @param conn the connection to obtain data from
     */
    private PiggyBackedSessionData(EngineConnection conn) throws SQLException {
        conn_ = conn;
        iso_ = -1; // Initialize to an illegal value
    }

    /**
     * Refresh with the latest session attribute values from
     * the connection. Any changes will be reflected in the corresponding
     * xModified() methods, until setUnmodified() is called.
     */
    public void refresh() throws SQLException {
        setUnmodified();
        int iso = conn_.getTransactionIsolation();
        if (iso != iso_) {
            isoMod_ = true;
            iso_ = iso;
        }
        String schema = conn_.getCurrentSchemaName();
        if (!schema.equals(schema_)) {
            schemaMod_ = true;
            schema_ = schema;
        }
    }

    /**
     * Clear the modified status. Called after session attributes have
     * been sent to the client so that the xModified methods will
     * return false.
     */
    public void setUnmodified() {
        isoMod_ = false;
        schemaMod_ = false;
    }

    /**
     * @return true if the isolation level was modified by the last call
     * to fetchLatest
     */
    public boolean isIsoModified() {
        return isoMod_;
    }

    /**
     * @return true if the current schema name was modified by the last
     * call to fetchLatest
     */
    public boolean isSchemaModified() {
        return schemaMod_;
    }

    /**
     * @return true if any piggy-backed session attribute was modified by
     * the last call to fetchLatest
     */
    public boolean isModified() {
        return (isoMod_ || schemaMod_);
    }

    /**
     * @return the saved jdbc isolation level
     */
    public int getIso() {
        return iso_;
    }

    /**
     * @return the saved schema name
     */
    public String getSchema() {
        return schema_;
    }

    public String toString() {
        return "iso:" + iso_ + (isoMod_ ? "(M)" : "") + " schema:" + schema_ +
            (schemaMod_ ? "(M)" : "");
    }
}
