/*

   Derby - Class org.apache.derby.client.am.SectionManager

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

package org.apache.derby.client.am;

import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.util.Hashtable;
import java.util.Stack;
import org.apache.derby.shared.common.reference.SQLState;


public class SectionManager {
    private Agent agent_;

    // The following stack of available sections is
    // for pooling and recycling previously used sections.
    // For performance, the section objects themselves are pooled,
    // rather than just keeping track of free section numbers;
    // this way, we don't have to new-up a section if one is available in the pool.
    private final Stack<Section> freeSectionsNonHold_;
    private final Stack<Section> freeSectionsHold_;

    private int nextAvailableSectionNumber_ = 1;

    // store package consistency token information and initialized in
    // setPKGNAMCBytes
    // holdPKGNAMCBytes stores PKGNAMCBytes when holdability is hold
    // noHoldPKGNAMCBytes stores PKGNAMCBytes when holdability is no hold
    byte[] holdPKGNAMCBytes = null;
    byte[] noHoldPKGNAMCBytes = null;


    private final static String packageNameWithHold__ = "SYSLH000";
    private final static String packageNameWithNoHold__ = "SYSLN000";

    private final static String cursorNamePrefixWithHold__ = "SQL_CURLH000C";
    private final static String cursorNamePrefixWithNoHold__ = "SQL_CURLN000C";

    // Jdbc 1 positioned updates are implemented via
    // sql scan for "...where current of <users-cursor-name>",
    // the addition of mappings from cursor names to query sections,
    // and the subtitution of <users-cursor-name> with <canned-cursor-name> in the pass-thru sql string
    // "...where current of <canned-cursor-name>" when user-defined cursor names are used.
    // Both "canned" cursor names (from our jdbc package set) and user-defined cursor names are mapped.
    // Statement.cursorName_ is initialized to null until the cursor name is requested or set.
    // When set (s.setCursorName()) with a user-defined name, then it is added to the cursor map at that time;
    // When requested (rs.getCursorName()), if the cursor name is still null,
    // then is given the canned cursor name as defined by our jdbc package set and added to the cursor map.
    // Still need to consider how positioned updates should interact with multiple result sets from a stored.
    private final Hashtable<String, Section>
        positionedUpdateCursorNameToQuerySection_ =
            new Hashtable<String, Section>();

    // Cursor name to ResultSet mapping is needed for positioned updates to check whether
    // a ResultSet is scrollable.  If so, exception is thrown.
    private final Hashtable<String, WeakReference<ClientResultSet>>
        positionedUpdateCursorNameToResultSet_ =
            new Hashtable<String, WeakReference<ClientResultSet>>();

    private final int maxNumSections_ = 32768;

    public SectionManager(Agent agent) {
        agent_ = agent;
        freeSectionsNonHold_ = new Stack<Section>();
        freeSectionsHold_ = new Stack<Section>();
    }

    /**
     * Store the Packagename and consistency token information This is called from Section.setPKGNAMCBytes
     *
     * @param b                    bytearray that has the PKGNAMC information to be stored
     * @param resultSetHoldability depending on the holdability store it in the correct byte array packagename and
     *                             consistency token information for when holdability is set to HOLD_CURSORS_OVER_COMMIT
     *                             is stored in holdPKGNAMCBytes and in noHoldPKGNAMCBytes when holdability is set to
     *                             CLOSE_CURSORS_AT_COMMIT
     */
    void setPKGNAMCBytes(byte[] b, int resultSetHoldability) {
        if (resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            agent_.sectionManager_.holdPKGNAMCBytes = b;
        } else if (resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            agent_.sectionManager_.noHoldPKGNAMCBytes = b;
        }
    }


    //------------------------entry points----------------------------------------

    // Get a section for either a jdbc update or query statement.
    Section getDynamicSection(int resultSetHoldability) throws SqlException {
        if (resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            return getSection(freeSectionsHold_, packageNameWithHold__, cursorNamePrefixWithHold__, resultSetHoldability);
        } else if (resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            return getSection(freeSectionsNonHold_, packageNameWithNoHold__, cursorNamePrefixWithNoHold__, resultSetHoldability);
        } else {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.UNSUPPORTED_HOLDABILITY_PROPERTY), 
                resultSetHoldability);
        }
    }

    private Section getSection(
            Stack freeSections,
            String packageName,
            String cursorNamePrefix,
            int resultSetHoldability) throws SqlException {

        if (!freeSections.empty()) {
            return (Section) freeSections.pop();
        } else if (nextAvailableSectionNumber_ < (maxNumSections_ - 1)) {
            String cursorName = cursorNamePrefix + nextAvailableSectionNumber_;
            Section section = new Section(agent_, packageName, nextAvailableSectionNumber_, cursorName, resultSetHoldability);
            nextAvailableSectionNumber_++;
            return section;
        } else
        // unfortunately we have run out of sections
        {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.EXCEEDED_MAX_SECTIONS),
                "32000");
        }
    }

    void freeSection(Section section, int resultSetHoldability) {
        if (resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            this.freeSectionsHold_.push(section);
        } else if (resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            this.freeSectionsNonHold_.push(section);
        }
    }

    // Get a section for a jdbc 2 positioned update/delete for the corresponding query.
    // A positioned update section must come from the same package as its query section.
    Section getPositionedUpdateSection(Section querySection) throws SqlException {
        ClientConnection connection = agent_.connection_;
        return getDynamicSection(connection.holdability());
    }

    // Get a section for a jdbc 1 positioned update/delete for the corresponding query.
    // A positioned update section must come from the same package as its query section.
    Section getPositionedUpdateSection(String cursorName, boolean useExecuteImmediateSection) throws SqlException {
        Section querySection = (Section) positionedUpdateCursorNameToQuerySection_.get(cursorName);

        // If querySection is null, then the user must have provided a bad cursor name.
        // Otherwise, get a new section and save the client's cursor name and the server's
        // cursor name to the new section.
        if (querySection != null) {
            Section section = getPositionedUpdateSection(querySection);
            // getPositionedUpdateSection gets the next available section from the query
            // package, and it has a different cursor name associated with the section.
            // We need to save the client's cursor name and server's cursor name to the
            // new section.
            section.setClientCursorName(querySection.getClientCursorName());
            section.serverCursorNameForPositionedUpdate_ = querySection.getServerCursorName();
            return section;
        } else {
            return null;
        }
    }

    void mapCursorNameToQuerySection(String cursorName, Section section) {
        positionedUpdateCursorNameToQuerySection_.put(cursorName, section);
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    void mapCursorNameToResultSet(
            String cursorName,
            ClientResultSet resultSet) {

        // DERBY-3316. Needs WeakReference so that ResultSet can be garbage collected
        positionedUpdateCursorNameToResultSet_.put(
                cursorName, new WeakReference<ClientResultSet>(resultSet));
    }

    ClientResultSet getPositionedUpdateResultSet(String cursorName)
            throws SqlException {
        ClientResultSet rs =
            positionedUpdateCursorNameToResultSet_.get(cursorName).get();
        if (rs == null) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.LANG_RESULT_SET_NOT_OPEN), "update");
        }
        return (rs.resultSetType_ == ResultSet.TYPE_FORWARD_ONLY) ? null : rs;
    }

    void removeCursorNameToResultSetMapping(String clientCursorName,
                                            String serverCursorName) {
        if (clientCursorName != null) {
            positionedUpdateCursorNameToResultSet_.remove(clientCursorName);
        }
        if (serverCursorName != null) {
            positionedUpdateCursorNameToResultSet_.remove(serverCursorName);
        }
    }

    void removeCursorNameToQuerySectionMapping(String clientCursorName,
                                               String serverCursorName) {
        if (clientCursorName != null) {
            positionedUpdateCursorNameToQuerySection_.remove(clientCursorName);
        }
        if (serverCursorName != null) {
            positionedUpdateCursorNameToQuerySection_.remove(serverCursorName);
        }
    }

}

