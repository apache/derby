/*  Derby - Class org.apache.derbyBuild.JiraIssue
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
package org.apache.derbyBuild;

import org.w3c.dom.*;
import java.io.InputStream;
import java.util.*;


/**
 *
 * An issue from a JIRA report. The constructor of this class parses text produced by
 * a JIRA report. This parsing logic probably has to be rewritten for every release because
 * the format of the JIRA reports is not stable.
 *
 */
class JiraIssue {
    private static final String RELEASE_NOTE_NAME = "releaseNote.html";

    private String key;
    private String title;
    private long releaseNoteAttachmentID = ReportParser.NO_RELEASE_NOTE;
    private HashSet fixVersionSet;

    /**
     * Create an object instance from a TagReader.
     */
    public JiraIssue(  ReportParser rp, TagReader tr ) throws Exception
    {
        key = rp.parseKey( tr );
        title = rp.parseTitle( tr );
        fixVersionSet = rp.parseFixedVersions( tr );
        releaseNoteAttachmentID = rp.getReleaseNoteAttachmentID( tr );
    }

    /**
     * Factory method which extracts a list of JiraIssue objects from a Jira
     * report (supplied as an XML Document). Issues with a fixVersion contained
     * in the exclude list will be omitted from the list.
     * @param masterReport a TagReader holding the JIRA report of all the fixed bugs
     * @param excludeReleaseIDList list of fixVersions that disqualifies an issue
     * @param parser a class to parse content in the master report
     * @return a List of JiraIssue objects
     * @throws java.lang.Exception
     */
    public static List createJiraIssueList
        ( TagReader masterReport, List excludeReleaseIDList, ReportParser parser ) throws Exception
    {
        int issueCount = 0;

        ArrayList jiraIssues = new ArrayList();

        while( true )
        {
            TagReader nextIssue = parser.parseNextIssue( masterReport );
            if ( nextIssue == null ) { break; }

            JiraIssue candidate = new JiraIssue( parser, nextIssue );

            boolean skip = false;
            for (Iterator ex = excludeReleaseIDList.iterator(); ex.hasNext();)
            {
                String rid = (String) ex.next();
                if (candidate.isFixedIn(rid))
                {
                    //System.out.println("Already fixed: "+candidate.getKey()+ " (in "+rid+")");
                    skip=true;
                    break;
                }
            }
            if (!skip)
            {
                //System.out.println("adding: " + candidate.getKey());
                jiraIssues.add(candidate);
            }
        }

        return jiraIssues;
    }

    /**
     * @return the issue's key (jira number, e.g., 1234)
     */
    public String getKey() {
        return key;
    }

    /**
     * @return the issue's title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @return the attachment id of the release note
     */
    public long getReleaseNoteAttachmentID() {
        return releaseNoteAttachmentID;
    }

    /**
     * @return true iff this issue has a release note attached
     */
    public boolean hasReleaseNote() {
        return (releaseNoteAttachmentID > ReportParser.NO_RELEASE_NOTE);
    }

    /**
     * Predicate for finding out if issue has a given fixVersion.
     * @param version to test
     * @return true iff issue has version as fixVersion
     */
    public boolean isFixedIn(String version) {
        return fixVersionSet.contains(version);
    }

    /**
     * @return URL for this Jira issue
     */
    public String getJiraAddress() {
        return "https://issues.apache.org/jira/browse/DERBY-" + key;
    }

    /**
     * @return Full URL to the latest release note
     */
    public String getReleaseNoteAddress() {
        return "https://issues.apache.org/jira/secure/attachment/" +
                releaseNoteAttachmentID + "/releaseNote.html";
    }
}


