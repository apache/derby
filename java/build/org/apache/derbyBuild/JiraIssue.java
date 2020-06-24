/*

   Derby - Class org.apache.derbyBuild.JiraIssue

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

package org.apache.derbyBuild;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An issue from JIRA.
 */
class JiraIssue {
    public static final long NO_RELEASE_NOTE = -1;
    public static final long MISSING_RELEASE_NOTE = -2;
    private static final String ATTACHMENT_BASE =
        "https://issues.apache.org/jira/secure/attachment/";
    private static final String ATTACHMENT_NAME = "releaseNote.html";

    // States for parsing source from the Derby JIRA SOAP client.
    private static final int STATE_ADD_RESET = -1;
    private static final int STATE_ADD_KEY = 0;
    private static final int STATE_ADD_SUMMARY = 1;
    private static final int STATE_ADD_FIXVERSIONS = 2;
    private static final int STATE_ADD_RELEASENOTE = 3;

    // JIRA issue information
    private String key;
    private String title;
    private long releaseNoteAttachmentID = NO_RELEASE_NOTE;
    private List fixVersions;

    public JiraIssue(String key, String title, List fixVersions,
                     long releaseNoteAttachmentID) {
        this.key = key;
        this.title = title;
        this.fixVersions = fixVersions;
        this.releaseNoteAttachmentID = releaseNoteAttachmentID;
    }

    /**
     * Factory method which extracts a list of JiraIssue objects from a data
     * file generated by the Derby JIRA SOAP client.
     *
     * @param source the source file (generated by the Derby JIRA SOAP client)
     * @return A List of {@code JiraIssue} objects.
     * @throws Exception if something goes wrong
     */
    public static List createJiraIssueList(String source)
            throws IOException {
        ArrayList<JiraIssue> jiraIssues = new ArrayList<JiraIssue>();
//IC see: https://issues.apache.org/jira/browse/DERBY-4893

        BufferedReader in = new BufferedReader(new FileReader(source));
        String line;
        System.out.println("--- Creating Jira issue list");
        while ((line = in.readLine()) != null && line.startsWith("//")) {
            System.out.println(line);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-4893
        ArrayList<String> comments = new ArrayList<String>();
        int state = STATE_ADD_KEY;
        String key = null;
        String summary = null;
        String[] fixVersions = null;
        long attachmentId = NO_RELEASE_NOTE;
        do {
            if (line.startsWith("//")) {
                comments.add(line.trim());
                continue;
            }
            if (line.startsWith("---")) {
                continue;
            }

            if (state == STATE_ADD_KEY) {
                key = line.trim();
                if (!key.startsWith("DERBY-")) {
                    throw new IllegalStateException(
                            "invalid JIRA key for Derby: " + key);
                }
                key = key.split("-")[1];
                // Sanity check
                Integer.parseInt(key);
            } else if (state == STATE_ADD_SUMMARY) {
                summary = line.trim();
            } else if (state == STATE_ADD_FIXVERSIONS) {
                line = line.trim();
                fixVersions = line.split(",");
            } else if (state == STATE_ADD_RELEASENOTE) {
                line = line.trim();
                if (line.equals("null")) {
                    attachmentId = NO_RELEASE_NOTE;
                } else  if (line.equals("missing")) {
                    attachmentId = MISSING_RELEASE_NOTE;
                } else {
                    attachmentId = Long.parseLong(line);
                }
                // We now have all the information we need.
                jiraIssues.add(new JiraIssue(key, summary,
                        Arrays.asList(fixVersions), attachmentId));
                state = STATE_ADD_RESET;
            }
            state++;
        } while ((line = in.readLine()) != null);
        if (state != STATE_ADD_KEY) {
            throw new IllegalStateException("illegal state, check source " +
                    "file for correctness (state=" + state + ")");
        }
        // Print the last few comments for information (by convention).
        int size = comments.size();
        if (size > 2) {
            System.out.println(comments.get(size -3));
            System.out.println(comments.get(size -2));
            System.out.println(comments.get(size -1));
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4857
        return (releaseNoteAttachmentID != NO_RELEASE_NOTE &&
                releaseNoteAttachmentID != MISSING_RELEASE_NOTE);
    }

    /**
     * @return true iff this issue is missing a release note
     */
    public boolean hasMissingReleaseNote() {
        return (releaseNoteAttachmentID == MISSING_RELEASE_NOTE);
    }

    /**
     * Predicate for finding out if issue has a given fixVersion.
     * @param version to test
     * @return true iff issue has version as fixVersion
     */
    public boolean isFixedIn(String version) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4857
        return fixVersions.contains(version);
    }

    /**
     * @return URL for this Jira issue
     */
    public String getJiraAddress() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4593
        return "https://issues.apache.org/jira/browse/DERBY-" + key;
    }

    /**
     * @return Full URL to the latest release note
     */
    public String getReleaseNoteAddress() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4857
        return ATTACHMENT_BASE +
                releaseNoteAttachmentID + "/" + ATTACHMENT_NAME;
    }
}
