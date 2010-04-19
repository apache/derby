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
import java.util.*;


/**
 *
 * An issue from a JIRA report.
 *
 */
class JiraIssue {
    private static final long NO_RELEASE_NOTE = -1;
    private static final String JIRA_ITEM = "h3";
    private static final String JIRA_ID = "id";
    private static final String JIRA_NAME = "name";
    private static final String JIRA_TITLE = "a";
    private static final String JIRA_KEY = "key";
    private static final String JIRA_ATTACHMENT = "attachment";
    private static final String JIRA_FIXVERSION = "fixVersion";
    private static final String RELEASE_NOTE_NAME = "releaseNote.html";

    private String key;
    private String title;
    private long releaseNoteAttachmentID = NO_RELEASE_NOTE;
    private HashSet fixVersionSet = new HashSet();

    /**
     * Create an object instance from an XML document Element. The Element given
     * as argument is assumed to be an 'item' sub-tree from the XML file
     * representation of a Jira filter/query.
     * @param itemElement the 'item' subtree representing a Jira issue
     * @throws java.lang.Exception
     */
    public JiraIssue(Element itemElement) throws Exception {
        ElementFacade ef = new ElementFacade(itemElement);
        title = ef.getTextByTagName(JIRA_TITLE);
        //key = ef.getTextByTagName(JIRA_KEY);
        key = parseKey( title );

        releaseNoteAttachmentID = getReleaseNoteAttachmentID( key, itemElement );

        //
        // A JIRA title has the following form:
        //
        //  "[DERBY-2598] new upgrade  test failures after change 528033"
        //
        // We strip off the leading JIRA id because that information already
        // lives in the key.
        //
        title = title.substring(title.indexOf(']') + 2);

        for (Iterator i = ef.getTextListByTagName(JIRA_FIXVERSION).iterator();
        i.hasNext();) {
            fixVersionSet.add(i.next());
        }
    }

    /**
     * Look up the attachment id for the release note attached to
     * an issue.
     */
    private long getReleaseNoteAttachmentID
        ( String key, Element itemElement )
        throws Exception
    {
        long result = NO_RELEASE_NOTE;

        //
        // The following code used to work before the time of Derby 10.6.
        // With that release, the list of attachments stopped appearing in
        // the xml reports.
        //
        //        NodeList attachmentsList =
        //                itemElement.getElementsByTagName(JIRA_ATTACHMENT);
        //
        //        for (int i = 0; i < attachmentsList.getLength(); i++) {
        //            Element attachment = (Element) attachmentsList.item(i);
        //            String name = attachment.getAttribute(JIRA_NAME);
        //            if (RELEASE_NOTE_NAME.equals(name)) {
        //                result =
        //                        Math.max(result,
        //                        Long.parseLong(attachment.getAttribute(JIRA_ID)));
        //            }
        //        }

        //
        // As a consequence, we now hardcode the attachment ids.
        // The attachment id is in the link of the latest release note
        // attached to the issue.
        //
        if ( key.equals( "DERBY-4602" ) ) { result = 12440335L; }
        else if ( key.equals( "DERBY-4483" ) ) { result = 12439775L; }
        else if ( key.equals( "DERBY-4432" ) ) { result = 12424709L; }
        else if ( key.equals( "DERBY-4380" ) ) { result = 12434514L; }
        else if ( key.equals( "DERBY-4355" ) ) { result = 12419298L; }
        else if ( key.equals( "DERBY-4312" ) ) { result = 12414219L; }
        else if ( key.equals( "DERBY-4230" ) ) { result = 12409466L; }
        else if ( key.equals( "DERBY-4191" ) ) { result = 12430699L; }
        else if ( key.equals( "DERBY-3991" ) ) { result = 12409798L; }
        else if ( key.equals( "DERBY-3844" ) ) { result = 12436979L; }
        else if ( key.equals( "DERBY-2769" ) ) { result = 12418474L; }
        
        return result;
    }

    /**
     * Extract the JIRA key (DERBY-XXXX) from the raw title.
     * A JIRA  raw title has the following form:
     *
     *  "[DERBY-2598] new upgrade  test failures after change 528033"
     */
    private String parseKey( String rawTitle ) throws Exception
    {
        String result = rawTitle.substring(1, title.indexOf(']') );

        return result;
    }

    /**
     * Factory method which extracts a list of JiraIssue objects from a Jira
     * report (supplied as an XML Document). Issues with a fixVersion contained
     * in the exclude list will be omitted from the list.
     * @param report the Jira report to extract issues from (as a Document object)
     * @param excludeReleaseIDList list of fixVersions that disqualifies an issue
     * @return a List of JiraIssue objects
     * @throws java.lang.Exception
     */
    public static List createJiraIssueList(Document report,
            List excludeReleaseIDList) throws Exception {
        Element reportRoot = report.getDocumentElement();
        NodeList itemList = reportRoot.getElementsByTagName(JIRA_ITEM);
        int count = itemList.getLength();
        ArrayList jiraIssues = new ArrayList();

        boolean skip;
        for (int i = 0; i < count; i++) {
            skip=false;
            JiraIssue candidate = new JiraIssue((Element) itemList.item(i));
            for (Iterator ex = excludeReleaseIDList.iterator(); ex.hasNext();) {
                String rid = (String) ex.next();
                if (candidate.isFixedIn(rid)) {
                    System.out.println("Already fixed: "+candidate.getKey()+
                            " (in "+rid+")");
                    skip=true;
                    continue;
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
     * @return the issue's key (jira number DERBY-xxx)
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
        return (releaseNoteAttachmentID > NO_RELEASE_NOTE);
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
        return "https://issues.apache.org/jira/browse/" + key;
    }

    /**
     * @return Full URL to the latest release note
     */
    public String getReleaseNoteAddress() {
        return "https://issues.apache.org/jira/secure/attachment/" +
                releaseNoteAttachmentID + "/releaseNote.html";
    }
}


