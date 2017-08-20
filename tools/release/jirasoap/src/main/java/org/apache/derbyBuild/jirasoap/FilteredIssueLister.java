/*

   Derby - Class org.apache.derbyBuild.jirasoap.FilteredIssueLister

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

package org.apache.derbyBuild.jirasoap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import javax.xml.rpc.ServiceException;

/**
 * Client talking to the Apache JIRA instance to retrieve and derive information
 * required to generate releases notes for a Derby release.
 * <p>
 * The purpose of this client is to carry out some of the tasks a release
 * manager has to do when generating the release notes.
 */
public class FilteredIssueLister {

    /** System property for specifying the ancestor cutoff threshold. */
    private static final String ANCESTOR_CUTOFF_PROP = "ancestorCutoff";
    private static final String DEFAULT_ANCESTOR_CUTOFF = "10.3.3.0";
    /** System property for turning on reporting of disqualified issues. */
    private static final String REPORT_DISQUALIFICATIONS_PROP =
            "reportDisqualifications";
    /** Help text for command line invocation. */
    private static final String USAGE =
"-- Apache Derby JIRA SOAP client --\n\n" +
"The main purpose of this client is to fetch the required information from\n" +
"JIRA, such that the release manager can generate the release notes.\n" +
"This tool does not generate the release notes, but provides some of the\n" +
"information for the tool doing that.\n\n" +
"Primary usage:\n" +
"  o <USER> <PASSWORD> <VERSION> <FILTERID> <DESTINATION_FILE> [ANCESTRY]\n" +
"    generates a list of fixed issues for the specified release version,\n" +
"    which can be processed by the ReleaseNotesGenerator tool.\n" +
"    Note that the release ancestry should be verified. The ancestry is\n" +
"    printed to standard out and into the generated file. You can also\n" +
"    check up-front by running the 'ancestors' mode (see 'Secondary usage').\n"+
"    If incorrect, re-run and specify the release ancestry manually.\n" +
"\n" +
"Secondary usage:\n" +
"  o <USER> <PASSWORD> ancestors <VERSION>\n" +
"    prints the ancestors of the specified version\n"+
"    (only released versions can be ancestors)\n" +
"  o <USER> <PASSWORD> releases\n" +
"    prints all Derby releases, sorted by release date\n"+
"\n" +
"Argument values:\n" +
"  o VERSION\n" +
"      Derby version string, i.e. 10.6.2.1\n" +
"  o FILTERID\n" +
"      JIRA id, only digits allowed.\n" +
"      If '0' (zero), a JQL query will be generated instead of using an\n" +
"      existing (manually created) JIRA filter.\n" +
"  o ANCESTRY\n" +
"      if necessary, the release ancestry can be overridden by specifying\n " +
"      the ancestors by version manually. Valid values:\n" +
"        - derive (the default)\n" +
"        - ignore (don't filter issues)\n" +
"        - VERSION[,VERSION]* (manually specified)\n" +
"\n" +
"System properties:\n" +
"  o " + ANCESTOR_CUTOFF_PROP + "\n" +
"    modify the value of the cutoff version\n" +
"    (default is 10.3.3.0)\n" +
"  o " + REPORT_DISQUALIFICATIONS_PROP + "\n" +
"    if set to true, disqualified issues will be printed to standard out\n" +
"    (default is false)\n" +
"\n";

    /** Apache Derby project identifier in JIRA. */
    private static final String DERBY_PROJECT = "DERBY";
    /** Custom Derby flag used in JIRA. */
    private static final String FIELD_RELEASE_NOTE = "Release Note Needed";
    /**
     * Name of the file containing release notes in JIRA. This is by
     * Apache Derby community convention.
     */
    private static final String RELEASE_NOTE_NAME = "releaseNote.html";
    /** Constant used to choose using JQL over an existing filter. */
    private static final int GENERATE_JQL = 0;

    private PrintStream logOut = new PrintStream(System.out);
    private JiraSoapService jiraSoapService;
    /** JIRA user to log in as. */
    private String user;
    /** JIRA authentication token. */
    private String auth;
    /** Cached version objects. */
    private DerbyVersion[] allVersions;
    /** The point at which we stop listing ancestors for a release. */
    private final DerbyVersion ancestorCutoff;
    /** Tells if disqualified issues should be reported. */
    private final boolean reportDisqualifiedIssues;
    /** Tells if the release ancestry has been overriden by the user. */
    private boolean ancestryOverridden;

    /**
     * Creates a new JIRA client.
     *
     * @param username JIRA user to log in as
     * @param cred JIRA password
     * @throws RemoteException if the login fails for some unexpected reason, or
     *      if fetching the version list fails
     * @throws ServiceException if obtaining the JIRA service fails
     * @throws RuntimeException if the JIRA credentials are invalid
     */
    public FilteredIssueLister(String username, String cred)
            throws RemoteException, ServiceException {
        JiraSoapServiceService jiraSoapServiceLocator =
                new JiraSoapServiceServiceLocator();
        log("getting JIRA service");
        jiraSoapService = jiraSoapServiceLocator.getJirasoapserviceV2();
        log("logging in as '" + username + "'");
        try {
            auth = jiraSoapService.login(username, cred);
        } catch (RemoteAuthenticationException rae) {
            // Give a friendlier error message for this case.
            throw new RuntimeException(
                    "JIRA login failed. Cause:\n" + rae.toString());
        }
        user = username;
        log("fetching versions");
        RemoteVersion[] jiraVer =
                jiraSoapService.getVersions(auth, DERBY_PROJECT);
        allVersions = new DerbyVersion[jiraVer.length];
        for (int i=0; i < jiraVer.length; i++) {
            allVersions[i] = new DerbyVersion(jiraVer[i]);
        }
        // Give a better error message if user-specified cutoff value is bad.
        try {
            ancestorCutoff = getVersion(System.getProperty(
                ANCESTOR_CUTOFF_PROP, DEFAULT_ANCESTOR_CUTOFF));
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(
                    "invalid ancestor cutoff version", iae);
        }
        reportDisqualifiedIssues =
                Boolean.getBoolean(REPORT_DISQUALIFICATIONS_PROP);

        printReleases();
    }

    /** Constructor for testing, where the Derby versions can be specified
     * manually.
     *
     * @param versions format is {{"major.minor.fixpack.point", "YYYY-MM-DD"}}
     */
    FilteredIssueLister(String username, String cred, String[][] versions)
            throws RemoteException, ServiceException {
        JiraSoapServiceService jiraSoapServiceLocator =
                new JiraSoapServiceServiceLocator();
        log("getting JIRA service");
        jiraSoapService = jiraSoapServiceLocator.getJirasoapserviceV2();
        log("logging in as '" + username + "'");
        try {
            auth = jiraSoapService.login(username, cred);
        } catch (RemoteAuthenticationException rae) {
            // Give a friendlier error message for this case.
            throw new RuntimeException(
                    "JIRA login failed. Cause:\n" + rae.toString());
        }
        user = username;
        allVersions = new DerbyVersion[versions.length];
        // Expected format: release version, release date (YYYY-MM-DD, or null)
        for (int i=0; i < versions.length; i++) {
            allVersions[i] = new DerbyVersion(
                    versions[i][0], versions[i][1] == null
                                            ? DerbyVersion.NOT_RELEASED
                                            : parseDate(versions[i][1]));
        }
        // Give a better error message if user-specified cutoff value is bad.
        try {
            ancestorCutoff = getVersion(System.getProperty(
                ANCESTOR_CUTOFF_PROP, DEFAULT_ANCESTOR_CUTOFF));
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(
                    "invalid ancestor cutoff version", iae);
        }
        reportDisqualifiedIssues =
                Boolean.getBoolean(REPORT_DISQUALIFICATIONS_PROP);
    }

    /**
     * Generates a list of Derby JIRA issues addressed by the target release
     * version and writes these to a file for further processing.
     * <p>
     * <b>Important:</b> Although some sanity checks are performed, it is
     * crucial that the manually created filter is set up correctly. If the
     * filter misses issues addressed by the release, they will not make it
     * into the generated release notes. Short description:
     * <ul>
     *  <li>include bugs and improvements</li>
     *  <li>include issues resolved as Fixed</li>
     *  <li>include issues marked as Resolved or Closed</li>
     *  <li>include all release candidates in the fix version field
     *      (if not already released)</li>
     * </ul>
     *
     * @param version the target release version
     * @param filterId the JIRA filter id
     * @param destFile output file for the issue report
    * @throws IOException if writing to the output file fails
     */
    public void prepareReleaseNotes(String version, long filterId,
                                    String destFile,
                                    String[] ancestorVersions)
            throws IOException {
        DerbyVersion releaseVersion = getVersion(version);
        DerbyVersion[] ancestors = null;
        if (ancestorVersions == null) {
            // Obtain a list of ancestors, used to disqualify JIRA issues
            // matched by the JIRA filter.
            ancestryOverridden = false;
            ancestors = getAncestors(releaseVersion);
        } else {
            ancestryOverridden = true;
            ancestors = new DerbyVersion[ancestorVersions.length];
            for (int i=0; i < ancestorVersions.length; i++) {
                ancestors[i] = getVersion(ancestorVersions[i]);
            }
        }
        persistFilterResult(releaseVersion, filterId, destFile, ancestors);
    }

    /**
     * Prints the list of ancestors, i.e. earlier releases down the release
     * chain, for specified Derby version.
     * <p>
     * Note that only released versions are considered to be ancestors.
     *
     * @param parentVersion the version to start at (released or not)
     */
    public void printAncestors(String parentVersion) {
        DerbyVersion parent = getVersion(parentVersion);
        if (parent.compareTo(ancestorCutoff) < 0) {
            throw new IllegalArgumentException(
                    "specified version " + parentVersion +
                    " is less than the ancestor cut-off version: " +
                    ancestorCutoff.getVersion());
        }
        DerbyVersion[] ancestors = getAncestors(parent);
        System.out.println("--- Ancestors for version " + parentVersion + " (" +
                (parent.isReleased() ? "released)" : "unreleased)"));
        for (int i=0; i < ancestors.length; i++) {
            System.out.println(ancestors[i]);
        }
        // Special case when there is no ancestor.
        if (ancestors.length == 0) {
            System.out.println("<no ancestors found in JIRA>");
        }
        System.out.println("(cutoff=" + ancestorCutoff.getVersion() + ")");
    }

    /**
     * Prints all Derby releases.
     */
    public void printReleases() {
        ArrayList releases = new ArrayList();
        for (int i=0; i < allVersions.length; i++) {
            DerbyVersion dv = allVersions[i];
            if (dv.isReleased()) {
                releases.add(dv);
            }
            else
            {
                System.out.println(dv.toString() + " was NOT released.");
            }
        }
        Collections.sort(releases, new Comparator() {

            public int compare(Object o1, Object o2) {
                Long release1 = new Long(
                        ((DerbyVersion)o1).getReleaseDateMillis());
                Long release2 = new Long(
                        ((DerbyVersion)o2).getReleaseDateMillis());
                return release1.compareTo(release2);
            }
        });
        Collections.reverse(releases);
        System.out.println("--- Derby releases");
        Iterator relIter = releases.iterator();
        while (relIter.hasNext()) {
            System.out.println(relIter.next());
        }
    }

    /**
     * Releases resources associated with the client.
     *
     * @throws RemoteException if logging out fails
     */
    public void destroy()
            throws RemoteException {
        jiraSoapService.logout(auth);
        auth = null;
        jiraSoapService = null;
        allVersions = null;
    }

    /**
     * Executes a JIRA filter and writes the matching JIRA issues to file.
     *
     * @param targetVersion targetted release version
     * @param filterId JIRA filter id used to obtain the relevant issues
     * @param destFile destination file
     * @param excludeFixVersions exclude issues which have been fixed on one
     *      of the exclude versions (also called ancestry chain)
     * @return The number of filters written to the destination file.
     * @throws IOException if writing to the output file fails
     */
    private int persistFilterResult(DerbyVersion targetVersion, long filterId,
                                   String destFile,
                                   DerbyVersion[] excludeFixVersions)
            throws IOException {
        // Extract the version string from the versions to exclude.
        int size = excludeFixVersions == null ? 0 : excludeFixVersions.length;
        final ArrayList excludeList = new ArrayList(size);
        for (int i=0; i < size; i++) {
            excludeList.add(excludeFixVersions[i].getVersion());
        }

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(destFile), "UTF-8"));
        out.write("// Produced on " + new java.util.Date().toString());
        out.newLine();
        out.write("// Release version: " + targetVersion.getVersion());
        out.newLine();
        out.write("// Previous release: " + excludeFixVersions[0].getVersion());
        out.newLine();
        out.write("// " + (ancestryOverridden ? "Overridden" : "Derived"));
        out.write(" ancestry chain");
        out.newLine();
        for (int i=0; i < excludeFixVersions.length; i++) {
            out.write("//   " + excludeFixVersions[i].getVersion());
            out.newLine();
        }
        RemoteIssue[] issues = null;
        if (filterId == GENERATE_JQL) {
            issues = execJiraJQLQuery( out, auth, targetVersion, excludeFixVersions );
        } else {
            issues = execJiraFilterQuery(out, auth, filterId);
        }
        log("persisting issues (" + issues.length + " candidate issues)");
        out.write("// Candidate issue count: " + issues.length);
        out.newLine();
        int count = 0;
        int issuesWithReleaseNote = 0;
        // Adhere to this very simple format.
        // --- (separator)
        // DERBY-XXXX
        // SUMMARY
        // FIX_VERSION[,FIX_VERSION]*
        // RELEASENOTE_ATTACHMENT_ID|null|missing
        // ("null" if not existing, "missing" if missing)
        for (int i=0; i < issues.length; i++) {
            RemoteIssue ri = issues[i];
            // This will throw exception if the target version isn't in the list
            // of fix versions, and return null if the issue has been dis-
            // qualified because it has already been fixed in an ancestor.
            String fixVersions = stringifyAndCheckFixVersions(ri.getKey(),
                    ri.getFixVersions(), excludeList, targetVersion);
            if (fixVersions == null) {
                continue;
            }
            // Persist the issue (human readable/editable).
            out.write("---");
            out.newLine();
            // key
            out.write(ri.getKey());
            out.newLine();
            // summary
            out.write(ri.getSummary());
            out.newLine();
            // fix versions
            out.write(fixVersions);
            out.newLine();
            // release note flag and affects existing applications flag
            RemoteCustomFieldValue[] fieldValues = ri.getCustomFieldValues();
            boolean releaseNoteNeeded = hasCustomField(
                    FIELD_RELEASE_NOTE, fieldValues);
            // release note attachemnt id
            if (hasReleaseNote(ri)) {
                issuesWithReleaseNote++;
                long latest = 0;
                RemoteAttachment[] attachments =
                        jiraSoapService.getAttachmentsFromIssue(
                                                            auth, ri.getKey());
                // Find the latest attachment, just use the one with the
                // highest id.
                for (int a=0; a < attachments.length; a++) {
                    String name = attachments[a].getFilename();
                    long id = Long.parseLong(attachments[a].getId());
                    if (name.equals(RELEASE_NOTE_NAME)) {
                        latest = Math.max(latest, id);
                    }
                }
                out.write(Long.toString(latest));
            } else {
                if (releaseNoteNeeded) {
                    out.write("missing");
                } else {
                    out.write("null");
                }
            }
            out.newLine();
            count++;
        }

        // Write some more status
        out.write("// Issues written: " + count);
        out.newLine();
        out.write("// Issues disqualified: ");
        if (excludeFixVersions == null) {
            out.write("disqualification disabled");
        } else {
            out.write(Integer.toString(issues.length - count));
        }
        out.newLine();
        out.write("// Issues with release note: " + issuesWithReleaseNote);
        out.newLine();
        out.close();

        // Log some basic information
        log("wrote " + count + " issues, " + issuesWithReleaseNote +
                " with release notes, " + (issues.length - count) +
                " issues disqualified");
        log("dump file: " + new File(destFile).getCanonicalPath());
        return count;
    }

    /**
     * Returns the version object for the specified Derby version.
     *
     * @param version target version
     * @return A version object.
     * @throws IllegalArgumentException if the specified version doesn't exist
     */
    private DerbyVersion getVersion(String version) {
        DerbyVersion match = null;
        for (int i=0; i < allVersions.length; i++) {
            if (version.equals(allVersions[i].getVersion())) {
                match = allVersions[i];
            }
        }
        if (match == null) {
            throw new IllegalArgumentException(
                    "version '" + version + "' doesn't exist");
        }
        return  match;
    }

    /**
     * Computes the ancestors for the specified version.
     *
     * @param parent the initial parent version
     * @return A list of ancestors for the specified version.
     */
    private DerbyVersion[] getAncestors(DerbyVersion parent) {
        ArrayList ancestors = new ArrayList();
        DerbyVersion[] dv = getSortedAndFilteredReleases(parent);
        while (dv.length > 1 && dv[0].compareTo(ancestorCutoff) >= 0) {
            dv = getSortedAndFilteredReleases(dv[1]);
            ancestors.add(dv[0]);
        }
        dv = new DerbyVersion[ancestors.size()];
        ancestors.toArray(dv);
        return dv;
    }

    /**
     * Returns a list of sorted and filtered Derby releases.
     * <p>
     * If a target release is specified, all later releases will be filtered
     * out. The filtering happens at two levels:
     * <ul> <li>version number (i.e. 10.6.2.1 > 10.5.1.0)</li>
     *      <li>release date</li>
     * </ul>
     * <p>
     * The target version will always be found at index zero.
     * <p>
     * Not specifying a target version will return all Derby releases sorted by
     * version number.
     *
     * @param target target version to start sorting/filtering at (may be null)
     * @return A list of previous releases, sorted by version number
     *      (highest first).
     */
    private DerbyVersion[] getSortedAndFilteredReleases(DerbyVersion target) {
        // Add versions to the list, filtering as specified.
        ArrayList tmp = new ArrayList();
        for (int i=0; i < allVersions.length; i++) {
            DerbyVersion dv = allVersions[i];
            // Skip versions that haven't been released.
            if (!dv.isReleased() && !dv.equals(target)) {
                continue;
            }
            if (target != null) {
                if (dv.compareTo(target) > 0) {
                    continue;
                }
                if (target.isReleased() && dv.getReleaseDateMillis() >
                        target.getReleaseDateMillis()) {
                    continue;
                }
            }
            tmp.add(dv);
        }
        // Sort, then reverse to get newest version at index zero.
        Collections.sort(tmp);
        Collections.reverse(tmp);
        DerbyVersion[] result = new DerbyVersion[tmp.size()];
        tmp.toArray(result);
        return result;
    }

    /**
     * Fetches JIRA issues matched by a predefined filter search.
     * <p>
     * The filter must be created manually and before the release notes are
     * generated.
     *
     * @param out output stream
     * @param auth JIRA authententication token
     * @param filterId JIRA filter id (digits only)
     * @return A list of matching issues.
     * @throws IOException if something goes wrong
     */
    private RemoteIssue[] execJiraFilterQuery(BufferedWriter out, String auth,
                                              long filterId)
            throws IOException {
        out.write("// Filter id: " + filterId + ", user id " + user);
        out.newLine();
        log("fetching issues from filter (id = " + filterId + ")");
        try {
             return jiraSoapService.getIssuesFromFilterWithLimit(
                auth, Long.toString(filterId), 0, 1000);
        } catch (org.apache.derbyBuild.jirasoap.RemoteException re) {
            throw new IllegalArgumentException(
                    "invalid filter id: " + filterId +
                    " (" + re.getFaultString() + ")");
        }
    }

    /**
     * Fetches JIRA issues matching a generated JQL (Jira Query Language)
     * search.
     *
     * @param out output stream
     * @param auth JIRA authententication token
     * @param targetVersion the target release version
     * @return A list of matching issues.
     * @throws IOException if something goes wrong
     */
    private RemoteIssue[] execJiraJQLQuery
        (
         BufferedWriter out,
         String auth,
         DerbyVersion targetVersion,
         DerbyVersion[] excludeFixVersions
         )
        throws IOException
    {
        // Here we have two scenarions:
        // a) A single target version number - the release has already been
        //    made, or there is only one release candidate.
        // b) Multiple target version numbers - there are multiple release
        //    candidates, and we want to include issues fixed in all of them.
        // To simplify code, just build an IN-clause for all scenarios.

        // Identify versions.
        List rcs = new ArrayList();
        for (int i=0; i < allVersions.length; i++) {
            DerbyVersion ver = allVersions[i];
            if (targetVersion.isSameFixPack(ver) &&
                    ver.compareTo(targetVersion) < 1) {
                rcs.add(ver);
            }
        }
        Collections.sort(rcs);
        Collections.reverse(rcs);
        Iterator rcIter = rcs.iterator();

        // Build JQL query.
        String jql = "project = DERBY AND resolution = fixed AND component not in ( Test ) AND fixversion ";
        StringBuffer sb = new StringBuffer("in (");
        while (rcIter.hasNext()) {
            DerbyVersion rc = (DerbyVersion)rcIter.next();
            sb.append(rc.getQuotedVersion());
            sb.append(", ");
        }
        sb.deleteCharAt(sb.length() -1).deleteCharAt(sb.length() -1);
        sb.append(')');
        jql += sb.toString();

        StringBuilder   notIn = new StringBuilder();
        notIn.append( " and fixversion not in ( " );
        for ( int i = 0; i < excludeFixVersions.length; i++ )
        {
            if ( i > 0 ) { notIn.append( ", " ); }
            DerbyVersion    exclusion = excludeFixVersions[ i ];
            notIn.append( exclusion.getQuotedVersion() );
        }
        notIn.append( " )" );
        jql += notIn.toString();

        // Execute the query.
        out.write("// JQL query: " + jql);
        out.newLine();
        log("executing JQL query: " + jql);
        try {
             return jiraSoapService.getIssuesFromJqlSearch(auth, jql, 1000);
        } catch (org.apache.derbyBuild.jirasoap.RemoteException re) {
            throw new IllegalArgumentException(
                    "JQL query '" + jql + "' failed (" +
                    re.getFaultString() + ")");
        }
    }

    /**
     * Interface for running from the command line.
     *
     * @param args see USAGE constant, or invoke with zero arguments
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args)
            throws Exception {
        // Always require JIRA user name and password.
        if (args.length > 2) {
            FilteredIssueLister client =
                    new FilteredIssueLister(args[0], args[1]);
            try {
                // PRINT ANCESTORS
                if (args[2].equalsIgnoreCase("ancestors")) {
                    if (args.length == 2) {
                        System.err.println("Missing version argument.");
                        System.exit(1);
                    }
                    client.printAncestors(args[3]);
                // PRINT VERSIONS
                } else if(args[2].equalsIgnoreCase("releases")) {
                    client.printReleases();
                // RELEASE NOTES PREPARATION / GENERATE ISSUE LIST
                } else {
                    if (args.length < 4) {
                        System.err.println("Missing argument(s).");
                        System.exit(1);
                    }
                    String[] overriddenAncestry = null;
                    // This is the default release target
                    // Args: user password version filterId dest [remove]
                    if (args.length > 5) {
                        overriddenAncestry = args[5].split(",");
                        if (args[0].equalsIgnoreCase("ignore")) {
                            overriddenAncestry = new String[0];
                        } else if(args[0].equalsIgnoreCase("derive")) {
                            overriddenAncestry = null;
                        }
                    }
                    client.prepareReleaseNotes(args[2], Long.parseLong(args[3]),
                            args[4], overriddenAncestry);
                }
            } finally {
                client.destroy();
            }
        } else {
            System.err.println(USAGE);
        }
    }

    /** Logs status/convenience messages. */
    private void log(String msg) {
        if (logOut != null) {
            logOut.println(msg);
        }
    }

    /**
     * Converts an array of fix versions into a string representation.
     *
     * @param fixVersions fix versions for a JIRA issue
     * @return A string describing all the fix versions.
     */
    private String stringifyAndCheckFixVersions(String issueKey,
            RemoteVersion[] fixVersions, List excludeVersions,
            DerbyVersion releaseVersion) {
        if (fixVersions.length == 0) {
            throw new IllegalStateException(issueKey + " has no fix version");
        }
        boolean disqualified = false;
        boolean sanityCheckPassed = false;
        StringBuffer sb = new StringBuffer();
        StringBuffer fixedIn = new StringBuffer(); // only used for reporting
        for (int i=0; i < fixVersions.length; i++) {
            String fv = fixVersions[i].getName();
            if (!sanityCheckPassed) {
                DerbyVersion dv = new DerbyVersion(fixVersions[i]);
                if (dv.equals(releaseVersion) ||
                        dv.isSameFixPack(releaseVersion)) {
                    sanityCheckPassed = true;
                }
            }
            if (excludeVersions.contains(fv)) {
                disqualified = true;
                fixedIn.append(fv).append(',');
                // Could return null here, but then the sanity-check may be
                // bypassed.
            }
            sb.append(fv).append(',');
        }
        sb.deleteCharAt(sb.length() -1);

        // Sanity check to catch if an invalid JIRA filter is being used.
        if (!sanityCheckPassed) {
            throw new IllegalStateException(issueKey + " not marked as fixed " +
                    "in the target release version" +
                    releaseVersion.getVersion() + ", nor in any of the " +
                    "versions with the same fixpack. Invalid JIRA filter?");
        }
        if (disqualified) {
            if (reportDisqualifiedIssues) {
                fixedIn.deleteCharAt(fixedIn.length() -1);
                System.out.println(issueKey + " disqualified, " +
                        "already fixed in " + fixedIn.toString());
            }
            return null;
        } else {
            return sb.toString();
        }
    }

    /**
     * Tells if the issue has a release note.
     *
     * @param issue JIRA issue
     * @return {@code true} if the issue has a release note attached.
     */
    private static boolean hasReleaseNote(RemoteIssue issue) {
        String[] aNames = issue.getAttachmentNames();
        for (int i=0; i < aNames.length; i++) {
            if (aNames[i].equals(RELEASE_NOTE_NAME)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tells if the issue has the specified custom field value set.
     *
     * @param fieldValue the value to look for
     * @param values the field values
     * @return {@code true} if the custom field value was found,
     *      {@code false} otherwise.
     */
    private static boolean hasCustomField(String fieldName,
                                          RemoteCustomFieldValue[] values) {
        // The API is a but awkward when it comes to fields, but we can do our
        // thing by looking at the custom field values only.
        for (int i=0; i < values.length; i++) {
            String[] v = values[i].getValues();
            for (int j=0; j < v.length; j++) {
                if (fieldName.equals(v[j])) {
                    return true;
                }
            }
        }
        return false;
    }

    private static final Calendar PARSECAL = GregorianCalendar.getInstance();
    private static synchronized long parseDate(String date) {
        String[] comp = date.split("-");
        int year = Integer.parseInt(comp[0]);
        int month = Integer.parseInt(comp[1]) -1;
        int day = Integer.parseInt(comp[2]);
        PARSECAL.set(year, month, day, 0, 0, 0);
        return PARSECAL.getTimeInMillis();
    }
}
