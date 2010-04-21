/*  Derby - Class org.apache.derbyBuild.ChangesFileGenerator

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

import java.util.*;
import org.w3c.dom.*;
import org.apache.tools.ant.BuildException;

/**
 * <p>
 * This tool generates the Changes file for a Derby release. See the USAGE
 * constant for details on how to run this tool standalone. It is recommended
 * that you freshly regenerate your BUG_LIST just before you run this tool.
 * </p>
 *
 * <p>
 * The tool is designed to be run from Derby's ant build scripts. To run under
 * ant, do the following:
 * </p>
 *
 * <ul>
 * <li>Define the "relnotes.src.reports" variable in your ant.properties or
 * on the command line (-D option). This
 * variable points at the directory which holds your xml JIRA reports.</li>
 * <li>Put your xml JIRA reports in that directory. They should have the
 * following names:
 *  <ul>
 *  <li>changes.xml - This is the list of issues fixed by the release.</li>
 *  </ul>
 * </li>
 * <li>You can use java org.apache.derbyBuild.JiraConnector to create this
 * file, or use the "XML" link found above the search results when using a Jira
 * filter. (You need to right-click the XML link and choose "Save link as" to
 * prevent your browser from interpreting it as an RSS feed).
 * </li>
 * <li>You also need to update trunk/releaseSummary.xml so that the values in
 * it is correct for this relase.</li>
 * <li>For greater control over the process you can override all input and
 * output files using properties. See tools/release/build.xml for the details.</li>
 * <li>Then cd to tools/release and run ant thusly: "ant genchanges"</li>
 * </ul>
 */
public class ChangesFileGenerator extends GeneratorBase {

    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    private static  final   String  USAGE =
        "Usage:\n" +
        "\n" +
        "  java org.apache.derbyBuild.ChangesFileGenerator SUMMARY BUG_LIST OUTPUT\n" +
        "\n" +
        "    where\n" +
        "                  BUG_LIST An xml JIRA report of issues addressed by this release.\n" +
        "                  OUTPUT  The output file to generate, typically CHANGES.html.\n" +
        "\n" +
        "The ChangesFileGenerator attempts to connect to issues.apache.org in\n" +
        "order to find the individual JIRAs. Before running this program, make sure that you can\n" +
        "ping issues.apache.org.\n" +
        "\n" +
        "The ChangesFileGenerator assumes that the two JIRA reports contain\n" +
        "key, title, and attachments elements for each Derby issue.\n" +
        "For this reason, it is recommended that you freshly generate BUG_LIST\n" +
        "just before you run this tool.\n";

    // major sections
    private static  final   String  BUG_FIXES_SECTION = "CHANGES";

    private ReportParser reportParser = ReportParser.makeReportParser();

    /**
     * Only different from the default no-args constructor in that it has a
     * throws clause.
     * @throws java.lang.Exception
     */
    public ChangesFileGenerator() throws Exception {}

    /////////////////////////////////////////////////////////////////////////
    //
    //  ENTRY POINT
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * Generate the changes file (for details on how to invoke this tool, see
     * the header comment on this class).
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ChangesFileGenerator me = new ChangesFileGenerator();

        me._invokedByAnt = false;

        if (me.parseArgs(args)) { me.execute(); }
        else { me.println(USAGE); }
    }

    /**
     * This is Ant's entry point into this task.
     * @throws BuildException
     */
    public void execute() throws BuildException {
        try {
            beginOutput();
            buildChangesList();
            replaceVariables();

            printOutput();
            printErrors();
        }
        catch (Throwable t) {
            t.printStackTrace();
            throw new BuildException("Error running ChangeFileGenerator: " +
                    t.getMessage(), t);
        }
    }



    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * Start the html output docment.
     */
    private void beginOutput()
        throws Exception {
        String titleText = "Changes for Derby " + releaseID;
        Element html = outputDoc.createElement(HTML);
        Element title = createTextElement(outputDoc, "title", titleText);
        Element body = outputDoc.createElement(BODY);

        outputDoc.appendChild(html);
        html.appendChild(title);
        html.appendChild(body);

        Element bannerBlock = createHeader(body, BANNER_LEVEL, titleText);
        buildDelta(bannerBlock);

        Element toc = createList(body);

        createSection(body, MAIN_SECTION_LEVEL, toc, BUG_FIXES_SECTION,
                BUG_FIXES_SECTION);
    }


    //////////////////////////////////
    //
    //  Bug List SECTION
    //
    //////////////////////////////////

    /**
     * Build the list of issues. Will only include issues <em>not</em> fixed in
     * previously released versions (as specified in releaseSummary.xml).
     * @throws Exception
     */
    protected void buildChangesList()
        throws Exception {
        Element bugListSection = getSection(outputDoc, MAIN_SECTION_LEVEL,
                BUG_FIXES_SECTION);

        String deltaStatement =
            "The following table lists issues in JIRA which were " +
            "fixed between " +
            "Derby release " + releaseID + " and the preceding release " +
            previousReleaseID + ". This includes issues for the product " +
            "source, documentation and tests";

        addParagraph(bugListSection, deltaStatement);

        Element table = createTable
            (bugListSection, DEFAULT_TABLE_BORDER_WIDTH,
            new String[] { ISSUE_ID_HEADLINE, DESCRIPTION_HEADLINE });

        for (Iterator i = JiraIssue.createJiraIssueList( bugListDoc, excludeReleaseIDList, reportParser ).iterator(); i.hasNext(); )
        {
            JiraIssue issue = (JiraIssue) i.next();
            println(issue.getKey());
            Element row = insertRow(table);
            Element linkColumn = insertColumn(row);
            Element descriptionColumn = insertColumn(row);
            Element hotlink = createLink(outputDoc, issue.getJiraAddress(),
                    issue.getKey());
            Text title = outputDoc.createTextNode(issue.getTitle());

            linkColumn.appendChild(hotlink);
            descriptionColumn.appendChild(title);
        }
    }



    //////////////////////////////////
    //
    //  ARGUMENT MINIONS
    //
    //////////////////////////////////

    /**
     * Returns true if arguments parse successfully, false otherwise.
     * @param args to be parsed
     * @return true if parsing succeeded, false otherwise
     */
    private boolean parseArgs( String[] args )
        throws Exception
    {
        if ( (args == null) || (args.length != 3) ) {
            return false;
        }

        int     idx = 0;

        setSummaryFileName( args[ idx++ ] );
        setBugListFileName( args[ idx++ ] );
        setOutputFileName( args[ idx++ ] );

        return true;
    }
}


