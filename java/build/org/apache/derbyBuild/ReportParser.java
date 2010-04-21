/*

   Derby - Class org.apache.derbyBuild.ReportParser

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

import java.util.HashSet;
import java.text.ParseException;

/**
 * <p>
 * This is the machine which parses a JIRA report, extracting information needed to generate
 * release notes. You will probably have to create a separate implementation of
 * this class for each release. That is because the JIRA reports change shape between releases.
 * </p>
 *
 * <p>
 * When you need to generate release notes for a new release, edit this file as follows:
 * </p>
 *
 * <ul>
 * <li>Create a new inner subclass, modelled on April_2010. Your new inner subclass will
 * provide the parsing logic specific to the shape of JIRA reports at that time.</li>
 * <li>Change makeReportParser() to return an instance of your new inner subclass.</li>
 * </ul>
 */
public abstract class ReportParser
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    public static final long NO_RELEASE_NOTE = -1;

    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////f

    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    //
    //  STATIC BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create a ReportParser which understands the current shape of JIRA reports.
     * </p>
     */
    public static ReportParser makeReportParser()
    {
        return new April_2010();
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  ABSTRACT BEHAVIOR TO BE RE-IMPLEMENTED FOR EACH RELEASE
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Return a TagReader wrapped around the next JIRA issue in the report.
     * </p>
     *
     * @param masterReport A TagReader wrapped around the JIRA report.
     *
     * @return A TagReader wrapped around the next issue in the report. Returns null if there are no more issues.
     */
    public abstract TagReader parseNextIssue( TagReader masterReport ) throws Exception;

    /**
     * <p>
     * Parse the numeric Derby JIRA id out of an issue description. For instance,
     * for DERBY-1234, this method returns the string "1234".
     * </p>
     *
     * @param tr A TagReader positioned on the JIRA issue which is to be parsed.
     *
     * @return The numeric id of the Derby JIRA issue as a string.
     */
    public abstract String parseKey( TagReader tr ) throws Exception;

    /**
     * <p>
     * Parse the title (one line summary) out of a Derby JIRA description.
     * </p>
     *
     * @param tr A TagReader positioned on the JIRA issue which is to be parsed.
     *
     * @return The one line summary of the issue.
     */
    public abstract String parseTitle( TagReader tr ) throws Exception;

    /**
     * <p>
     * Parse the set of Fixed-in versions out of a Derby JIRA description.
     * These are the ids of all of the Derby versions in which the bug is fixed.
     * For instance, this could be the set { "10.5.3.0", "10.6.0.0" }.
     * </p>
     *
     * @param tr A TagReader positioned on the JIRA issue which is to be parsed.
     *
     * @return The set of versions in which the bug is fixed.
     */
    public abstract HashSet parseFixedVersions( TagReader tr ) throws Exception;

    /**
     * <p>
     * Get the attachment id of the latest release note attached to an issue.
     * </p>
     *
     * @param tr A TagReader positioned on the JIRA issue which is to be parsed.
     *
     * @return The attachment id of the latest release note, or NO_RELEASE_NOTE if the issue has no release note.
     */
    public abstract long getReleaseNoteAttachmentID( TagReader tr ) throws Exception;

    /////////////////////////////////////////////////////////////////////////
    //
    //  INNER CLASSES
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * A ReportParser which understands the format of Apache JIRA reports
     * in April 2010. This ReportParser assumes that the report includes the following
     * columns and was produced by selecting "Full Content" option under "Content View"
     * and then saved to disk. For the 10.6.1 release, I produced the report using the JIRA filter called
     * "10.6.1 Fixed Bugs List":
     * </p>
     *
     * <ul>
     * <li>Key</li>
     * <li>Summary</li>
     * <li>Fix Version/s</li>
     * </ul>
     */
    public static final class April_2010 extends ReportParser
    {
        public TagReader parseNextIssue( TagReader masterReport ) throws Exception
        {
            int newIssueIdx = masterReport.position( "<tr id=\"issuerow", false );
            if ( newIssueIdx < 0 ) { return null; }

            String issueContent = masterReport.getUpTill( "</tr>", true );

            return new TagReader( issueContent );
        }
        
        public String parseKey( TagReader tr ) throws Exception
        {
            tr.reset();
            
            tr.position( "issues.apache.org/jira/browse/DERBY-", true );
            String keyString = tr.getUpTill( "\">", true );
            
            return keyString;
        }
        
        public String parseTitle( TagReader tr ) throws Exception
        {
            tr.reset();
            
            tr.position( "nav summary\">", true );
            tr.position( ">", true );
            String title = tr.getUpTill( "</a>", true );
            
            return title;
        }
        
        public HashSet parseFixedVersions( TagReader tr ) throws Exception
        {
            tr.reset();
            
            HashSet retval = new HashSet();
            
            while ( tr.position( "fixforversion", false ) >= 0 )
            {
                tr.position( ">", true );
                String version = tr.getUpTill( "<", true );
                
                retval.add( version );
            }
            
            return retval;
        }

        /**
         * <p>
         * The attachment ids don't turn up in the html reports produced by the Apache
         * JIRA site. So, lamely, we hardcode attachment ids here. For this ReportParser,
         * we hardcode the attachment ids of the 10.6.1 release notes.
         * </p>
         */
        public long getReleaseNoteAttachmentID( TagReader tr ) throws Exception
        {
            tr.reset();

            long result = NO_RELEASE_NOTE;
            String key = parseKey( tr );

            if ( key.equals( "4602" ) ) { result = 12440335L; }
            else if ( key.equals( "4483" ) ) { result = 12439775L; }
            else if ( key.equals( "4432" ) ) { result = 12424709L; }
            else if ( key.equals( "4380" ) ) { result = 12434514L; }
            else if ( key.equals( "4355" ) ) { result = 12419298L; }
            else if ( key.equals( "4312" ) ) { result = 12442288L; }
            else if ( key.equals( "4230" ) ) { result = 12409466L; }
            else if ( key.equals( "4191" ) ) { result = 12442312L; }
            else if ( key.equals( "3991" ) ) { result = 12409798L; }
            else if ( key.equals( "3844" ) ) { result = 12436979L; }
            else if ( key.equals( "2769" ) ) { result = 12418474L; }
            
            return result;
        }
    }

}
