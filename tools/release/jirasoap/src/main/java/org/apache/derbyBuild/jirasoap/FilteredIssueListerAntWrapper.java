/*

   Derby - Class org.apache.derbyBuild.jirasoap.FilteredIssueListerAntWrapper

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

import org.apache.tools.ant.BuildException;

/**
 * Wrapper for invoking {@code FilteredIssueLister} from ant.
 */
public class FilteredIssueListerAntWrapper {

    private String user;
    private String password;
    private String releaseVersion;
    /** JIRA filter id, or 0 (zero) for JQL. */
    private long filterId;
    private String output;

    public FilteredIssueListerAntWrapper() {};

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setReleaseVersion(String releaseVersion) {
        this.releaseVersion = releaseVersion;
    }

    public void setFilterId(String id)
            throws BuildException {
        // NOTE: A filter id of 0 (zero) will be treated specially,
        //       resulting in a JQL query being generated.
        try {
            filterId = Long.parseLong(id);
        } catch (NumberFormatException nfe) {
            throw new BuildException(
                    "invalid JIRA filter id (only digits allowed): " + id, nfe);
        }
    }

    public void setOutputFile(String output) {
        this.output = output;
    }

    public void setReportDisqualifications(String bool) {
        System.setProperty("reportDisqualifications", bool);
    }

    public void execute()
            throws BuildException {
        try {
            FilteredIssueLister issueLister =
                new FilteredIssueLister(user, password);
            // NOTE: A filter id of 0 (zero) will be treated specially,
            //       resulting in a JQL query being generated.
            issueLister.prepareReleaseNotes(
                    releaseVersion, filterId, output, null);
        } catch (Exception e) {
            throw new BuildException(e);
        }

    }
}
