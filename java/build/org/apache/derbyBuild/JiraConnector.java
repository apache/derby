/*

   Derby - Class org.apache.derbyBuild.JiraConnector

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

/*
 * class that will build an xml file based on the xml jira reports. 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class JiraConnector {
	static String filePath=null;
	// filenames of files to be created.
	static String fixedBugsListFileName="fixedBugsList.xml";
	static String releaseNotesListFileName="ReleaseNotesList.xml";
	static String allJiraListFileName="all_JIRA_ISSUES.xml";

    
	public static String jira_releaseNotesSource =
		"http://issues.apache.org/jira/secure/IssueNavigator.jspa?view=rss" +
		"&pid=10594&sorter/field=issuekey&sorter/order=DESC&tempMax=50" +
		"&reset=true&decorator=none&customfield_12310090=" +
		"Existing+Application+Impact&customfield_12310090=Release+Note+Needed";

	public static String jira_fixedBugsSource =
		"http://issues.apache.org/jira/sr/jira.issueviews:" +
		"searchrequest-xml/temp/SearchRequest.xml?&pid=10594&resolution=1&" +
		"fixVersion=10.3.0.0&sorter/field=issuekey&sorter/order=DESC&" +
		"tempMax=1000&reset=true&decorator=none";

	public static String jira_allBugsSource= "https://issues.apache.org/jira/secure/IssueNavigator.jspa?view=rss&pid=10594&sorter/field=issuekey&sorter/order=DESC&tempMax=6000&reset=true&decorator=none";

	// other urls to some cute jira reports in xml.
	// all 
	//  (warning: avoid using this - it's tough on apache infrastructure.
	//  public static String jira_report="http://issues.apache.org/jira/secure/IssueNavigator.jspa?view=rss&pid=10594&sorter/field=issuekey&sorter/order=DESC&tempMax=5000&reset=true&decorator=none";
	// all open bugs
	//  public static String jira_BUG_OPEN="http://issues.apache.org/jira/secure/IssueNavigator.jspa?view=rss&pid=10594&types=1&statusIds=1&sorter/field=issuekey&sorter/order=DESC&tempMax=1000&reset=true&decorator=none";
	// one bug - the following two would be joined with in the middle a string like 
	// 'DERBY-225' to make the http to get xml for 1 bug.
	//  public static String onejirabegin="http://issues.apache.org/jira/browse/"; // 
	//  public static String onejiraend="?decorator=none&view=rss";

	public static void main(String[] args) {
		try{
//IC see: https://issues.apache.org/jira/browse/DERBY-4014
			if (args.length > 0 && args[0].equals("all"))
				// don't use this too often it is hard on Apache infrastructure.
				refreshJiraIssues(allJiraListFileName, jira_allBugsSource);
			else {
				refreshJiraIssues(fixedBugsListFileName, jira_fixedBugsSource);
				refreshJiraIssues(releaseNotesListFileName, jira_releaseNotesSource);
			}
		}catch(IOException ex){
			ex.printStackTrace();
		}catch(Exception exe){
			exe.printStackTrace();
		}
	}

	public static void refreshJiraIssues(String fileName, String stream) throws Exception {
		String sep = System.getProperty("file.separator");
		filePath = System.getProperty("user.dir") + sep + fileName; 
		getXMLStreamAndFile(fileName, stream);
	}

	private static void getXMLStreamAndFile(String fileName, String stream) throws IOException {
		FileInputStream fins=null;
		String XMLurl = stream;
		try{
			BufferedReader in = 
				new BufferedReader( new InputStreamReader(getXMLStream(XMLurl)));
			String inputLine;
			File file=new File(filePath);
			FileWriter fw=new FileWriter(file);
			while ((inputLine = in.readLine()) != null)
			{
				fw.write(inputLine);
			}
			in.close();
			fw.close();
			System.out.println("A new Jira XML File created: "+file);
		}catch(IOException e){
			//e.printStackTrace();
			throw e;
		}
	}

	public static InputStream getXMLStream(String XMLurl) throws MalformedURLException, IOException {
		URL url= new URL(XMLurl);
		System.out.println("Accessing url: " + XMLurl);
		URLConnection jiraSite = url.openConnection();
		return jiraSite.getInputStream();
	}
}
