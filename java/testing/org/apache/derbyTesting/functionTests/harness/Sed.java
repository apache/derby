/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.Sed

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.harness;

/***
 * Sed.java
 *
 * This is a version of "sed" in Java for the Cloudscape Function Tests,
 * written using the OROMatcher Perl5 regular expression classes.
 * The substitutions/deletions are based on the original kornshell tests.
 *
 ***/

import java.io.*;
import java.util.Vector;
import org.apache.oro.text.regex.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;

public class Sed
{

    public Sed()
    {
    }

    public static void main(String[] args) throws Exception {
	if (args == null || args.length != 2) {
		System.err.println("Usage: Sed sourcefile targetfile");
		System.exit(1);
	}
	File src = new File(args[0]);
	File tgt = new File(args[1]);
	new Sed().exec(src,tgt,null, false);
    }

    // The arguments should be the names of the input and output files
    public void exec(File srcFile, File dstFile, InputStream isSed, boolean isJCC)
        throws IOException
    {
        // Vector for storing lines to be deleted
        Vector deleteLines = new Vector();
        deleteLines.addElement("^ij version.*$");
        deleteLines.addElement("^\\*\\*\\*\\* Test Run Started .* \\*\\*\\*\\*$");
        deleteLines.addElement("^\\*\\*\\*\\* Test Run Completed .* \\*\\*\\*\\*$");
        deleteLines.addElement("^ELAPSED TIME = [0-9]* milliseconds$");
        deleteLines.addElement("^Symantec Java! JustInTime Compiler Version .*$");
        deleteLines.addElement("^Copyright .* Symantec .*$");
        deleteLines.addElement("^\\^\\?$");
        //deleteLines.addElement("^\\.$"); // originally to remove lines with a dot
        deleteLines.addElement("^S.*ij> $");
        deleteLines.addElement("^ *$");
        deleteLines.addElement("^Server StackTrace:$");
        deleteLines.addElement("^\\[ *$");
        deleteLines.addElement("^\\] *$");
        deleteLines.addElement("^\\[$");
        deleteLines.addElement("^\\]$");
        deleteLines.addElement("^<not available>\\]$");
        deleteLines.addElement("^weblogic\\..*$");
        deleteLines.addElement("^(.*at .*)\\(.*:[0-9].*\\)$");
        deleteLines.addElement("^(.*at .*)\\(*.java\\)$");
        deleteLines.addElement("^(.*at .*)\\(Compiled Code\\)$");
        deleteLines.addElement("^(.*at .*)\\(Interpreted Code\\)$");
        deleteLines.addElement("^(.*at .*)\\(Unknown Source\\)$");
        deleteLines.addElement("^(.*at .*)\\(Native Method\\)$");
        deleteLines.addElement("^.*at weblogic\\..*$");
        deleteLines.addElement("^\\tat $"); // rare case of incomplete stack trace line
        deleteLines.addElement("JBMSTours\\.vti\\.jdbc1_2\\.ExternalTable"); // For some reason ArchiveData.out outputs it's errors in random order, sed them both out.
        deleteLines.addElement("optimizer estimated cost");
        deleteLines.addElement("optimizer estimated row count");
        deleteLines.addElement("^WARNING: Cloudscape \\(instance.*$");
        deleteLines.addElement("^Warning: Cloudscape \\(instance.*$");
        deleteLines.addElement("Using executables built for native_threads");
        deleteLines.addElement("Estimate of memory used");
        deleteLines.addElement("Size of merge runs");
        deleteLines.addElement("Number of merge runs");
        deleteLines.addElement("Sort type");
        deleteLines.addElement("Optimization started at .*$");
        deleteLines.addElement("WARNING 02000: No row was found for FETCH, UPDATE or DELETE");
	// deleteLines for stack traces from j9 jvm to match those above for other jvms
 	deleteLines.addElement("Stack trace:");	
        deleteLines.addElement("^.*java/.*\\<init\\>\\(.*\\)V");
 	deleteLines.addElement("^.*org/apache/derby/.*\\(.*\\).*$");	
	// next for j9 stack trace with jarfiles test run.
 	deleteLines.addElement("^.*derby/.*\\<.*\\>\\(.*\\).*$");	
 	deleteLines.addElement("^.*derby/.*\\(.*\\).*$");	
 	deleteLines.addElement("^.*java/.*\\(.*\\).*$");
	deleteLines.addElement("^\\[.*db2jcc.jar\\] [0-9].[1-9] - .*$");	
	deleteLines.addElement("^\\[.*db2jcc_license_c.jar\\] [1-9].[0-9] - .*$");	

        // Vectors for substitutions
        Vector searchStrings = new Vector();
        searchStrings.addElement("^WARNING: JBMS \\(instance *");
        searchStrings.addElement("^Warning: JBMS \\(instance *");
        searchStrings.addElement("^Transaction:\\(.*\\) *\\|");
        searchStrings.addElement("^Read [0-9]* of [0-9]* bytes$");
	// added for ibridge connections
        searchStrings.addElement("jdbc:derby:informix://localhost:1527/");
        // This was for wl output; it needs some FIXUP to work
        // or we need to change the masters (which would be easier)
        //searchStrings.addElement("\\[B\\@[0-9a-f]");
        searchStrings.addElement("Directory .*connect.wombat.seg0");
        searchStrings.addElement("^ij> Warning: Cloudscape \\(instance.*$");
        searchStrings.addElement("^ij> WARNING: Cloudscape \\(instance.*$");
        searchStrings.addElement("^ij(\\([0-9]\\))> WARNING: Cloudscape \\(instance.*$");
        searchStrings.addElement("^ij(\\([0-9]\\))> Warning: Cloudscape \\(instance.*$");
        deleteLines.addElement("^XSDB.*$");
        // Filter for constraint names - bug 5622 - our internal constraint names are too long. To be db2 compatible, we have reworked them.
        StringBuffer constraintNameFilter = new StringBuffer();
        constraintNameFilter.append("SQL[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]");
        searchStrings.addElement(constraintNameFilter.toString());
        // Filter for uuids
        StringBuffer uuidFilter = new StringBuffer();
        uuidFilter.append("[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]-");
        uuidFilter.append("[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-");
        uuidFilter.append("[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-");
        uuidFilter.append("[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-");
        uuidFilter.append("[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]");
        searchStrings.addElement(uuidFilter.toString());
		// Filter for timestamps
		StringBuffer	timestampFilter = new StringBuffer();
		timestampFilter.append( "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] " );
		timestampFilter.append( "[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]* *" );
		searchStrings.addElement( timestampFilter.toString() );
		// 3 digit year
		timestampFilter = new StringBuffer();
		timestampFilter.append( "[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] " );
		timestampFilter.append( "[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]* *" );
		searchStrings.addElement( timestampFilter.toString() );
		// ibm13 year
		timestampFilter = new StringBuffer();
		timestampFilter.append( "[0-9]-[0-9][0-9]-[0-9][0-9] " );
		timestampFilter.append( "[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]* *" );
		searchStrings.addElement( timestampFilter.toString() );
		searchStrings.addElement("^COM\\.jbms\\..*\\|");
		// Filter remove transaction id's from deadlock messages
		searchStrings.addElement("^  Waiting XID : {.*}");
		searchStrings.addElement("^  Granted XID : .*$");
		searchStrings.addElement("^The selected victim is XID : .*");
		// Filters for build numbers
		searchStrings.addElement("(Cloudscape - DBMS:[A-Za-z]* - [0-9]\\.[0-9]\\.[0-9] - )\\(([0-9]*)\\)");
		searchStrings.addElement("(beta - )\\(([0-9]*)\\)");
		searchStrings.addElement("Level2CostEstimateImpl: .*");
		// Filter for xa tests for the numbers representing the db name (it can change)
		searchStrings.addElement("^Transaction ([0-9])* : \\(([0-9]*)\\,([0-9a-f]*)\\,([0-9a-f]*)\\)");
		// Filter for optimizer number for zindexesLevel1 test (due to a change in display width for the test)
		searchStrings.addElement("^Modifying access paths using optimizer .[0-9]*");
		searchStrings.addElement("CDWS[0-9]*");
		searchStrings.addElement("IXWS[0-9]*");
		searchStrings.addElement("^.*COM.ibm.db2.jdbc.DB2Exception: \\[IBM\\]\\[CLI Driver\\] SQL1013N  The database alias name or database name \".*\\_M\".*");
		// for j9, to eliminate intermittent failures due to this problem in j9:
		searchStrings.addElement("FAILED STACK MAP");
		if (isJCC)
		{
			searchStrings.addElement("[ ]*\\|");
			searchStrings.addElement("^--*");
		}

		//Filter to suppress absould paths in error message for roll forward recovery tests 
		searchStrings.addElement("Directory.*.wombat.already.exists");

        Vector subStrings = new Vector();
        subStrings.addElement("");
        subStrings.addElement("");
        subStrings.addElement("Transaction:(XXX)|");
        subStrings.addElement("Read ... bytes");
		// for iBridge connections
        subStrings.addElement("cloudscape:derby:");
        //subStrings.addElement("Bx"); // originally for 
        subStrings.addElement("Directory DBLOCATION/seg0");
        subStrings.addElement("ij> ");
        subStrings.addElement("ij> ");
        subStrings.addElement("ij$1> ");
        subStrings.addElement("ij$1> ");
        subStrings.addElement("xxxxGENERATED-IDxxxx");
        subStrings.addElement("xxxxFILTERED-UUIDxxxx");
        subStrings.addElement("xxxxxxFILTERED-TIMESTAMPxxxxx");
        subStrings.addElement("xxxxxxFILTERED-TIMESTAMPxxxxx");
        subStrings.addElement("xxxxxxFILTERED-TIMESTAMPxxxxx");
        subStrings.addElement("Transaction:(XXX)|");
		// remove transaction id's from deadlock messages
		subStrings.addElement("  Waiting XID : {WWW,QQQ}");
		subStrings.addElement("  Granted XID : {GGG.QQQ}...");
		subStrings.addElement("The selected victim is XID : VVV");
		// sub build numbers
		subStrings.addElement("$1(xxXXxxFILTERED-BUILD-NUMBERxxXXxx)");
		subStrings.addElement("$1(xxXXxxFILTERED-BUILD-NUMBERxxXXxx)");
		subStrings.addElement("Level2CostEstimateImpl: xxXXxxFILTERED-INFORMATIONxxXXxx");
		// sub for db name in xa tests (it can change)
		subStrings.addElement("Transaction $1 : ($2,FILTERED,FILTERED)");
		// sub for optimizer number for zindexesLevel1 test
		subStrings.addElement("Modifying access paths using optimizer FILTERED_NUMBER");
		subStrings.addElement("CDWSno");
		subStrings.addElement("IXWSno");
		subStrings.addElement("COM.ibm.db2.jdbc.DB2Exception: [IBM][CLI Driver] SQL1013N  The database alias name or database name xxxFILTEREDMIRRORDBxxx could not be found.  SQLSTATE=42705");
		// for j9, to eliminate intermittent failures due to this problem in j9:
		subStrings.addElement("");
		// for JCC replace multiple blanks with one blank to handle differences
		// in display width
		if (isJCC)
		{
			subStrings.addElement(" |");
			subStrings.addElement("-----");
		}
		subStrings.addElement("Directory DBLOCATION/wombat already exists");
		doWork(srcFile, dstFile, null, deleteLines, searchStrings, subStrings, isSed);

	}
	// This just does JCC changes on the output master file
    public void execJCC(InputStream is, File dstFile)
        throws IOException
    {
        // Vector for storing lines to be deleted
        Vector deleteLines = new Vector();

        // Vectors for substitutions
        Vector searchStrings = new Vector();
		searchStrings.addElement("^true[ ]*\\|");
		searchStrings.addElement("^false[ ]*\\|");
		searchStrings.addElement("\\|true[ ]*\\|");
		searchStrings.addElement("\\|false[ ]*\\|");
		searchStrings.addElement("[ ]*\\|");
		searchStrings.addElement("^--*");

        Vector subStrings = new Vector();
		// true and false show up as 1 and 0 in JCC. 
		//because they have no boolean support
		subStrings.addElement("1 |");
		subStrings.addElement("0 |");
		subStrings.addElement("|1 |");
		subStrings.addElement("|0 |");
		subStrings.addElement(" |");
		subStrings.addElement("-----");

		doWork(null, dstFile, is, deleteLines, searchStrings, subStrings, null);

	}
	// for rmi, there's a warning about the db, but it's really ok.
    public void rmiexec(File srcFile, File dstFile, InputStream isSed)
        throws IOException
    {
        // Vector for storing lines to be deleted
        Vector deleteLines = new Vector();
		// for Rmi:
	 	deleteLines.addElement("^.*WARNING: Wierd RMI server URL:.*$");	
	 	deleteLines.addElement("^.*WARNING 01J01: Database 'wombat' not created, connection made to existing database instead.");	

        Vector searchStrings = new Vector();
		//searchStrings.addElement("");
        Vector subStrings = new Vector();
		//subStrings.addElement("");

		doWork(srcFile, dstFile, null, deleteLines, searchStrings, subStrings, isSed);
	}

	private void doWork(File srcFile, File dstFile, InputStream is, Vector deleteLines, 
		Vector searchStrings, Vector subStrings, InputStream isSed)
        throws IOException
	{
		
        boolean lineDeleted = false;
        PatternMatcher matcher;
        Perl5Compiler pcompiler;
        PatternMatcherInput input;
        BufferedReader inFile;
        PrintWriter outFile;
        String result = "";
        String regex;
        Vector delPatternVector = new Vector();
        Vector subPatternVector = new Vector();

	// ---------------------------------
        // Try loading the sed properties if they exist (see jdbc_sed.properties as an example)
        if ( isSed != null )
        {
	    Properties sedp = new Properties();
		
            sedp.load(isSed);
	    for (Enumeration e = sedp.propertyNames(); e.hasMoreElements(); )
		{
		    String key = (String)e.nextElement();
		    if (key.equals("substitute"))
		    {
			String value = sedp.getProperty(key);
			// value string contains a comma separated list of patterns
			StringTokenizer st = new StringTokenizer(value,",");
			String patternName = ""; 
			String patName = ""; 
			String subName = ""; 
			while (st.hasMoreTokens())
			{
			    patternName = st.nextToken();
			    // pattern;substitute
			    StringTokenizer st2 = new StringTokenizer(patternName,";");
			    patName = st2.nextToken();
			    subName = st2.nextToken();
			    if (!patName.equals("") && !subName.equals(""))
			    {
				searchStrings.addElement(patName);
				subStrings.addElement(subName);
			    }
//System.out.println("pattern = " + patName + " substitute " + subName);
			}
		    }
		    else if (key.equals("delete"))
		    {
			String value = sedp.getProperty(key);
			// value string contains a comma separated list of patterns
			StringTokenizer st = new StringTokenizer(value,",");
			String patternName = ""; 
			while (st.hasMoreTokens())
			{
			    patternName = st.nextToken();
			    deleteLines.addElement(patternName);
			}
		    }
                }
        }
	// ---------------------------------

        //Create Perl5Compiler and Perl5Matcher
        pcompiler = new Perl5Compiler();
        matcher = new Perl5Matcher();

        // Define the input and output files based on args
		if (is == null)
        	inFile = new BufferedReader(new FileReader(srcFile));
		else
			inFile = new BufferedReader(new InputStreamReader(is));
        outFile = new PrintWriter
            ( new BufferedWriter(new FileWriter(dstFile), 10000), true );

        // Attempt to compile the patterns for deletes
        for (int i = 0; i < deleteLines.size(); i++)
        {
            try
            {
                regex = (String)deleteLines.elementAt(i);
                //System.out.println("The pattern: " + regex);
                Pattern pattern = pcompiler.compile(regex);
                if (pattern == null)
                    System.out.println("pattern is null");
                delPatternVector.addElement(pattern);
            }
            catch(MalformedPatternException e)
            {
                System.out.println("Bad pattern.");
                System.out.println(e.getMessage());
            }
        }

        // Attempt to compile the patterns for substitutes
        for (int i = 0; i < searchStrings.size(); i++)
        {
            try
            {
                regex = (String)searchStrings.elementAt(i);
                //System.out.println("The pattern: " + regex);
                Pattern pattern = pcompiler.compile(regex);
                if (pattern == null)
                    System.out.println("pattern is null");
                subPatternVector.addElement(pattern);
            }
            catch(MalformedPatternException e)
            {
                System.out.println("Bad pattern.");
                System.out.println(e.getMessage());
            }
        }

        String str;
        int j;
        int lineCount = 0;
        // Read the input file
        while ( (str = inFile.readLine()) != null )
        {
            lineCount++;
            
            //System.out.println("***Line no: " + lineCount);
            //System.out.println("***Line is: " + str);
            lineDeleted = false;

            // First delete any nulls (Cafe 1.8 leaves nulls)
            if (str.length() == 1)
            {
                if (str.charAt(0) == (char) 0)
                {
                    // Skip this line, don't write it
                    //System.out.println("Skip this line...");
                    lineDeleted = true;
                }
            }

            // Determine if this line should be deleted for delete pattern match
            if ( lineDeleted == false )
            {
                for (j = 0; j < delPatternVector.size(); j++)
                {
                    if ( matcher.contains( str, (Pattern)delPatternVector.elementAt(j) ) )
                    {
                        //System.out.println("***Match found to delete line***");
                        String tmpp = ((Pattern)delPatternVector.elementAt(j)).getPattern();
                        //System.out.println("***Pattern is: " + tmpp);

                        // In this case we are removing the line, so don't write it out
                        lineDeleted = true;
                        break;
                    }
                }
            }

            // Determine if any substitutions are needed
            if (lineDeleted == false)
            {
		Substitution substitution;
		StringSubstitution strsub = new StringSubstitution("");
		Perl5Substitution perlsub = new Perl5Substitution("");
                boolean subDone = false;
                for (j = 0; j < subPatternVector.size(); j++)
                {
                    input = new PatternMatcherInput(str);
                    Pattern patt = (Pattern)subPatternVector.elementAt(j);
                    String pstr = patt.getPattern();
                    //System.out.println("Pattern string is " + pstr);
                    String sub = (String)subStrings.elementAt(j);
		    if (sub.indexOf("$") > 0)
		    {
                        perlsub.setSubstitution(sub);
                        substitution = (Substitution)perlsub;
                    } else {
                        strsub.setSubstitution(sub);
			substitution = (Substitution)strsub;
		    }
                    //System.out.println("Substitute str = " + sub);
                    if ( matcher.contains( input, patt ) )
                    {
                        MatchResult mr = matcher.getMatch();
                        //System.out.println("***Match found for substitute***");
                        // In this case we do a substitute
                        result = Util.substitute(matcher, patt, substitution, str,
                            Util.SUBSTITUTE_ALL);
                        //System.out.println("New string: " + result);
                        //outFile.println(result);
                        str = result;
                        subDone = true;
                    }
                }
                if (subDone)
                {
                    //System.out.println("write the subbed line");
                    outFile.println(result);
                }
                else
                {
                    //System.out.println("Write the str: " + str);
                    outFile.println(str);
                    outFile.flush();
                }
            }// end if
        } // end while
        inFile.close();
        outFile.flush();
        outFile.close();
    }// end exec
}
