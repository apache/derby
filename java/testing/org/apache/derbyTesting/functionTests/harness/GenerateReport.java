/* 

   Derby - Class org.apache.derbyTesting.functionTests.harness.GenerateReport

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

import java.util.Properties;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
	Generate summary information from a RunSuite run.
	Can be called separately, if given the suite name.
	Will be called from RunSuite if System property genrep=true.

	Condenses run information down, prints out result stats,
	and shows details of failures (.diff files).

	@author ames
**/
public class GenerateReport {

	static void CollectProperties () {
		Properties ps = System.getProperties();
		char[] newline = {' '};
		propFile.println(PropertyUtil.sortProperties(ps,newline));
	}

	static void CalculateRunLength () {
	// read suite.sum for start/end timestamps, 
	// report start date and full duration of run
		String odn = System.getProperty("outputdir");
		if (odn==null) odn = System.getProperty("user.dir");

		BufferedReader sumFile = null;
		String firstLine = null;
		String lastLine = null;
		try {
			sumFile = new BufferedReader(new FileReader(new File(new File(odn),SuiteName+".sum")));
			firstLine = sumFile.readLine();
			String aLine = firstLine;
			while (aLine!=null) {
				lastLine = aLine;
				aLine = sumFile.readLine();
			}
			sumFile.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace(System.out);
		}

		// have firstLine and lastLine.
		// format is: 
		// ******* Start Suite: <suite> <timestamp> *******
		// ******* End Suite: <suite> <timestamp> *******
		int tsStart = 22+SuiteName.length();
		int tsEnd = firstLine.length()-8;
		TestStart = Timestamp.valueOf(firstLine.substring(tsStart,tsEnd));
		// last line is two shorter
		tsStart-=2; tsEnd-=2;
		Timestamp testEnd = Timestamp.valueOf(lastLine.substring(tsStart,tsEnd));

		long testLen = testEnd.getTime() - TestStart.getTime();
		// Time isn't really a duration, so we have to set the fields
		int sec = (int) (testLen / 1000);
		int min = sec / 60;
		int hr = min / 60;
		sec = sec - (min*60); // adjust for part removed
		min = min - (hr*60); // adjust for part removed
		Calendar cal = new GregorianCalendar();
		cal.set(Calendar.HOUR_OF_DAY,hr);
		cal.set(Calendar.MINUTE,min);
		cal.set(Calendar.SECOND,sec);
		TestDuration = new Time(cal.getTime().getTime());
	}

	static void CollectPassFailStats () {
		// need to ensure outputdir is set...
		String odn = System.getProperty("outputdir");
		if (odn==null) odn = System.getProperty("user.dir");
		CollectPassFailStats(new File(odn),"");
	}

	static void addLines(PrintWriter outFile, File inFile, String relativeName) {
		BufferedReader readFile = null;
		try {
		readFile = new BufferedReader(new FileReader(inFile));
		String aLine = readFile.readLine();
		while (aLine!=null) {
			outFile.print(relativeName);
			outFile.print(":");
			outFile.println(aLine);
			aLine = readFile.readLine();
		}
		readFile.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace(System.out);
		}

	}

	static void addDiff(PrintWriter outFile, File inFile, String relativeName) {
		BufferedReader readFile = null;
		try {
		readFile = new BufferedReader(new FileReader(inFile));
		outFile.print("********* Diff file ");
		outFile.println(relativeName);
		String aLine = readFile.readLine();
		while (aLine!=null) {
			outFile.println(aLine);
			aLine = readFile.readLine();
		}
		readFile.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace(System.out);
		}
	}

	static void CollectPassFailStats (File dir,String relativeName) {
		// starting in specified dir, 
		String[] fileList = dir.list(fileFilter);
		int l = fileList.length;
		for (int i=0;i<l;i++) {
			String fileName = fileList[i];
			File file = new File(dir,fileName);

			// collect all .pass files into suite_pass.txt (passFile)
			if (fileName.endsWith(".pass")) {
				addLines(passFile,file,relativeName+"/"+fileName);
			}
			// collect all .fail files into suite_fail.txt (failFile)
			else if (fileName.endsWith(".fail")) {
				addLines(failFile,file,relativeName+"/"+fileName);
			}
			// collect all .skip files into suite_skip.txt (skipFile)
			else if (fileName.endsWith(".skip")) {
				addLines(skipFile,file,relativeName+"/"+fileName);
			}
			// collect all .diff files into suite_diff.txt (diffFile)
			else if (fileName.endsWith(".diff")) {
				addDiff(diffFile,file,relativeName+"/"+fileName);
			}

			// recurse on all directories
			else // it's a directory
			{
				String newDir;
				if (relativeName.length()>0)
					newDir = relativeName+"/"+fileName;
				else newDir = fileName;
				CollectPassFailStats(file, newDir);
			}
		}
	}

	static void CalculatePassFailStats() {
		// total tests run
		// #, % failures
		// #, % passed
		NumPass = CountLines (passFileName);
		NumFail = CountLines (failFileName);
		NumRun = NumPass+NumFail;
		NumSkip = CountLines (skipFileName);
		PercentPass = (int)Math.round(100* ((double)NumPass/(double)NumRun));
		PercentFail = (int)Math.round(100* ((double)NumFail/(double)NumRun));
	}

	static int CountLines(String fileName) {
		BufferedReader readFile = null;
		int line = 0;
		try {
		readFile = new BufferedReader(new FileReader(fileName));
		String aLine = readFile.readLine();
		while (aLine!=null) {
			line++;
			aLine = readFile.readLine();
		}
		readFile.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace(System.out);
		}
		return line;
	}

	static void OutputFile(String fileName) {
		BufferedReader readFile = null;
		try {
		readFile = new BufferedReader(new FileReader(fileName));
		String aLine = readFile.readLine();
		while (aLine != null) {
			reportFile.println(aLine);
			aLine = readFile.readLine();
		}
		readFile.close();
		} catch (IOException ioe) { 
			ioe.printStackTrace(System.out);
		}
	}

	static PrintWriter setupFile(String fn) {
		File f = null;
		PrintWriter pw = null;
		try {
		f = new File(fn);
		if (f.exists()) {
			System.out.println("WARNING: removing "+fn);
			f.delete();
		}
		pw = new PrintWriter(new FileWriter(fn,true));
		} catch (IOException ioe) { 
			ioe.printStackTrace(System.out);
		}
		return pw;
	}

	public static void main(String[] args) {
		SuiteName = args[0];
		String jvmName = args[1];
		String javaCmd = args[2];
		String classpath = args[3];
		String framework = args[4];
		String processexec = args[5];
		boolean useprocess = true;
		if ( (processexec.toLowerCase()).startsWith("false") )
		    useprocess = false;
		String reportFileName = SuiteName+"_report.txt";
		reportFile = setupFile(reportFileName);
		reportFile.print("Generating report for RunSuite ");
		for (int i=0;i<args.length;i++)
			reportFile.print(args[i]+" ");
		reportFile.println();
		passFileName = SuiteName+"_pass.txt";
		failFileName = SuiteName+"_fail.txt";
		diffFileName = SuiteName+"_diff.txt";
		skipFileName = SuiteName+"_skip.txt";
		propFileName = SuiteName+"_prop.txt";
		passFile = setupFile(passFileName);
		failFile = setupFile(failFileName);
		diffFile = setupFile(diffFileName);
		skipFile = setupFile(skipFileName);
		propFile = setupFile(propFileName);

		// sysinfo printout
		SysInfoLog sysLog = new SysInfoLog();
		try
		{
		    sysLog.exec(jvmName, javaCmd, classpath, framework, reportFile, useprocess);
		    //SysInfoMain.getMainInfo(reportFile,false,false);
		}
		catch (Exception e)
		{
		    System.out.println("SysInfoLog Exception: " + e.getMessage());
		}
	
		reportFile.println("Test environment information:");
		reportFile.print("COMMAND LINE STYLE: ");
		String jvm = System.getProperty("jvm");
		if (jvm == null) jvm="jdk13";
		reportFile.println(jvm);
		reportFile.print("TEST CANONS: ");
		String canondir = System.getProperty("canondir");
		if (canondir == null) canondir = "master";
		reportFile.println(canondir);
		reportFile.println(DASHLINE);

		reportFile.println(DASHLINE);
		reportFile.println("Summary results:");
		CalculateRunLength();
		CollectPassFailStats();
		CollectProperties();
		passFile.close();
		failFile.close();
		skipFile.close();
		diffFile.close();
		propFile.close();
		CalculatePassFailStats();
		reportFile.println();
		reportFile.println("Test Run Started: "+TestStart);
		reportFile.println("Test Run Duration: "+TestDuration);
		reportFile.println();
		reportFile.println(NumRun+" Tests Run");
		if (PercentPass<10) reportFile.print(" ");
		reportFile.println(PercentPass+"% Pass ("+NumPass+" tests passed)");
		if (PercentFail<10) reportFile.print(" ");
		reportFile.println(PercentFail+"% Fail ("+NumFail+" tests failed)");
		reportFile.println(NumSkip + " Suites skipped");
		reportFile.println(DASHLINE);

		if (NumFail>0) {
			reportFile.println("Failed tests in: "+failFileName);
			reportFile.println(DASHLINE);
		}

		if (NumPass>0) {
			reportFile.println("Passed tests in: "+passFileName);
			reportFile.println(DASHLINE);
		}

		if (NumSkip>0) {
			reportFile.println("Skipped suites in: "+skipFileName);
			reportFile.println(DASHLINE);
		}

		reportFile.println("System properties in: "+propFileName);
		reportFile.println(DASHLINE);

		reportFile.println(DASHLINE);
		if (NumFail>0) {
			reportFile.println("Failure Details:");
			// cat each .diff file with full test name
			OutputFile(diffFileName);
		}
		else reportFile.println("No Failures.");
		reportFile.println(DASHLINE);
		reportFile.close();

		System.out.println("Generated report: "+reportFileName);
	}
	
	static final String DASHLINE="------------------------------------------------------";
	static String passFileName, failFileName, diffFileName, skipFileName, propFileName;
	static PrintWriter passFile, failFile, diffFile, skipFile, propFile;
	static PrintWriter reportFile;
	static FilenameFilter fileFilter = new GRFileFilter();
	static int NumPass, NumFail, NumRun, NumSkip;
	static int PercentPass, PercentFail;
	static Timestamp TestStart;
	static Time TestDuration;
	static String SuiteName;
}
