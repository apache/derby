/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.MultiTest

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

package org.apache.derbyTesting.functionTests.harness;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.util.Vector;
import java.util.Enumeration;
import org.apache.derby.impl.tools.ij.*;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.tools.i18n.LocalizedOutput;
import org.apache.derbyTesting.functionTests.util.TestUtil;


/**
 * MultiTest is a multiuser test harness.  It
 * runs multiple threads with differing scripts
 * against an embedded server.  For syntax see
 * grammar.jj
 *
 * Tests are ended as soon as an unexpected error
 * has occurred or after the specified test duration
 * has transpired.  The main loop ends by explicitly
 * quitting (i.e. doing a System.exit()).
 *
 * Deprecated APIs: this test uses AppStreamWriter instead
 * of the preferred AppStreamWriter.  This is because utilMain()
 * uses AppStreamWriter (deliberately, i think) so since
 * that is called from this, there you have it.
 */

public class MultiTest
{

	private static final  int MAX_WAIT_FOR_COMPLETION = 180; 

	private static mtTestSuite suite;
	private static LocalizedOutput log; 
	private static String	inputDir;
	private static String	outputDir;
	private static String	testName;


	
	public MultiTest () { };

	public static void syntax()
	{
		System.out.println("Syntax:"
				+"\n\t <file>\t- the cmd file"
				+"\n\t[-o <dir>]\t-the output directory"
				+"\n\t[-i <dir>]\t-the input directory");
	}

	/**
	** Main loop
	@exception IOException thrown on error
	@exception ParseException thrown on error
	@exception FileNotFoundException thrown on error
	*/
	public static void main(String[] args) 
			throws IOException, ParseException, FileNotFoundException
	{
		String	cmdFilePath;
		InputStream	in;
		String	cmdFile;
		mtTester[] testers;
		int	i;
		int	max;

		if ((cmdFile = util.getFileArg(args)) == null)
		{
			syntax();
			return;
		}

		LocalizedResource.getInstance();

		testName = getTestName(cmdFile);
		inputDir = util.getArg("-i", args);
		outputDir = util.getArg("-o", args);

		/*
		** If cmdfile doesn't have a path, prepend
		** inputDir
		*/
		cmdFilePath = ((inputDir != null) && (cmdFile.indexOf("/") == -1)) ?
				(inputDir + "/" + cmdFile) : cmdFile;
		try 
		{
			in = new BufferedInputStream(new FileInputStream(cmdFilePath), 
										utilMain.BUFFEREDFILESIZE);
      	} catch (FileNotFoundException e) {
			System.out.println("MultiTest ERROR: config file not found: "+cmdFile);
        	return; 
      	}
		mtGrammar parser = new mtGrammar(in);
		suite = parser.grammarStatement();
		suite.setRoot(inputDir);
		suite.init();
	
		log = openFile(outputDir, testName + ".log");

		try
		{
			seqRunCases(suite.getInitCases(), 
						"initialization", 
						inputDir, 
						outputDir);
		} catch (ijFatalException e) 
		{
			System.exit(1);
		}

		max = suite.getNumThreads();
		System.out.println("...running with "+max+" threads");
		testers = new mtTester[max];

		// create the testers
		for (i = 0; i < max; i++)
		{
			String tester = "Tester" + (i+1);
			try 
			{
				LocalizedOutput out = openFile(outputDir, tester + ".out");
				testers[i] = new mtTester(tester, suite, out, log);
			} catch (IOException e) {
				System.out.println("MultiTest ERROR: unable open output file "+e);
				return;
			}
		}

		long duration = execTesters(testers);

		log.println("");
		log.println("test ran "+duration+" ms");
		log.println("total memory is "+Runtime.getRuntime().totalMemory());
		log.println("free memory  is "+Runtime.getRuntime().freeMemory());
		// Delete the .out files for Testers that did not report errors.
		for (i = 0; i < max; i++)
		{
			if ( testers[i].noFailure() )
			{
				log.println("Deleting " + "Tester" + (i+1) + ".out" + "(" + outputDir + ")");
				File out = new File(outputDir, "Tester" + (i+1) + ".out");
				out.delete();
			}
			else
			{
				log.println("Tester" + (i+1) + " failed.");
			}
		}
        
		System.exit(0);
	}

	/*
	**
	** NOTE ON OUT OF MEMORY PROBLEMS:  in theory 
	** when the VM runs out of memory an OutOfMemoryException
	** should be thrown by the runtime, but unfortunately, that
	** doesn't always seem to be the case.  When running this
	** program the Testers just wind up hanging on memory
	** allocation if there is insufficient memory.  To combat
	** this we try to manually stop each thread, but when
	** there is no memory, this doesn't seem to do anything
	** either.  Also, we grab some memory up front and release
	** that after telling the threads to stop themselves.
	*/
	private static long execTesters(mtTester[] testers)
			throws FileNotFoundException, IOException
	{
		boolean interrupted = false;
		boolean allWereAlive = true;
		int		i;
		long 	duration = 0;
		int 	max = testers.length;
		Thread[] threads;
		byte[] extraMemory;

		// new thread group
		ThreadGroup tg = new ThreadGroup("workers");
		//tg.allowThreadSuspension(false);

		// grab start time
		long start = System.currentTimeMillis();
		long runTime = suite.getTimeMillis();
		System.out.println("...running duration "+suite.getTime());

		// grab some memory to make stopping easier
 		extraMemory = new byte[4096];

		threads = new Thread[max];	
		// run them
		for (i = 0; i < max; i++)
		{
			threads[i] = new Thread(tg, testers[i]);
			threads[i].start();
		}

		// loop sleeping 800ms a bite.
		while (((duration = (System.currentTimeMillis() - start)) < runTime) &&
				(allWereAlive = allAlive(threads)) && (!interrupted))
		{
			try 
			{ 
				Thread.sleep(800L); 
			} catch (InterruptedException e) 
			{ 
				interrupted = true;	
			}
		}

		System.out.println("...stopping testers");


		/*
		** Free up 2k of memory and garbage
		** collect.  That should allow any
		** starved testers to stop themselves.
		*/
		extraMemory = null;
		System.gc();

		/*
		** Now stop everyone. First ask them to
		** willingly stop.  By calling mtTester.stop()
		** we prevent the testers from picking up the
		** next task.  
		*/
		for (i = 0; i < testers.length; i++)
		{
			testers[i].stop();
		}

		/*
		** Sleep 180 seconds, or until everyone
		** is done.
		*/
		System.out.println("...waiting for testers to complete");
		for (i = 0; i < MAX_WAIT_FOR_COMPLETION; i++)
		{
			try 
			{ 
				Thread.sleep((long)1000); 
			} catch (InterruptedException e) 
			{
				System.out.println("...Unexpected InterrupedException: "+e);
			}
			if (allDead(threads))
			{
				break;
			}
		}

		if (i == MAX_WAIT_FOR_COMPLETION)
		{
			log.println("WARNING: testers didn't die willingly, so I'm going to kill 'em.");
			log.println("\tThis may result in connection resources that aren't cleaned up");
			log.println("\t(e.g. you may see problems in the final script run with deadlocks).");
		}
	
		/*	
		** Now stop everyone that hasn't already stopped.
		* First get thread dumps for jdk 15.
		*/
		TestUtil.dumpAllStackTracesIfSupported(log);
		for (i = 0; i < MAX_WAIT_FOR_COMPLETION && (tg.isDestroyed() == false ); i++) 
		{ 

			// can't really stop - deprecated because 'unsafe'. interrupt.
			tg.interrupt();
			try { Thread.sleep((long) 1000); } catch (InterruptedException e) {}

			try 
			{ 
				tg.destroy(); 
			} catch (IllegalThreadStateException e)
			{
				log.println("...waiting for ThreadGroup.interrupt() to work its magic");
				try { Thread.sleep((long)1000); } catch (InterruptedException e2) {}
				continue;
			}
			break;	
		} 

		if (interrupted == true)
		{
			System.out.println("TEST CASE SUMMARY: run interrupted");
		}
		else if (allWereAlive == false)
		{
			System.out.println("TEST CASE SUMMARY: abnormal termination due to error(s)"+
				" -- see test log (./"+testName+"/"+testName+".log) for details ");
		}
		else
		{
			System.out.println("TEST CASE SUMMARY: normal termination");
			if (i < MAX_WAIT_FOR_COMPLETION)
			{
				try
				{
					seqRunCases(suite.getFinalCases(), 
							"last checks", 
							inputDir, 
							outputDir);
				} catch (ijFatalException e)  
				{ 
					System.out.println("...error running final test cases");
				}
			}
			else
			{
				System.out.println("...timed out trying to kill all testers,\n" +
								"   skipping last scripts (if any).  NOTE: the\n"+
								"   likely cause of the problem killing testers is\n"+
								"   probably not enough VM memory OR test cases that\n"+
								"   run for very long periods of time (so testers do not\n"+
								"   have a chance to notice stop() requests");
			}
		}

		return duration;
	}


	/**
	** Search through the list of threads and see
	** if they are all alive.
	*/
	public static boolean allAlive(Thread[] threads)
	{
		int	i;
		for (i = 0; i < threads.length; i++)
		{
			if (threads[i].isAlive() == false)
				break;
		}
		return (i == threads.length);
	}

	/**
	** Search through the list of threads and see
	** if they are all alive.
	*/
	public static boolean allDead(Thread[] threads)
	{
		int	i;
		for (i = 0; i < threads.length; i++)
		{
			if (threads[i].isAlive() == true)
				break;
		}
		return (i == threads.length);
	}

	/**
	** Figure out the name of the log file and open
	** it 
	*/
	private static LocalizedOutput openFile(String dir, String fileName) 
			throws IOException
	{
		
		java.io.File file = new java.io.File(dir, fileName);

		return new LocalizedOutput(new FileOutputStream(file));
	}
	/**
	** Sequentially run scripts
	*/
	private static void seqRunCases(Vector cases, String descr, String inputDir, String outputDir) 
		throws FileNotFoundException, IOException, ijFatalException
	{
		LocalizedOutput	out;
		BufferedInputStream	in;
		mtTestCase		testCase;

		if (cases == null)
		{
			System.out.println("...no "+descr+" being performed");
			return;
		}

		Enumeration e = cases.elements();

		while (e.hasMoreElements())
		{
			testCase = (mtTestCase)e.nextElement();
			String testName = testCase.getFile();
			System.out.println("...running "+descr+" via "+testName);
			String logFileName = 
				testName.substring(0, testName.lastIndexOf('.'));
			out = openFile(outputDir, logFileName + ".out");
			in = testCase.initialize(inputDir);
			testCase.runMe(log, out, in);
		}
	}

	/**
	** Given the command file, infer the test name.
	** Takes the portion of the file name between
	** the last '.' and the last '/'.  e.g.
	** x/y/Name.suffix -> Name
	**
	*/
	private static String getTestName(String cmdFile)
	{
		int slash, dotSpot;

		slash = cmdFile.lastIndexOf("/");
		if (slash == -1)
		{
			slash = 0;
		}

		dotSpot = cmdFile.lastIndexOf(".");
		if (dotSpot == -1)
		{
			dotSpot = cmdFile.length();
		}
		return cmdFile.substring(slash, dotSpot);

	}
}
