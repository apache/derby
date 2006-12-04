/*
   Derby - Class LocCompare

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


import java.io.*;
import java.util.ArrayList;

/**
 * Program that attempts to flag the derby i18n properties files for a variety
 * of possible and actual problems.
 * For syntax, see USAGE string ( obtained with -h)
 * For further info, see readme file 
 *
 */
public class LocCompare {

	private static StringBuffer strbuf;
	private static boolean interesting;
	
    private static String USAGE=
    	"USAGE: \n java \n  [-Dderbysvntop=<svntop>] [-Dtranslations=<newloc>] LocCompare [<territories>][?|-h]\n" +
    	"  where \n" +
    	"     svntop      = top of derby svn tree for branch or trunk \n" +
    	"                   default is current dir is the top\n" +
    	"     newloc      = temporary location for translated files \n" +
    	"                   drda locale files are expected in a subdir 'drda'\n" +
    	"                   default is same file structure as for the english files\n" +
    	"     territories = one translated territories, or 'all' \n" +
    	"                   where all means: \n" +
    	"                      {cs,de_DE,es,fr,hu,it,ja_JP,ko_KR,pl,pt_BR,ru,zh_CN,zh_TW}\n" +
    	"     ?|-h        = you can pass on -h or ? to get this message\n" +
    	"\n" +
    	"   you can also pass -Dtvtdebug=true in to see more comments.";
	
	public static void main(String[] args) {
		// some args checking and usage.
		String curdir = System.getProperty("user.home");
		String svntop = curdir;
		String locnewlocfiles = curdir;
		String languages[] = {"cs","de_DE","es","fr","hu","it","ja_JP","ko_KR","pl","pt_BR","ru","zh_CN","zh_TW"};
		if (args.length == 0) // no arguments, will assume currentdir for loc files
			curdir = System.getProperty("user.home");
		else if ((args.length >2) || (args[0].equals("?") || (args[0].startsWith("-h"))))
		{
			System.out.println(USAGE);
			return;
		}
		else if ((args.length==1) && (!args[0].equals("all")))
		{
			languages = new String[1];
			languages[0] = args[0];
		}
		
		if (System.getProperty("derbysvntop") == null )
			chatterMsg("assuming start from current dir - if not, run with -h for info\n");
		else
			svntop = System.getProperty("derbysvntop");
	
		boolean temporaryfiles = false;	
		if (System.getProperty("translations") == null )
		{
			chatterMsg("comparing files in same locations as english message files\n");
		}	
		else
		{
			temporaryfiles = true;
			locnewlocfiles = System.getProperty("translations");
		}
		
		// making assumptions about the paths and filenames
		String ext = ".properties";
		String[] typefiles = {"messages_","sysinfoMessages_","toolsmessages_"};
		String[] drdatypefiles = {"messages_","servlet_"};
		
		String englishPath = svntop + "/java/engine/org/apache/derby/loc/";
		String english = "en";
		String englishDrdaPath = svntop + "/java/drda/org/apache/derby/loc/drda/";
		String englishToolsPath = svntop + "/java/tools/org/apache/derby/loc/";
	
		String forLangPath = svntop + "/java/engine/org/apache/derby/loc/";
		String forDrdaPath = svntop + "/java/drda/org/apache/derby/loc/drda/";
		String forToolsPath = svntop + "/java/tools/org/apache/derby/loc/";
		if ( temporaryfiles )	
		{
			forLangPath = locnewlocfiles + "/";
			forDrdaPath = locnewlocfiles + "/drda/";
			forToolsPath = locnewlocfiles + "/";
		}
		
		String EnglishFileName;
		String ForeignFileNameString;
		String ForeignFileName="";

		// first find the apprioriate embedded messages
		for (int i=0; i< typefiles.length; i++)
		{
			if (( typefiles[i].equals("sysinfoMessages_")) || (typefiles[i].equals("toolsmessages_")))
			{
				EnglishFileName=englishToolsPath + typefiles[i].substring(0,(typefiles[i].length()-1)) + ext;
				if (checkExistsFile(EnglishFileName) == false)
				{	
					System.out.println(" English file does not exist: \n  " + EnglishFileName);
					continue;
				}
				ForeignFileNameString=forToolsPath + typefiles[i];
			}
			else	
			{
				EnglishFileName=englishPath + typefiles[i] + english + ext;
				if (checkExistsFile(EnglishFileName) == false)
				{
					System.out.println(" English file does not exist: \n  " + EnglishFileName);
					continue;
				}
				ForeignFileNameString=forLangPath + typefiles[i];
			}
			for (int j=0; j < languages.length; j++)
			{
				String ForeignFileNametmp = ForeignFileNameString + languages[j] + ext;
				if (checkExistsFile(ForeignFileNametmp) == false)
				{
					System.out.println(" Translated file does not exist: \n  " + ForeignFileNametmp);
					continue;
				}
				ForeignFileName=ForeignFileNametmp;
				System.out.println("********************************************* ");
				System.out.println("********************************************* ");
				System.out.println("Now comparing \n < " + EnglishFileName +
						"\n > " + ForeignFileName);
				compare(EnglishFileName, ForeignFileName, languages[j]);
			}
		}
		
		// now compare the drda messages
		for (int i=0; i< drdatypefiles.length; i++)
		{
			EnglishFileName=englishDrdaPath + drdatypefiles[i] + english + ext;
			if (checkExistsFile(EnglishFileName) == false)
			{
				System.out.println(" English file does not exist: \n  " + EnglishFileName);
				continue;
			}
			for (int j=0; j < languages.length; j++)
			{
				ForeignFileName=forDrdaPath + drdatypefiles[i] + languages[j] + ext;
				if (checkExistsFile(ForeignFileName) == false)
				{
					System.out.println(" Translated file does not exist: \n  " + ForeignFileName);
					continue;
				}
				System.out.println(" ********************************************* ");
				System.out.println(" ********************************************* ");
				System.out.println("Now comparing \n < " + EnglishFileName +
						"\n > " + ForeignFileName);
				compare(EnglishFileName, ForeignFileName, languages[j]);
			}
		}
	}
	
	public static void compare(String englishFileName, String foreignFileName, String langcode){
		String openBrace="{";
		String closeBrace="}";
		String apostrophe="'";
		
				try {
			BufferedReader englishR = new BufferedReader(
					new InputStreamReader(new FileInputStream(englishFileName), "UTF8"));
			BufferedReader foreignR = new BufferedReader(
					new InputStreamReader(new FileInputStream(foreignFileName), "UTF8"));
			
			int i=0;
			String englishStr;
			String foreignStr;
			
			while ((englishStr = englishR.readLine())!=null)
			{
				i++;
				interesting = false;
				// first position English on a message
				// note that this means we're only checking the first
				// line of each message...After that, we can't be sure
				// how long the messages are. 
				// An improvement would be to string all text found after a line
				// ending in '\' together and compare the whole thing. 
				// But the readLine reads \ lines as null...
				
				if ((englishStr.indexOf("=") < 0) ||
					(englishStr.indexOf("#")>=0)   || 
					(englishStr.indexOf("user=usr")>0)) // may be url syntax
					continue;
				String englishError = "";
				String foreignError = "";
				
				englishError = englishStr.substring(0,englishStr.indexOf("="));				
				
				foreignStr = lookForForeignErrorString(foreignR, englishError, langcode);
				if (foreignStr == null)
				{
					logMsg("  ===============");
					String spacingforformat = "";
					if (langcode.length()>2)
						spacingforformat = "   ";
					logMsg("  " + spacingforformat + "en: < " + englishStr);
					logMsg("  " + langcode + ": > ------- No translation found");
					interesting = true;
					// reset foreign reader to top
					foreignR = new BufferedReader(
						new InputStreamReader(new FileInputStream(foreignFileName), "UTF8"));	
					continue;
				}
				else
				{
					// theoretically, we should now be at the same error.
					//chatterMsg("\tMessage: " + englishError + "(en), found match") ;
					logMsg("  ===============");
					String spacingforformat = "";
					if (langcode.length()>2)
						spacingforformat = "   ";
					logMsg("  " + spacingforformat + "en: < " + englishStr);
					logMsg("  " + langcode + ": > " + foreignStr);
					
					checkISO8559(foreignStr);
				
					// just for fun, compare occurrences of some unusual characters:				
					int count = countAndCompareCountCharacter(
						englishStr, foreignStr, "%", englishError);
					// let's not check for , it is a language specific construct
					// count = countAndCompareCountCharacter(
					//		englishStr, foreignStr, ",", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, ";", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, ":", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "_", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "=", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "(", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, ")", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "+", englishError);
					// let's not check for -, it is a language specific construct
					// count = countAndCompareCountCharacter(
					//		englishStr, foreignStr, "-", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "/", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "]", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "[", englishError);
					count = countAndCompareCountCharacter(
						englishStr, foreignStr, "\"", englishError);	
				
					// check to see if the errorstring has a {
					int countOpen = countAndCompareCountCharacter(
						englishStr, foreignStr, openBrace, englishError);
					// then check to see if the errorstring has a } 
					int countClose = countAndCompareCountCharacter(
						englishStr, foreignStr, closeBrace, englishError);
					if ((countOpen <0) || (countClose <0))
					{
						// we have a mismatch between the languages should already have seen an error
						continue;
					}			
					if (countOpen != countClose)
					{	
						logMsg("\t FAILURE!!! - unmatched braces");
						interesting = true;
						continue;
					}
					// now check that the parameter numbers encircled by the braces are matching
					compareParameterSequenceNumbers(englishStr, foreignStr, countOpen, englishError);

					// now, for the toolsmessages files, if we do not have replacements,
					// in theory there is no need for double single quotes. 
					// For all other files, single quotes need to be doubled
					// So, check for apostropes
					if ((englishFileName.indexOf("tools")>0) ||
						(foreignFileName.indexOf("tools")>0))
					{
						if ((countOpen == 0) || (countClose == 0))
						{
							lookAtSingleQuotes(englishStr, apostrophe, englishError, englishFileName, "in English String");
							lookAtSingleQuotes(foreignStr, apostrophe, foreignError, foreignFileName, "in translated String");
						}
						else if ((countOpen == countClose) && (countOpen > 0))
						{
							// except for strings with replacements in toolsmessages files
							// all apostrophes *must* get doubled.
							if ((englishStr.indexOf(apostrophe) < 0) && (foreignStr.indexOf(apostrophe) < 0))
							{
								continue;
							}
							else
							{
								lookAtDoubleQuotes(englishStr, apostrophe, englishError, englishFileName, "in English String");
								lookAtDoubleQuotes(foreignStr, apostrophe, foreignError, foreignFileName, "in translated String");
							}
						}
					}
					else // just check that the quotes are doubled
					{
						lookAtDoubleQuotes(englishStr, apostrophe, englishError, englishFileName, "in English String");
						lookAtDoubleQuotes(foreignStr, apostrophe, foreignError, foreignFileName, "in translated String");
					}
					// last automated check is to find strings
					// with all capitals and compare....
					compareUpperCaseStrings(englishStr, foreignStr, englishError, foreignFileName);
					
					if ((strbuf!=null) && (strbuf.length() >0) && interesting)
						System.out.println(strbuf);
					strbuf=null;
					
				}
			}
		} catch (UnsupportedEncodingException e) {
		} catch (IOException e) {
		}
	}
	
	/**
	 * 
	 * @param englishStr
	 * @param foreignStr
	 * @param Character
	 * @return 0 if the character does not occur at all
	 *         -1 if there is a difference between English and translation
	 *         #>0 indicating the number of occurrences of the character
	 */
	public static int countAndCompareCountCharacter(
			String englishStr, String foreignStr, String Character, String englishError)
	{
		if (englishStr.indexOf(Character)< 0)
			return 0;
		else { // (if character exists)
			//then compare the number of occurrences per line
			int noCharE=0;
			int noCharF=0;
			noCharE = countCharacter(englishStr, Character);
			noCharF = countCharacter(foreignStr, Character);
			if (noCharE!=noCharF)
			{
				logMsg("\t WARNING - not the same number of the character " + Character);
				interesting = true;
				return -1;
			}
			else
			{	
				chatterMsg("\t\tsame number of the character " + Character + ", namely, :" + noCharE);
				return noCharE;
			}
		}
	}
	
	/**
	 * 
	 * @param englishStr
	 * @param foreignStr
	 * @param Character
	 * @return 0 if the character does not occur at all
	 *         -1 if there is a difference between English and translation
	 *         #>0 indicating the number of occurrences of the character
	 */
	public static int countCharacter(String Str, String Character)
	{
		if (Str.indexOf(Character)< 0)
			return 0;
		else { // (if character exists)
		//then compare the number of occurrences per line
			String tmpstr = Str;
			int noChar=0;
			for (int k=0; k<Str.length() ; k++){
				if (tmpstr.indexOf(Character) >= 0)
					noChar++;
					tmpstr = tmpstr.substring(tmpstr.indexOf(Character)+1);
			}
			return noChar;
		}
	}
	
	public static void compareParameterSequenceNumbers(
			String englishStr, String foreignStr, int NumberOfParameters, String englishError)
	{
		for ( int i=0 ; i < NumberOfParameters; i++)
		{
			int eindex1 = englishStr.indexOf("{");
			int findex1 = foreignStr.indexOf("{");
			int eindex2 = englishStr.indexOf("}");
			int findex2 = foreignStr.indexOf("}");
			String englishSubStr1 = englishStr.substring(eindex1+1, eindex2);
			String foreignSubStr1 = foreignStr.substring(findex1+1, findex2);
			chatterMsg("\t\tcomparing english parameter or string substr: " + englishSubStr1 + " with translated substr " + foreignSubStr1);
			if (!englishSubStr1.equals(foreignSubStr1))
			{
				logMsg("\t WARNING - not the same parameter or string in brackets");
				interesting = true;
			}
			englishStr=englishStr.substring(eindex2+1);
			foreignStr=foreignStr.substring(findex2+1);
		}
	}	
	
	/**
	 * check that strings without replacements only have
	 * single quotes 
	 * Note that we're passing in 'Character' but it's really only
	 * thought about for quotes.
	 * If the apostrophes are doubled, flag a warning.
	 * Note that it may still be ok, we just want to know.
	 * This is only relevant in the toolsmessages files.	
	 * 
	 */
	private static void lookAtSingleQuotes (
			String Str, String Character, String Error, String FileName, String print ) 
	{
		String tmpStr = Str;
		int countOfCharacter = countCharacter(tmpStr, Character);
		//chatterMsg("\t\t\tcountOfCharacter for " + Character + " is: " + countOfCharacter);
		if (countOfCharacter < 0)
			return;
		for (int m=0 ; m < countOfCharacter ; m++)
		{
			int index1 = Str.indexOf("'");
			String SubStr1 = Str.substring(index1 +1);
			int index2 = SubStr1.indexOf("'");
			if (index2 < 0)  
				return; // we're done
			if (index2 == 0) // it *is* right after!
			{
				logMsg("\t WARNING - double quotes in String without replacements");
				interesting = true;
			}
			else
				chatterMsg("\t\t" + print + " found: " + countOfCharacter + " single quotes");
			tmpStr = SubStr1;
		}
	}

	/**
	 * check that strings have doubled quotes.
	 * Note that we're passing in 'Character' but it's really only
	 * thought about for quotes/apostrophes.
	 * If the apostrophes are single FAIL, not OK. (except for tools)	
	 * Note that we already know there *are* quotes in the string
	 * 
	 */
	private static void lookAtDoubleQuotes (
			String Str, String Character, String Error, String FileName, String print ) 
	{
		int countOfCharacter = countCharacter(Str, Character);
		if (countOfCharacter % 2 == 1)
		{
			logMsg("\t WARNING - found single single quotes - quotes need to be doubled");
			interesting = true;
		}
		else
			chatterMsg(" \t\t" + print + " found: " + (countOfCharacter / 2) + " double quotes");
	}
	
	public static void compareUpperCaseStrings(String englishStr, String foreignStr, String Error, String FileName)
	{
		// first check to see if there are any strings with 
		// more than one uppercase.
		ArrayList englishArray = findUpperCaseStrings(englishStr);
		ArrayList foreignArray = findUpperCaseStrings(foreignStr);
		if ((englishArray == null) && (foreignArray==null))
		{
			chatterMsg("no such character in string");
			return;
		}
		else if ((englishArray == null) || (foreignArray == null))
		{
			logMsg("\t FAILURE!!! - not the same number of Uppercase strings, one has none, the other something");
			interesting = true;
			return;
		}
		if (englishArray.size() != foreignArray.size())
		{
			logMsg("\t FAILURE!!! - not the same number of Uppercase strings");
			interesting = true;
		}
		else
		{
			for (int i=0; i < englishArray.size(); i++)
			{
				if (!englishArray.get(i).equals(foreignArray.get(i)))
				{
					logMsg("\t WARNING - difference in Uppercase strings");
					interesting = true;
				}
				else 
					chatterMsg("\t\tsuccessfully compared " + englishArray.get(i));
			}
		}
	}
	
	/**
	 * 
	 * Find uppercase strings in a string passed in
	 */
	public static ArrayList findUpperCaseStrings(String Strin)
	{
		ArrayList StrArr = new ArrayList(); // for out
		StringBuffer buf = new StringBuffer();

		int length = Strin.length();
		for ( int upperIdx = 0 ; upperIdx < length ; ++upperIdx )
		{
			char ch = Strin.charAt( upperIdx );
			if ((buf == null) || (buf.length() == 0))
			{
				buf = new StringBuffer();
				if (Character.isUpperCase(ch))
					buf.append(ch);
			}
			else if (buf.length() == 1)
			{
				if (Character.isUpperCase(ch))
					buf.append(ch);
				else 
					buf = null; // never mind
			}
			else
			{
				if (Character.isUpperCase(ch))
					buf.append(ch);
				else
				{
					StrArr.add(buf.toString());
					buf = null;
				}
			}
		}
		return StrArr;
	}
	
	/**
	 * Check for characters in the range 0x00-0x1f (which are ASCII) and 0x7f-0xff
	 * If found, suggest native2ascii modification of file
	 */
	private static void checkISO8559(String lineRead){
		int numchars = lineRead.length();
		for (int i = 0 ; i < numchars ; i++) 
		{
		      int c = lineRead.charAt(i);
		      if (((c >= 0x0000) && (c <= 0x1F)) || ((c >= 0x7F) && c <= 0XFF ))
		      {
		    	  logMsg("\t FAILURE: encountered non-ISO8559-1 character");
		    	  logMsg("\t please run: native2ascii -encoding UTF-8 on this file");
		    	  interesting = true;
		      }
		}
	}
	
	
	/**
	 * Check that the file exists
	 */
	private static boolean checkExistsFile(String FileName){
		//chatterMsg(" FileName: " + FileName);
		File File = new File(FileName);
		if (!File.exists())
			return false;
		else 
			return true;
	}
	
	private static String lookForForeignErrorString (
    		BufferedReader foreignR, String englishError, String langcode)
    throws IOException
    {	
    	String foreignError="";
		String foreignStr="";
		while (true)
		{
			// some messages have only a \, which will be a null string
			// If this is the case, try to grab a next line. 
			// If that too is null, bail out.
			if (foreignStr == null)
			{
				foreignStr = foreignR.readLine();
				// if still null, we must really be at the end.
				if (foreignStr == null)
				{
					// Assume we Reached EOF
					return null;
				}
			}
			else if ((foreignStr.indexOf("=")>0) &&
				 (foreignStr.indexOf("#")!=0) &&
				 (!foreignStr.trim().equals("")))
			{
				foreignError = foreignStr.substring(0,foreignStr.indexOf("="));
				if (foreignError.equals(englishError))
				{
					return foreignStr;
				}
			}
			foreignStr = foreignR.readLine();
		}
	}
    
	/**
	 * Write message to the standard output.
	 */
	private static void logMsg(String str)	{
		if (strbuf == null)
			strbuf = new StringBuffer(str + "\n");
		else
			strbuf.append(str + "\n");
	}

	/**
	 * Write more messages to the standard output if property tvtdebug is true.
	 */
	private static void chatterMsg(String str)	{
	 	String debug = System.getProperty("tvtdebug");
	   	if ((debug!=null) && (debug.equals("true")))
	   	{
	   		interesting = true;
	   		if (strbuf == null)
				strbuf = new StringBuffer(str + "\n");
			else
				strbuf.append(str + "\n");
	   	}
	}
}
