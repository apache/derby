/*

   Derby - Class org.apache.derby.impl.load.ImportReadData

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.load;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.derby.iapi.services.sanity.SanityManager;

final class ImportReadData implements java.security.PrivilegedExceptionAction {
  //Read data from this file
  private String inputFileName;

  private int[] columnWidths;
  private int rowWidth;
  private char[] tempString;
  private int numberOfCharsReadSoFar;

  //temporary variables
  private BufferedReader bufferedReader;

  //temporary variable which holds each token as we are building it.
  private static final int START_SIZE = 10240;
  private char[] currentToken = new char[START_SIZE];
  private int currentTokenMaxSize = START_SIZE;

  //This tells whether to look for a matching stop pattern
  boolean foundStartDelimiter;
  int totalCharsSoFar;
  //following is used to ignore whitespaces in the front
  int positionOfNonWhiteSpaceCharInFront;
  //following is used to ignore whitespaces in the back
  int positionOfNonWhiteSpaceCharInBack;
  int lineNumber;
  int fieldStartDelimiterIndex;
  int fieldStopDelimiterIndex;
  int stopDelimiterPosition;
  boolean foundStartAndStopDelimiters;

  //in the constructor we open the stream only if it's delimited file to find out
  //number of columns. In case of fixed, we know that already from the control file.
  //then we close the stream. Now the stream is reopened when the first record is
  //read from the file(ie when the first time next is issued. This was done for the
  //bug 1032 filed by Dan
  boolean streamOpenForReading;

  static final int DEFAULT_FORMAT_CODE = 0;
  static final int ASCII_FIXED_FORMAT_CODE = 1;
  private int formatCode = DEFAULT_FORMAT_CODE;
  private boolean hasColumnDefinition;
  private char recordSeparatorChar0;
  private char fieldSeparatorChar0;
  private boolean recordSepStartNotWhite = true;
  private boolean fieldSepStartNotWhite = true;

  //get properties infr from following
  protected ControlInfo controlFileReader;

  //Read first row to find out how many columns make up a row and put it in
  //the following variable
  protected int numberOfColumns;
 
  // the types of the columns that we are about to read
  protected String [] columnTypes;
  
  //Read control file properties and write it in here
  protected char[] fieldSeparator;
  protected int fieldSeparatorLength;
  protected char[] recordSeparator;
  protected int recordSeparatorLength;
  protected String nullString;
  protected String columnDefinition;
  protected String format;
  protected String dataCodeset;
  protected char[] fieldStartDelimiter;
  protected int fieldStartDelimiterLength;
  protected char[] fieldStopDelimiter;
  protected int fieldStopDelimiterLength;
  protected boolean hasDelimiterAtEnd;
  

  //load the control file properties info locally, since we need to refer to them
  //all the time while looking for tokens
  private void loadPropertiesInfo() throws Exception {
    fieldSeparator = controlFileReader.getFieldSeparator().toCharArray();
    fieldSeparatorLength = fieldSeparator.length;
    recordSeparator = controlFileReader.getRecordSeparator().toCharArray();
    recordSeparatorLength = recordSeparator.length;
    nullString = controlFileReader.getNullString();
    columnDefinition = controlFileReader.getColumnDefinition();
    format = controlFileReader.getFormat();
    dataCodeset = controlFileReader.getDataCodeset();
    fieldStartDelimiter = controlFileReader.getFieldStartDelimiter().toCharArray();
    fieldStartDelimiterLength = fieldStartDelimiter.length;
    fieldStopDelimiter = controlFileReader.getFieldEndDelimiter().toCharArray();
    fieldStopDelimiterLength = fieldStopDelimiter.length;
    hasDelimiterAtEnd = controlFileReader.getHasDelimiterAtEnd();

    // when record or field separators start with typical white space,
    // we can't ignore it around values in the import file.  So set up
    // a boolean so we don't keep re-testing for it.
    if (recordSeparatorLength >0) {
      recordSeparatorChar0=recordSeparator[0];
      recordSepStartNotWhite = (Character.isWhitespace(recordSeparatorChar0)==false);
    }
    if (fieldSeparatorLength >0) {
      fieldSeparatorChar0=fieldSeparator[0];
      fieldSepStartNotWhite = (Character.isWhitespace(fieldSeparatorChar0)==false);
    }
  }
  //inputFileName: File to read data from
  //controlFileReader: File used to interpret data in the inputFileName
  public ImportReadData(String inputFileName, ControlInfo controlFileReader)
  throws Exception {
    this.inputFileName = inputFileName;
    this.controlFileReader = controlFileReader;

    //load the control file properties info locally, since we need to refer to
    //them all the time while looking for tokens
    loadPropertiesInfo();
    //read the first row to find how many columns make a row and then save that
    //column information for further use
    loadMetaData();
  }

  //just a getter returning number of columns for a row in the data file
  public int getNumberOfColumns() {
    return numberOfColumns;
  }
  /**if columndefinition is true, ignore first row. The way to do that is to just
  *  look for the record separator
 	* @exception	Exception if there is an error
	*/
  protected void ignoreFirstRow() throws Exception {
    readNextToken(recordSeparator, 0, recordSeparatorLength, true);
  }

  /** load the column types from the meta data line to be analyzed
    * later in the constructor of the ImportResultSetMetaData.
	*/
  protected void loadColumnTypes() throws Exception {
    int idx;
    String [] metaDataArray;

    // start by counting the number of columns that we have at the
    // meta data line
    findNumberOfColumnsInARow();

    // reopen the file to the start of the file to read the actual column types data
	closeStream();
	openFile();

    // make room for the meta data
    metaDataArray=new String [numberOfColumns];

    // read the meta data line line - meta data is always in a delimited format
    readNextDelimitedRow(metaDataArray);

    // allocate space for the columnTypes  meta data
    // since the meta data line contains a combination of column name and
    // column type for every column we actually have only half the number of
    // columns that was counted.
    columnTypes=new String[numberOfColumns/2];

    for(idx=0 ; idx<numberOfColumns ; idx=idx+2) {
      columnTypes[idx/2]=metaDataArray[idx+1];
    }


    // reopen to the start of the file so the rest of the program will
    // work as expected
	closeStream();
	openFile();

    // init the numberOfColumns variable since it is
    // being accumulate by the findNumberOfColumnsInARow method
    numberOfColumns=0;
  }

  private void openFile() throws Exception {
	try {
		java.security.AccessController.doPrivileged(this);
	} catch (java.security.PrivilegedActionException pae) {
		throw pae.getException();
	}
  }

  public final Object run() throws Exception {
	  realOpenFile();
	  return null;
  }

  //open the input data file for reading
  private void realOpenFile() throws Exception {
    try {
      try {
        URL url = new URL(inputFileName);
        if (url.getProtocol().equals("file")) { //this means it's a file url
           inputFileName = url.getFile(); //seems like you can't do openstream on file
           throw new MalformedURLException(); //so, get the filename from url and do it ususal way
        }
        InputStream inputStream =  url.openStream();
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream, dataCodeset));
      } catch (MalformedURLException ex) {
        InputStreamReader inputFileStreamReader = new InputStreamReader(new FileInputStream(inputFileName), dataCodeset);
        bufferedReader = new BufferedReader(inputFileStreamReader, 32*1024);
      }
    } catch (FileNotFoundException ex) {
      throw LoadError.dataFileNotFound(inputFileName);
    } catch (SecurityException se) {
		java.sql.SQLException sqle = LoadError.dataFileNotFound(inputFileName);

		sqle.setNextException(new java.sql.SQLException("XJ001", se.getMessage(), 0));

		throw sqle;
	}
    streamOpenForReading = true;
  }

  //read the first data row to find how many columns make a row and then save that
  //column information for future use
  private void loadMetaData() throws Exception {
    //open the input data file for reading the metadata information
    openFile();
    // if column definition is true, ignore the first row since that's not
    // really the data do uppercase because the ui shows the values as True
    // and False
    if (columnDefinition.toUpperCase(java.util.Locale.ENGLISH).equals(ControlInfo.INTERNAL_TRUE.toUpperCase(java.util.Locale.ENGLISH))) {
      hasColumnDefinition = true;
      ignoreFirstRow();
    }

    if (formatCode == DEFAULT_FORMAT_CODE) {
      findNumberOfColumnsInARow();
    }
    closeStream();
  }

  /**close the input data file
 	* @exception	Exception if there is an error
	*/
  public void closeStream() throws Exception {
    if (streamOpenForReading) {
       bufferedReader.close();
       streamOpenForReading = false;
    }
  }

  //actually looks at the data file to find how many columns make up a row
  public int findNumberOfColumnsInARow() throws Exception {
    // init the number of columns to 1 - no such thing as a table
    // without columns
    numberOfColumns=1;
    while (! readTokensUntilEndOfRecord() ) {
      numberOfColumns++;
    }
    //--numberOfColumns;
    //what shall we do if there is delimeter after the last column?
    //reducing the number of columns seems to work fine.

    //this is necessary to be able to read delimited files that have a delimeter
    //at the end of a row.
    if (hasDelimiterAtEnd){
        --numberOfColumns;
    }

    // a special check - if the imported file is empty then
    // set the number of columns to 0
    if (numberOfCharsReadSoFar==0) {
      numberOfColumns=0;
    }
    return numberOfColumns;
  }

  //keep track of white spaces in the front. We use positionOfNonWhiteSpaceCharInFront for
  //that. It has the count of number of white spaces found so far before any non-white char
  //in the token.
  //Look for whitespace only if field start delimiter is not found yet. Any white spaces
  //within the start and stop delimiters are ignored.
  //Also if one of the white space chars is same as recordSeparator or fieldSeparator then
  //disregard it.
  private void checkForWhiteSpaceInFront() {
    //if found white space characters so far, the following if will be true
    if ((positionOfNonWhiteSpaceCharInFront + 1) == totalCharsSoFar &&
        ((!foundStartDelimiter) && (!foundStartAndStopDelimiters) )) {
       char currentChar = currentToken[positionOfNonWhiteSpaceCharInFront];
       if (//currentChar == '\t' ||
           //currentChar == '\r' || alc: why isn't this included?
		// alc: BTW, \r and \n should be replaced
		// or amended with the first char of line.separator...
           //currentChar == '\n' ||
           //currentChar == ' ') {
           // use String.trim()'s definition of whitespace.
		   // i18n - check for whitespace - avoid doing a hard coded character
		   // check and use the isWhitespace method to cover all the Unicode
		   // options
		   Character.isWhitespace(currentChar) == true) {

             if ((recordSepStartNotWhite || (currentChar != recordSeparatorChar0))
                  &&
                 (fieldSepStartNotWhite || (currentChar != fieldSeparatorChar0)))
             //disregard if whitespace char is same as separator first char
                positionOfNonWhiteSpaceCharInFront++;
       }
    }
  }


  //look for white spaces from the back towards the stop delimiter position.
  //If there was no startdelimite & stopdelimiter combination, then we start from the back
  //all the way to the beginning and stop when we find non-white char
  //positionOfNonWhiteSpaceCharInBack keeps the count of whitespaces at the back
  private void checkForWhiteSpaceInBack() {
    boolean onlyWhiteSpaceSoFar = true;
    positionOfNonWhiteSpaceCharInBack = 0;

    for (int i = totalCharsSoFar; (i > stopDelimiterPosition) && onlyWhiteSpaceSoFar; i--) {
       char currentChar = currentToken[i];
	// replace test on \t,\n,' ' with String.trim's definition of white space
	   // i18n - check for whitespace - avoid doing a hard coded character
	   // check and use the isWhitespace method to cover all the Unicode
	   // options
       if (Character.isWhitespace(currentChar)==true) {

             if ((recordSepStartNotWhite || (currentChar != recordSeparatorChar0))
                  &&
                 (fieldSepStartNotWhite || (currentChar != fieldSeparatorChar0)))
             //disregard if whitespace char is same as separator first char
                positionOfNonWhiteSpaceCharInBack++;
       } else
         onlyWhiteSpaceSoFar = false;
    }
  }

  //keep looking for field and record separators simultaneously because we don't yet
  //know how many columns make up a row in this data file. Stop as soon as we get
  //the record separator which is indicated by a return value of true from this function
  public boolean readTokensUntilEndOfRecord() throws Exception {
    int nextChar;
    int fieldSeparatorIndex = 0;
    int recordSeparatorIndex = 0;

    fieldStopDelimiterIndex =  0;
    fieldStartDelimiterIndex =  0;
    totalCharsSoFar = 0;
    //at the start of every new token, make white space in front count 0
    positionOfNonWhiteSpaceCharInFront = 0;
    foundStartDelimiter = false;
    foundStartAndStopDelimiters = false;
    numberOfCharsReadSoFar = 0;

    while (true) {
      nextChar = bufferedReader.read();
      if (nextChar == -1)
         return true;
      numberOfCharsReadSoFar++;
      //read the character into the token holder. If token holder reaches it's capacity,
      //double it's capacity
      currentToken[totalCharsSoFar++] = (char)nextChar;
      //check if character read is white space char in front
      checkForWhiteSpaceInFront();
      if (totalCharsSoFar == currentTokenMaxSize) {
        currentTokenMaxSize = currentTokenMaxSize * 2;
        char[] tempArray = new char[currentTokenMaxSize];
        System.arraycopy(currentToken, 0, tempArray, 0, totalCharsSoFar);
        currentToken = tempArray;
      }

      //see if we can find fieldSeparator
      fieldSeparatorIndex = lookForPassedSeparator(fieldSeparator, 
												   fieldSeparatorIndex, 
												   fieldSeparatorLength,
												   nextChar, false);
      //every time we find a column separator, the return false will indicate that count
      //this token as column data value and keep lookin for more tokens or record
      //separator
      if (fieldSeparatorIndex == -1)
         return false;

      //if found start delimiter, then don't look for record separator, just look for
      //end delimiter
      if (!foundStartDelimiter ) {
         //see if we can find recordSeparator
         recordSeparatorIndex = lookForPassedSeparator(recordSeparator, recordSeparatorIndex,
           recordSeparatorLength, nextChar, true);
         if (recordSeparatorIndex == -1)
            return true;
      }
    }
  }

  //if not inside a start delimiter, then look for the delimiter passed
  //else look for stop delimiter first.
  //this routine returns -1 if it finds field delimiter or record delimiter
  private int lookForPassedSeparator(char[] delimiter, int delimiterIndex,
									 int delimiterLength, int nextChar,  
									 boolean lookForRecordSeperator) throws
									 IOException
	{

    //foundStartDelimiter will be false if we haven't found a start delimiter yet
    //if we haven't found startdelimiter, then we look for both start delimiter
    //and passed delimiter(which can be field or record delimiter). If we do find
    //start delimiter, then we only look for stop delimiter and not the passed delimiter.
    if (!foundStartDelimiter ) {
       //look for start delimiter only if it's length is non-zero and only if haven't already
       //found it at all so far.
       if (fieldStartDelimiterLength != 0 && (!foundStartAndStopDelimiters) ) {
          //the code inside following if will be executed only if we have gone past all the
          //white characters in the front.
          if (totalCharsSoFar != positionOfNonWhiteSpaceCharInFront &&
              (totalCharsSoFar - positionOfNonWhiteSpaceCharInFront) <= fieldStartDelimiterLength) {
             //After getting rid of white spaces in front, look for the start delimiter. If
             //found, set foundStartDelimiter flag.
             if (nextChar == fieldStartDelimiter[fieldStartDelimiterIndex]){
                fieldStartDelimiterIndex++;
                if (fieldStartDelimiterIndex == fieldStartDelimiterLength) {
                   foundStartDelimiter = true;
                   //since characters read so far are same as start delimiters, discard those chars
                   totalCharsSoFar = 0;
                   positionOfNonWhiteSpaceCharInFront = 0;
                   return 0;
                }
             } else {
                //found a mismatch for the start delimiter
                //see if found match for more than one char of this start delimiter before the
                //current mismatch, if so check the remaining chars agains
                //eg if stop delimiter is xa and data is xxa
                if (fieldStartDelimiterIndex > 0) {
                   reCheckRestOfTheCharacters(totalCharsSoFar-fieldStartDelimiterIndex,
                   fieldStartDelimiter, fieldStartDelimiterLength);
                }
             }
          }
       }

	   /*look for typical record seperators line feed (\n),  a carriage return
		* (\r) or a carriage return followed by line feed (\r\n)
		*/
	   if(lookForRecordSeperator)
	   {
		   if(nextChar == '\r' || nextChar == '\n')
		   {
			   recordSeparatorChar0 = (char) nextChar;
			   if(nextChar == '\r' )
			   {
				   //omot the line feed character if it exists in the stream
				   omitLineFeed();
			   }

			   totalCharsSoFar = totalCharsSoFar - 1 ;
			   return -1;
		   }

		   return delimiterIndex;
	   }

       //look for passed delimiter
       if (nextChar == delimiter[delimiterIndex]) {
          delimiterIndex++;
          if (delimiterIndex == delimiterLength) { //found passed delimiter
             totalCharsSoFar = totalCharsSoFar - delimiterLength;
             return -1;
          }
          return delimiterIndex; //this number of chars of delimiter have exact match so far
       } else {
         //found a mismatch for the delimiter
         //see if found match for more than one char of this delimiter before the
         //current mismatch, if so check the remaining chars agains
         //eg if delimiter is xa and data is xxa
         if (delimiterIndex > 0)
            return(reCheckRestOfTheCharacters(totalCharsSoFar-delimiterIndex,
		delimiter,
            	delimiterLength));
       }
    } else {
      //see if we can find fieldStopDelimiter
      if (nextChar == fieldStopDelimiter[fieldStopDelimiterIndex]) {
         fieldStopDelimiterIndex++;
         if (fieldStopDelimiterIndex == fieldStopDelimiterLength) {
			 boolean skipped = 	skipDoubleDelimiters(fieldStopDelimiter);
			 if(!skipped)
			 {
				 foundStartDelimiter = false;
				 //found stop delimiter, discard the chars corresponding to stop delimiter
				 totalCharsSoFar = totalCharsSoFar - fieldStopDelimiterLength;
				 //following is to take care of a case like "aa"aa This will result in an
				 //error. Also a case like "aa"   will truncate it to just aa
				 stopDelimiterPosition = totalCharsSoFar;
				 //following is used to distinguish between empty string ,"", and null string ,,
				 foundStartAndStopDelimiters = true;
			 }else
			 {
				 fieldStopDelimiterIndex =0 ; 
			 }
            return 0;
         }
         return 0;
      } else {
         //found a mismatch for the stop delimiter
         //see if found match for more than one char of this stop delimiter before the
         //current mismatch, if so check the remaining chars agains
         //eg if stop delimiter is xa and data is xxa
        if (fieldStopDelimiterIndex > 0) {
            reCheckRestOfTheCharacters(totalCharsSoFar-fieldStopDelimiterIndex,
            fieldStopDelimiter, fieldStopDelimiterLength);
            return 0;
        }
      }
    }
    return 0;
  }

  //If after finding a few matching characters for a delimiter, find a mismatch,
  //restart the matching process from character next to the one from which you
  //were in the process of finding the matching pattern
  private int reCheckRestOfTheCharacters(int startFrom,
         char[] delimiter, int delimiterLength) {
    int delimiterIndex =  0;
    // alc: need to test delim of abab with abaabab
    // if delimIndex resets to 0, i probably needs to reset to
    // (an ever increasing) startFrom=startFrom+1, not stay where it is
    for (int i = startFrom; i<totalCharsSoFar; i++) {
        if (currentToken[i] == delimiter[delimiterIndex])
           delimiterIndex++;
        else
         delimiterIndex =  0;
    }
    return delimiterIndex;
  }

	/*
	 * skips the duplicate delimeter characters inserd character stringd ata 
	 * to get the original string. In Double Delimter recognigation Delimiter 
	 * Format strings are written with a duplicate delimeter if a delimiter is
	 * found inside the data while exporting.
	 * For example with double quote(") as character delimiter
	 *
	 *	 "What a ""nice""day!"
	 *
	 *   will be imported as:
	 *
	 *	 What a "nice"day!
	 *
	 *	 In the case of export, the rule applies in reverse. For example,
	 *
	 *	 I am 6"tall.
	 *
	 *	 will be exported to a file as:
	 *
	 *	 "I am 6""tall."
 	 */
	private boolean skipDoubleDelimiters(char [] characterDelimiter) throws IOException
	{ 
		boolean skipped = true;
		int cDelLength = characterDelimiter.length ;
		bufferedReader.mark(cDelLength);
		for(int i = 0 ; i < cDelLength ; i++)
		{
			int nextChar = bufferedReader.read();
			if(nextChar != characterDelimiter[i])
			{
				//not a double delimter case
				bufferedReader.reset();
				skipped = false;
				break;
			}
		}
		return skipped;
	}



	//omit the line feed character(\n) 
	private void omitLineFeed() throws IOException
	{ 
		bufferedReader.mark(1);
		int nextChar = bufferedReader.read();
		if(nextChar != '\n')
		{
			//not a Line Feed
			bufferedReader.reset();
		}
	}



  /**returns the number of the current row
  */
  public int getCurrentRowNumber() {
    return lineNumber;
  }

  /**the way we read the next row from input file depends on it's format
 	* @exception	Exception if there is an error
	*/
  public boolean readNextRow(String[] returnStringArray) throws Exception {
    boolean readVal;
    int idx;

    if (!streamOpenForReading) {
       openFile();
       //as earlier, ignore the first row if it's colum definition
       //do uppercase because the ui shows the values as True and False
       if (hasColumnDefinition){
          ignoreFirstRow();
	   }
    }
    if (formatCode == DEFAULT_FORMAT_CODE)
       readVal=readNextDelimitedRow(returnStringArray);
    else
       readVal=readNextFixedRow(returnStringArray);

    return readVal;
  }

	// made this a field so it isn't inited for each row, just
	// set and cleared on the rows that need it (the last row
	// in a file, typically, so it isn't used much)

	private boolean haveSep = true;
  //read the specified column width for each column
  private boolean readNextFixedRow(String[] returnStringArray) throws Exception {
    // readLength is how many bytes it has read so far
    int readLength = 0;
    int totalLength = 0;

    // keep reading until rolWidth bytes have been read
    while ((readLength +=
      bufferedReader.read(tempString, readLength,
                 rowWidth-readLength))
        < rowWidth) {

      if (readLength == totalLength-1) {// EOF
         if ( readLength == -1) { // no row, EOF
           return false;
         }
         else {
            // it's only a bad read if insufficient data was
            // returned; missing the last record separator is ok
            if (totalLength != rowWidth - recordSeparator.length) {
              throw LoadError.unexpectedEndOfFile(lineNumber+1);
            }
            else {
              haveSep = false;
              break;
            }
         }
      }
      // else, some thing is read, continue until the whole column is
      // read
      totalLength = readLength;
    }

	 int colStart = 0;
     for (int i=0; i< numberOfColumns; i++) {
  		 int colWidth = columnWidths[i];

       if (colWidth == 0) //if column width is 0, return null
          returnStringArray[i] = null;
       else {
          // if found nullstring, return it as null value
          String checkAgainstNullString = new String(tempString, colStart, colWidth);
          if (checkAgainstNullString.trim().equals(nullString))
             returnStringArray[i] = null;
          else
              returnStringArray[i] = checkAgainstNullString;
          colStart += colWidth;
       }
     }

     //if what we read is not recordSeparator, throw an exception
     if (haveSep) {
        for (int i=(recordSeparatorLength-1); i>=0; i--) {
            if (tempString[colStart+i] != recordSeparator[i])
               throw LoadError.recordSeparatorMissing(lineNumber+1);
        }
     } else haveSep = true; // reset for the next time, if any.

     lineNumber++;
     return true;
  }

  //by this time, we know number of columns that make up a row in this data file
  //so first look for number of columns-1 field delimites and then look for record
  //delimiter
  private boolean readNextDelimitedRow(String[] returnStringArray) throws Exception {

    int upperLimit = numberOfColumns-1; //reduce # field accesses

    //no data in the input file for some reason
    if (upperLimit < 0)
       return false;

    //look for number of columns - 1 field separators
    for (int i = 0; i<upperLimit; i++) {
      if (!readNextToken(fieldSeparator, 0, fieldSeparatorLength, false) ) {
        if (i == 0) // still on the first check
          return false;
        else
          throw LoadError.unexpectedEndOfFile(lineNumber+1);
      }
      //following is to take care of a case like "aa"aa This will result in an
      //error. Also a case like "aa"   will truncate it to just aa. valid blank
      //chars are  ' ' '\r' '\t'
      if (stopDelimiterPosition!=0 && ((stopDelimiterPosition) != totalCharsSoFar)) {
        for (int k=stopDelimiterPosition+1; k<totalCharsSoFar; k++) {
          // alc: should change || to && since || case is never true --
          // currentChar can't be three different things at once.
          // alc: why no \n? BTW, \r and \n should be replaced
          // or amended with the first char of line.separator...
                  //char currentChar = currentToken[k];
                  //if (currentChar != ' ' && currentChar != '\r' && currentChar != '\t')
                  // use String.trim()'s definition of whitespace.
          // i18n - check for whitespace - avoid doing a hard coded
          // character check and use the isWhitespace method to cover all
          // the Unicode options
          if (Character.isWhitespace(currentToken[k])==false) {
              throw LoadError.dataAfterStopDelimiter(lineNumber+1, i+1);
          }
        }
        totalCharsSoFar = stopDelimiterPosition;
      }
      //totalCharsSoFar can become -1 in readNextToken
      if (totalCharsSoFar != -1) {
        returnStringArray[i] = new String(currentToken,
                      positionOfNonWhiteSpaceCharInFront, totalCharsSoFar);
      }
      else
         returnStringArray[i] = null;
    }

    //look for record separator for the last column's value
    //if I find endoffile and the it's only one column table, then it's a valid endoffile
    //case. Otherwise, it's an error case. Without the following check for the return value
    //of readNextToken, import was going into infinite loop for a table with single column
    //import. end-of-file was getting ignored without the following if.
    if (!readNextToken(recordSeparator, 0, recordSeparatorLength, true) ) {
       if (upperLimit == 0)
          return false;
       else
          throw LoadError.unexpectedEndOfFile(lineNumber+1);
    }
    //following is to take care of a case like "aa"aa This will result in an
    //error. Also a case like "aa"   will truncate it to just aa. valid blank
    //chars are  ' ' '\r' '\t'
    if (stopDelimiterPosition!=0 && (stopDelimiterPosition != totalCharsSoFar)) {
      for (int i=stopDelimiterPosition+1; i<totalCharsSoFar; i++) {
        // alc: should change || to && since || case is never true --
        // currentChar can't be three different things at once.
        // alc: why no \n? BTW, \r and \n should be replaced
        // or amended with the first char of line.separator...
        //char currentChar = currentToken[i];
        //if (currentChar != ' ' && currentChar != '\r' && currentChar != '\t')
        // use String.trim()'s definition of whitespace.
        // i18n - check for whitespace - avoid doing a hard coded character
        // check and use the isWhitespace method to cover all the Unicode
        // options
        if (Character.isWhitespace(currentToken[i])==false) {
          throw LoadError.dataAfterStopDelimiter(lineNumber+1, numberOfColumns);
        }
      }
      totalCharsSoFar = stopDelimiterPosition;
    }

    //to be able to read delimited files that have a delimeter at the end,
    //we have to reduce totalCharsSoFar by one when it is last column.
    //Otherwise last delimeter becomes part of the data.
    if (hasDelimiterAtEnd) {
      if (!(fieldStopDelimiterLength > 0)) { //if there is no field stop delimeter specified,
                                              //hopefully fieldStopDelimiterLength will not be >0

        //there is weird behavior in the code that makes it read the last
        //delimeter as part of the last column data, so this forces us to
        //reduce number of read chars only if there is data stop delimeter

        //Only if it is the last column:
        //if (fieldStopDelimiter==null){
          --totalCharsSoFar;
        //}
      }
    }

    if (totalCharsSoFar != -1) {

      /* This is a hack to fix a problem: When there is missing data in columns
      and hasDelimiterAtEnd==true, then the last delimiter was read as the last column data.
      Hopefully this will tackle that issue by skipping the last column which is in this case
      just the delimiter.
      We need to be careful about the case when the last column data itself is
      actually same as the delimiter.
      */
      if (!hasDelimiterAtEnd) {//normal path:
        returnStringArray[upperLimit] = new String(currentToken,
                          positionOfNonWhiteSpaceCharInFront, totalCharsSoFar);
      }
      else if (totalCharsSoFar==fieldSeparatorLength && isFieldSep(currentToken) ){
        //means hasDelimiterAtEnd==true and all of the above are true

        String currentStr = new String(currentToken,
                          positionOfNonWhiteSpaceCharInFront, totalCharsSoFar);

        if (currentToken[totalCharsSoFar+1]==fieldStopDelimiter[0]){
          returnStringArray[upperLimit] = currentStr;
        }
        else {
          returnStringArray[upperLimit] = null;
        }
      }
      else {
        //means hasDelimiterAtEnd==true and previous case is wrong.
        if (totalCharsSoFar>0) {
          returnStringArray[upperLimit] = new String(currentToken,
                            positionOfNonWhiteSpaceCharInFront, totalCharsSoFar);
        }
        else{
          returnStringArray[upperLimit] = null;
        }
      }
    }
    else
      returnStringArray[upperLimit] = null;

    lineNumber++;
    return true;
  }
  //tells if a char array is field separator:
  private boolean isFieldSep(char[] chrArray){
    for (int i=0; i<chrArray.length && i<fieldSeparatorLength; i++){
      if (chrArray[i]!=fieldSeparator[i])
        return false;
    }
    return true;
  }
  //read one column's value at a time
  public boolean readNextToken(char[] delimiter, int delimiterIndex,
							   int delimiterLength, 
							   boolean isRecordSeperator) throws Exception {
    int nextChar;

    fieldStopDelimiterIndex =  0;
    fieldStartDelimiterIndex =  0;
    totalCharsSoFar = 0;
    //at the start of every new token, make white space in front count 0
    positionOfNonWhiteSpaceCharInFront = 0;
    stopDelimiterPosition = 0;
    foundStartAndStopDelimiters = false;
    foundStartDelimiter = false;
    int returnValue;

    while (true) {
      nextChar = bufferedReader.read();
      if (nextChar == -1) //end of file
         return false;

      //read the character into the token holder. If token holder reaches it's capacity,
      //double it's capacity
      currentToken[totalCharsSoFar++] = (char)nextChar;
      //check if character read is white space char in front
      checkForWhiteSpaceInFront();
      if (totalCharsSoFar == currentTokenMaxSize) {
        currentTokenMaxSize = currentTokenMaxSize * 2;
        char[] tempArray = new char[currentTokenMaxSize];
        System.arraycopy(currentToken, 0, tempArray, 0, totalCharsSoFar);
        currentToken = tempArray;
      }

      returnValue = lookForPassedSeparator(delimiter, delimiterIndex,
										   delimiterLength, nextChar, 
										   isRecordSeperator);
      if (returnValue == -1) {
         //if no stop delimiter found that "" this means null
         //also if no stop delimiter found then get rid of spaces around the token
         if (!foundStartAndStopDelimiters ) {
            if (totalCharsSoFar == 0)
               totalCharsSoFar = -1;
            else {
               //get the count of white spaces from back and subtract that and white spaces in
               //the front from the characters read so far so that we ignore spaces around the
               //token.
               checkForWhiteSpaceInBack();
               totalCharsSoFar = totalCharsSoFar - positionOfNonWhiteSpaceCharInFront - positionOfNonWhiteSpaceCharInBack;
            }
         }
         return true;
      }
      delimiterIndex = returnValue;
    }
  }
}






