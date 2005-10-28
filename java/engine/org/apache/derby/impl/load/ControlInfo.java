/*

   Derby - Class org.apache.derby.impl.load.ControlInfo

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

import java.io.PrintStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Properties;

//read the control file properties. If the passed parameter for control file
//name is null, assigns default values to the properties. Also, if the control
//file has message property in it, it sends the errors to that file by
//redirecting system err to that message file
class ControlInfo
{

  static final String ESCAPE = "Escape";
  static final String DEFAULT_ESCAPE = "\\";
  static final String QUOTE = "Quote";
  static final String DEFAULT_QUOTE = "'";
  static final String COMMIT_COUNT = "CommitCount";
  static final String DEFAULT_COMMIT_COUNT = "0";
  static final String START_ROW = "StartRow";
  static final String DEFAULT_START_ROW = "1";
  static final String STOP_ROW = "StopRow";
  static final String DEFAULT_STOP_ROW = "0";

  static final String FIELD_SEPARATOR = "FieldSeparator";
  static final String DEFAULT_FIELD_SEPARATOR = ",";
  static final String RECORD_SEPARATOR = "RecordSeparator";
  static final String DEFAULT_RECORD_SEPARATOR = System.getProperty("line.separator");
  static final String COLUMN_DEFINITION = "ColumnDefinition";
  static final String DEFAULT_COLUMN_DEFINITION = "FALSE";
  static final String NULL_STRING = "Null";
  static final String DEFAULT_NULL_STRING = "NULL";
  static final String FORMAT = "Format";
  static final String DEFAULT_FORMAT = "ASCII_DELIMITED";
  static final String DB2_DELIMITED_FORMAT = "DB2_DELIMITED";  //beetle 5007
  static final String FIELD_START_DELIMITER = "FieldStartDelimiter";
  static final String DEFAULT_FIELD_START_DELIMITER = "\"";
  static final String FIELD_END_DELIMITER = "FieldEndDelimiter";
  static final String DEFAULT_FIELD_END_DELIMITER = "\"";
  static final String COLUMN_WIDTHS = "ColumnWidths";
  static final String MESSAGE_FILE = "MessageFile";
  static final String DEFAULT_VERSION = "1";
  static final String VERSION = "Version";
  static final String NEWLINE = "\n";
  static final String COMMA = ",";
  static final String SPACE = " ";
  static final String TAB = "\t";
  static final String CR = "\r";
  static final String LF = "\n";
  static final String CRLF = "\r\n";
  static final String LFCR = "\n\r";
  static final String FF = "\f";
  static final String EMPTY_LINE = "\n\n";
  static final String SEMICOLON = ";";
  static final String DATA_CODESET = "DataCodeset";
  static final String HAS_DELIMETER_AT_END = "HasDelimeterAtEnd";

  static final String INTERNAL_NONE = "None";
  static final String INTERNAL_TRUE = "True";
  static final String INTERNAL_FALSE = "False";
  static final String INTERNAL_TAB = "Tab";
  static final String INTERNAL_SPACE = "Space";
  static final String INTERNAL_CR = "CR";
  static final String INTERNAL_LF = "LF";
  static final String INTERNAL_CRLF = "CR-LF";
  static final String INTERNAL_LFCR = "LF-CR";
  static final String INTERNAL_COMMA = "Comma";
  static final String INTERNAL_SEMICOLON = "Semicolon";
  static final String INTERNAL_NEWLINE = "New Line";
  static final String INTERNAL_FF = "FF";
  static final String INTERNAL_EMPTY_LINE = "Empty line";

  private Properties currentProperties;

  public ControlInfo() throws Exception  {
    getCurrentProperties();
    //the field and record separators can't be subset of each other
    if (getFieldSeparator().indexOf(getRecordSeparator()) != -1) {
       throw LoadError.fieldAndRecordSeparatorsSubset();
    }
  }

  //read the value of a given property
  String getPropertyValue(String aKey) throws Exception {
    return getCurrentProperties().getProperty(aKey);
   }

  //following are the default values for few of the properties
  private void loadDefaultValues() {
    currentProperties = new Properties();
    currentProperties.put(FIELD_SEPARATOR, DEFAULT_FIELD_SEPARATOR);
    currentProperties.put(RECORD_SEPARATOR, DEFAULT_RECORD_SEPARATOR);
    currentProperties.put(COLUMN_DEFINITION, DEFAULT_COLUMN_DEFINITION);
    currentProperties.put(NULL_STRING, DEFAULT_NULL_STRING);
    currentProperties.put(FORMAT, DEFAULT_FORMAT);
    currentProperties.put(FIELD_START_DELIMITER, DEFAULT_FIELD_START_DELIMITER);
    currentProperties.put(FIELD_END_DELIMITER, DEFAULT_FIELD_END_DELIMITER);
    currentProperties.put(VERSION, DEFAULT_VERSION);
    // set the default code set to the platform default encoding value
    String default_data_codeset =
                          (new InputStreamReader(System.in)).getEncoding();
    currentProperties.put(DATA_CODESET, default_data_codeset);
    currentProperties.put(HAS_DELIMETER_AT_END, INTERNAL_FALSE);
  }

  //get control file version.
  String getCurrentVersion() throws Exception {
    return(DEFAULT_VERSION);
  }

  //2 possible formats: fixed and delimited. default is ASCII_DELIMITED
  String getFormat() throws Exception {
    return(getCurrentProperties().getProperty(FORMAT));
  }

  //read the column widths property which is comma delimited.
  //In case of fixed format, if column widths are missing, it will
  //throw an exception
  int[] getColumnWidths() {
      return null;
  }

  //default is DEFAULT_FIELD_SEPARATOR
  String getFieldSeparator() throws Exception {
    String fieldSeparator = getCurrentProperties().getProperty(FIELD_SEPARATOR);
    fieldSeparator = mapFromUserFriendlyFieldDelimiters(fieldSeparator);
    return fieldSeparator;
  }

  String getFieldStartDelimiter() throws Exception {
    return(getCurrentProperties().getProperty(FIELD_START_DELIMITER));
  }

  String getFieldEndDelimiter() throws Exception {
    return(getCurrentProperties().getProperty(FIELD_END_DELIMITER));
  }

  String getRecordSeparator() throws Exception {
    String recordSeparator = getCurrentProperties().getProperty(RECORD_SEPARATOR);
    recordSeparator = mapFromUserFriendlyRecordDelimiters(recordSeparator);
    return recordSeparator;
  }

  //to be used to cover cases where column delimeters are placed at the end of
  //each column resulting in an extra delimeter at the end of a row.
  boolean getHasDelimiterAtEnd() throws Exception {
    String hasDelimeterAtEnd = getCurrentProperties().getProperty(HAS_DELIMETER_AT_END);
    return hasDelimeterAtEnd.equals(INTERNAL_TRUE);
  }
  String getHasDelimeterAtEndString() throws Exception {
    String hasDelimeterAtEnd = getCurrentProperties().getProperty(HAS_DELIMETER_AT_END);
    return hasDelimeterAtEnd;
  }
  //if at the time of export, the column has null into it, we will spit
  //nullString in the output file.
  //If at the time of import, we see nullString for a column, we will
  //send null as part of resultSet interface
  String getNullString() throws Exception {
    return(getCurrentProperties().getProperty(NULL_STRING));
  }

  //for fixed format, get column definitions
  String getColumnDefinition() throws Exception {
    return(getCurrentProperties().getProperty(COLUMN_DEFINITION));
  }

  private String mapFromUserFriendlyFieldDelimiters(String aDelimiter) {
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_TAB.toUpperCase(java.util.Locale.ENGLISH)))
       return TAB;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_SPACE.toUpperCase(java.util.Locale.ENGLISH)))
       return SPACE;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_CR.toUpperCase(java.util.Locale.ENGLISH)))
       return CR;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_LF.toUpperCase(java.util.Locale.ENGLISH)))
       return LF;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_CRLF.toUpperCase(java.util.Locale.ENGLISH)))
       return CRLF;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_LFCR.toUpperCase(java.util.Locale.ENGLISH)))
       return LFCR;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_COMMA.toUpperCase(java.util.Locale.ENGLISH)))
       return COMMA;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_SEMICOLON.toUpperCase(java.util.Locale.ENGLISH)))
       return SEMICOLON;

    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\n", '\n');
    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\t", '\t');
    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\r", '\r');
    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\f", '\f');
    return aDelimiter;
  }

  //vjbms: when user types \n in vjbms, it comes as 2 characters \ and n
  //and not just one character '\n' That's the reason for the following
  //check. I look for "\n" and replace it with '\n'. Same thing for \t
  // \r and \f
  private String commonToFieldAndRecordDelimiters(String aDelimiter,
  String specialChars, char replacementChar) {
    String beforeSpecialChars;
    String afterSpecialChars;
    int specialCharsPosition;
    while (aDelimiter.indexOf(specialChars) != -1) {
      specialCharsPosition = aDelimiter.indexOf(specialChars);
      beforeSpecialChars = aDelimiter.substring(0,specialCharsPosition);
      afterSpecialChars = aDelimiter.substring(specialCharsPosition+2);
      aDelimiter = beforeSpecialChars + replacementChar + afterSpecialChars;
    }
    return aDelimiter;
  }

  private String mapFromUserFriendlyRecordDelimiters(String aDelimiter) {
    if (aDelimiter.equals("\n"))
       aDelimiter = INTERNAL_NEWLINE;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_NEWLINE.toUpperCase(java.util.Locale.ENGLISH)))
       return NEWLINE;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_CR.toUpperCase(java.util.Locale.ENGLISH)))
       return CR;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_LF.toUpperCase(java.util.Locale.ENGLISH)))
       return LF;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_CRLF.toUpperCase(java.util.Locale.ENGLISH)))
       return CRLF;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_LFCR.toUpperCase(java.util.Locale.ENGLISH)))
       return LFCR;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_FF.toUpperCase(java.util.Locale.ENGLISH)))
       return FF;
    if (aDelimiter.toUpperCase(java.util.Locale.ENGLISH).equals(INTERNAL_EMPTY_LINE.toUpperCase(java.util.Locale.ENGLISH)))
       return EMPTY_LINE;

    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\n", '\n');
    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\t", '\t');
    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\r", '\r');
    aDelimiter = commonToFieldAndRecordDelimiters(aDelimiter, "\\f", '\f');
    return aDelimiter;
  }

  String getDataCodeset() throws Exception {
    return(getCurrentProperties().getProperty(DATA_CODESET));
  }


  /**read the control file properties into a local variable which is used later on
  *In case there is no control file, read the default values for these properties
 	* @exception	Exception if there is an error
	*/
  Properties getCurrentProperties()  throws Exception{
    if (currentProperties == null) {
       loadDefaultValues();
    }
    return currentProperties;
  }



	
	// Following set routines can be used to change the default properties

	public void setColumnWidths(String columnWidths) throws Exception {
		if(columnWidths!=null)
			currentProperties.setProperty(COLUMN_WIDTHS, columnWidths);
	}


	public void setFieldSeparator(String fieldSeperator) throws Exception {
		if(fieldSeperator!=null)
			currentProperties.setProperty(FIELD_SEPARATOR, fieldSeperator);
	}

	public void setFieldStartDelimiter(String fsdl) throws Exception {
		if(fsdl!=null)
			currentProperties.setProperty(FIELD_START_DELIMITER, fsdl);
	}

	public void setFieldEndDelimiter(String fedl) throws Exception {
		if(fedl!=null)
			currentProperties.setProperty(FIELD_END_DELIMITER, fedl);
	}

	public void  setRecordSeparator(String recordSeperator) throws Exception {
		if(recordSeperator!=null)
			currentProperties.setProperty(RECORD_SEPARATOR, recordSeperator);
	}

	public void setHasDelimiterAtEnd(String hasDelimeterAtEnd) throws Exception {
		if(hasDelimeterAtEnd!=null)
			currentProperties.setProperty(HAS_DELIMETER_AT_END, hasDelimeterAtEnd);
	}
  
	public void setNullString(String nullString) throws Exception {
		if(nullString!=null)
			currentProperties.setProperty(NULL_STRING, nullString);
	}

	//for fixed format, set column definitions
	public void setcolumnDefinition(String columnDefinition) throws Exception {
		if(columnDefinition!=null)
			currentProperties.setProperty(COLUMN_DEFINITION, columnDefinition);
	}


	public void setDataCodeset(String codeset) throws Exception {
		if(codeset!=null)
			currentProperties.setProperty(DATA_CODESET, codeset);
	}

	
	public void setCharacterDelimiter(String charDelimiter) throws Exception{
		if(charDelimiter !=null)
		{
			setFieldStartDelimiter(charDelimiter) ;
			setFieldEndDelimiter(charDelimiter);
		}
	}


	
	public void setControlProperties(String characterDelimiter ,
									 String columnDelimiter, 
									 String codeset) throws Exception
	{
		setCharacterDelimiter(characterDelimiter);
		setFieldSeparator(columnDelimiter);
		setDataCodeset(codeset);
		//check whether the delimiters are valid ones
		validateDelimiters();
	}


	private void validateDelimiters() throws Exception
	{


		char colDel = (getFieldSeparator()).charAt(0);
		char charDel = (getFieldStartDelimiter()).charAt(0);

		//The period was specified as a character string delimiter.
		if(charDel == '.')
		{
			throw LoadError.periodAsCharDelimiterNotAllowed();
		}
		
		
		//A delimiter is not valid or is used more than once.
		if(colDel == charDel || 
		   colDel == '.' ||
		   Character.isSpaceChar(colDel) ||  
		   Character.isSpaceChar(charDel)
		   )
		{
			throw LoadError.delimitersAreNotMutuallyExclusive();
		}

	}
}

