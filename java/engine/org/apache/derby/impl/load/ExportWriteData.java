/*

   Derby - Class org.apache.derby.impl.load.ExportWriteData

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

import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.io.IOException;

//this class takes the passed row and writes it into the data file using the
//properties from the control file
//FIXED FORMAT: if length of nullstring is greater than column width, throw execption

final class ExportWriteData extends ExportWriteDataAbstract
	implements java.security.PrivilegedExceptionAction {

  private String outputFileName;
  // i18n support - instead of using DataOutputStream.writeBytes - use
  // OutputStreamWriter.write with the correct codeset.
  private OutputStreamWriter aStream;

  //writes data into the o/p file using control file properties
  public ExportWriteData(String outputFileName, ControlInfo controlFileReader)
  throws Exception {
    this.outputFileName = outputFileName;
    this.controlFileReader = controlFileReader;
    loadPropertiesInfo();

	try {
		java.security.AccessController.doPrivileged(this);
	} catch (java.security.PrivilegedActionException pae) {
		throw pae.getException();
	}

  }

  public final Object run() throws Exception {
	  openFile();
	  return null;
  }

  //prepares the o/p file for writing
  private void openFile() throws Exception {
    try {
      URL url = new URL(outputFileName);
      outputFileName = url.getFile();
    } catch (MalformedURLException ex) {}
    FileOutputStream anOutputStream = new FileOutputStream(outputFileName);
    aStream = new OutputStreamWriter(new BufferedOutputStream(anOutputStream), dataCodeset);
  }

  /**if control file says true for column definition, write it as first line of the
  *  data file
 	* @exception	Exception if there is an error
	*/
  public void writeColumnDefinitionOptionally(String[] columnNames,
  											  String[] columnTypes)
  														throws Exception {
	boolean ignoreColumnTypes=true;

    //do uppercase because the ui shows the values as True and False
    if (columnDefinition.toUpperCase(java.util.Locale.ENGLISH).equals(ControlInfo.INTERNAL_TRUE.toUpperCase(java.util.Locale.ENGLISH))) {
       String tempStr=new String();
       //put the start and stop delimiters around the column name and type
       for (int i=0; i<columnNames.length; i++) {
		 // take care at adding fieldSeparator at the 
		 // end of the field if needed
		 if (i>0) {
			 tempStr=fieldSeparator;
		 } else {
			 tempStr="";
		 }

         tempStr=tempStr+
		 		 fieldStartDelimiter+columnNames[i]+fieldStopDelimiter;
		 if (ignoreColumnTypes==false) {
			 tempStr=tempStr+fieldSeparator+
		 		 fieldStartDelimiter+columnTypes[i]+fieldStopDelimiter;
		 }

         aStream.write(tempStr, 0, tempStr.length());
       }
       aStream.write(recordSeparator, 0, recordSeparator.length());
    }
  }

  //puts the start and stop delimiters only if column value contains field/record separator
  //in it
  private void writeNextColumn(String oneColumn, boolean isNumeric) throws Exception {
    if (oneColumn != null) {
       //put the start and end delimiters always
       //because of the bug 2045, I broke down following
       //aStream.writeBytes(fieldStartDelimiter+oneColumn+fieldStopDelimiter);
       //into 3 writeBytes. That bug had a table with long bit varying datatype and while
       //writing data from that column using the stream, it would run out of memory.
	   // i18n - write using the write method of OutputStreamWriter
       if (!isNumeric)
		   aStream.write(fieldStartDelimiter, 0, fieldStartDelimiter.length());
	   //convert the string to double character delimiters format if requred.
	   if(doubleDelimiter)
		   oneColumn = makeDoubleDelimiterString(oneColumn , fieldStartDelimiter);
	   aStream.write(oneColumn, 0, oneColumn.length());
       if (!isNumeric)
         aStream.write(fieldStopDelimiter, 0, fieldStopDelimiter.length());
    }
  }

  /**write the passed row into the data file
 	* @exception	Exception if there is an error
	*/
  public void writeData(String[] oneRow, boolean[] isNumeric) throws Exception {
    if (format.equals(ControlInfo.DEFAULT_FORMAT)) {
       //if format is delimited, write column data and field separator and then the record separator
       //if a column's value is null, write just the column separator
       writeNextColumn(oneRow[0], isNumeric[0]);
       for (int i = 1; i < oneRow.length; i++) {
         aStream.write(fieldSeparator, 0, fieldSeparator.length());
         writeNextColumn(oneRow[i], isNumeric[i]);
       }
       if (hasDelimiterAtEnd){ //write an additional delimeter if user wants one at the end of each row
          aStream.write(fieldSeparator, 0, fieldSeparator.length());
       }
    }
    aStream.write(recordSeparator, 0, recordSeparator.length());
  }

  /**if nothing more to write, then close the file and write a message of completion
  *  in message file
 	*@exception	Exception if there is an error
	*/
  public void noMoreRows() throws IOException {
    aStream.flush();
    aStream.close();
//    System.err.print(new Date(System.currentTimeMillis()) + " ");
//    System.err.println("Export finished");
//    System.setErr(System.out);
  }


	/*
	 * Convert the input string into double delimiter format for export.
	 * double character delimiter recognition in delimited format
	 * files applies to the export and import utilities. Character delimiters are
	 * permitted within the character-based fields of a file. This applies to
	 * fields of type CHAR, VARCHAR, LONGVARCHAR, or CLOB. Any pair of character
	 * delimiters found between the enclosing character delimiters is imported
	 * into the database. For example with doble quote(") as character delimiter
	 *
	 *	 "What a ""nice""day!"
	 *
	 *  will be imported as:
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
	private String makeDoubleDelimiterString(String inputString , String charDelimiter)
	{
		int start = inputString.indexOf(charDelimiter);
		StringBuffer result;
		//if delimeter is not found inside the string nothing to do
		if(start != -1)
		{
			result = new StringBuffer(inputString);
			int current;
			int delLength = charDelimiter.length();
			while(start!= -1)
			{
				//insert delimter character 
				result = result.insert(start, charDelimiter);
				current = start + delLength +1 ;
				start = result.toString().indexOf(charDelimiter, current);
			}
			return result.toString();
		}
		return inputString;
	}
}

