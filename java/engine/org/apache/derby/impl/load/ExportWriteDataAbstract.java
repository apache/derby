/*

   Derby - Class org.apache.derby.impl.load.ExportWriteDataAbstract

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

abstract class ExportWriteDataAbstract {

  protected ControlInfo controlFileReader;
  protected int[] columnLengths;

  protected String fieldSeparator;
  protected String recordSeparator;
  protected String nullString;
  protected String columnDefinition;
  protected String format;
  protected String fieldStartDelimiter;
  protected String fieldStopDelimiter;
  protected String dataCodeset;
  protected String dataLocale;
  protected boolean hasDelimiterAtEnd;
  protected boolean doubleDelimiter=true;

  //load properties locally for faster reference to them periodically
  protected void loadPropertiesInfo() throws Exception {
    fieldSeparator = controlFileReader.getFieldSeparator();
    recordSeparator = controlFileReader.getRecordSeparator();
    nullString = controlFileReader.getNullString();
    columnDefinition = controlFileReader.getColumnDefinition();
    format = controlFileReader.getFormat();
    fieldStartDelimiter = controlFileReader.getFieldStartDelimiter();
    fieldStopDelimiter = controlFileReader.getFieldEndDelimiter();
    dataCodeset = controlFileReader.getDataCodeset();
    hasDelimiterAtEnd = controlFileReader.getHasDelimiterAtEnd();
  }

  //if control file says true for column definition, write it as first line of the
  //data file
  public abstract void writeColumnDefinitionOptionally(String[] columnNames,
  													   String[] columnTypes)
  											throws Exception;

  //used in case of fixed format
  public void setColumnLengths(int[] columnLengths) {
    this.columnLengths = columnLengths;
  }

  //write the passed row into the data file
  public abstract void writeData(String[] oneRow, boolean[] isNumeric) throws Exception;

  //if nothing more to write, then close the file and write a message of completion
  //in message file
  public abstract void noMoreRows() throws Exception;
}
