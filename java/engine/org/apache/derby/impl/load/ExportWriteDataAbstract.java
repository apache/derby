/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.load
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.load;

abstract class ExportWriteDataAbstract {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
