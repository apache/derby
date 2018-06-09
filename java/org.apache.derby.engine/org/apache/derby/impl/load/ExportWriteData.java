/*

   Derby - Class org.apache.derby.impl.load.ExportWriteData

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

package org.apache.derby.impl.load;

import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.util.Date;
import java.io.IOException;
import java.io.File;

import org.apache.derby.iapi.services.io.FileUtil;

//this class takes the passed row and writes it into the data file using the
//properties from the control file
//FIXED FORMAT: if length of nullstring is greater than column width, throw execption

final class ExportWriteData extends ExportWriteDataAbstract
	implements java.security.PrivilegedExceptionAction<Object> {

	private String outputFileName;
	private String lobsFileName;
	private boolean lobsInExtFile = false;
	private long lobFileOffset = 0;

	// i18n support - instead of using DataOutputStream.writeBytes - use
	// OutputStreamWriter.write with the correct codeset.
	private OutputStreamWriter aStream;
	private OutputStreamWriter lobCharStream;
	private BufferedOutputStream lobOutBinaryStream;
	private ByteArrayOutputStream lobByteArrayStream;

    // temporary buffers userd to read/write the lob data.
    private byte[] byteBuf; 
    private char[] charBuf;

	//writes data into the o/p file using control file properties
	ExportWriteData(String outputFileName, ControlInfo controlFileReader)
		throws Exception {
		this.outputFileName = outputFileName;
		this.controlFileReader = controlFileReader;
		init();
	}
	
	//writes data into the o/p file using control file properties
	ExportWriteData(String outputFileName, 
					String lobsFileName,
					ControlInfo controlFileReader)
		throws Exception {
		this.outputFileName = outputFileName;
		this.lobsFileName = lobsFileName;
		this.controlFileReader = controlFileReader;
		lobsInExtFile = true;
        byteBuf = new byte[8192];
        charBuf = new char[8192];
		init();
	}

	private void init() throws Exception 
	{
		loadPropertiesInfo();
		try {
			java.security.AccessController.doPrivileged(this);
		} catch (java.security.PrivilegedActionException pae) {
			throw pae.getException();
		}

	}

  public final Object run() throws Exception {
	  openFiles();
	  return null;
  }

  //prepares the o/p file for writing
  private void openFiles() throws Exception {

    outputFileName = FileUtil.stripProtocolFromFileName( outputFileName );
    if ( lobsInExtFile ) { lobsFileName = FileUtil.stripProtocolFromFileName( lobsFileName ); }
    
    FileOutputStream anOutputStream = null;
    BufferedOutputStream buffered = null;
    FileOutputStream lobOutputStream = null;

    try {
        File outputFile = new File(outputFileName);
        anOutputStream = new FileOutputStream(outputFileName);
        FileUtil.limitAccessToOwner(outputFile);

        buffered = new BufferedOutputStream(anOutputStream);
    
        aStream = dataCodeset == null ?
    		new OutputStreamWriter(buffered) :
    		new OutputStreamWriter(buffered, dataCodeset);    	        

        // if lobs are exported to an external file, then 
        // setup the required streams to write lob data.
        if (lobsInExtFile) 
        {
            // setup streams to write large objects into the external file. 
            File lobsFile =  new File(lobsFileName);
            if (lobsFile.getParentFile() == null) {
                // lob file name is unqualified. Make lobs file 
                // parent directory is same as the the main export file. 
                // lob file should get created at the same location 
                // as the main export file.
                lobsFile = new File((new File (outputFileName)).getParentFile(),
                                    lobsFileName);

            }

            lobOutputStream = new FileOutputStream(lobsFile);
            FileUtil.limitAccessToOwner(lobsFile);

            lobOutBinaryStream = new BufferedOutputStream(lobOutputStream);

            // helper stream to convert char data to binary, after conversion
            // data is written to lobOutBinaryStream.
            lobByteArrayStream = new ByteArrayOutputStream();
            lobCharStream =  dataCodeset == null ?
                new OutputStreamWriter(lobByteArrayStream) :
                new OutputStreamWriter(lobByteArrayStream, dataCodeset);    	        
        }
    } catch (Exception e) {
        // might have failed to setup export file stream. for example 
        // user has specified invalid codeset or incorrect file path. 
        // close the opened file streams.

        if (aStream == null) {
            if (buffered != null) {
                buffered.close();
            } else {
                if(anOutputStream != null)
                    anOutputStream .close();
            }
        } else {
            // close the main export file stream.
            aStream.close();
            // close the external lob file stream.
            if (lobOutBinaryStream != null) {
                lobOutBinaryStream.close() ;
            } else {
                if (lobOutputStream != null) 
                    lobOutputStream.close();
            }
        }

        // throw back the original exception.
        throw e;
    }
  }

  /**if control file says true for column definition, write it as first line of the
  *  data file
 	* @exception	Exception if there is an error
	*/
  void writeColumnDefinitionOptionally(String[] columnNames,
  											  String[] columnTypes)
  														throws Exception {
	boolean ignoreColumnTypes=true;

    //do uppercase because the ui shows the values as True and False
    if (columnDefinition.toUpperCase(java.util.Locale.ENGLISH).equals(ControlInfo.INTERNAL_TRUE.toUpperCase(java.util.Locale.ENGLISH))) {
       String tempStr="";
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


	
    /**
     * Writes the binary data in the given input stream to an 
     * external lob export file, and return it's location 
     * information in the file as string. Location information 
     * is written in the main export file. 
     * @param istream   input streams that contains a binary column data.
     * @return Location where the column data written in the external file. 
     * @exception Exception  if any error occurs while writing the data.  
     */
    String writeBinaryColumnToExternalFile(InputStream istream) 
        throws Exception
    {
        // read data from the input stream and write it to 
        // the lob export file and also calculate the amount
        // of data written in bytes. 

        long blobSize = 0;
        int noBytes = 0 ;
        if (istream != null ) {
            noBytes = istream.read(byteBuf) ;
            while(noBytes != -1) 
            {
                lobOutBinaryStream.write(byteBuf, 0 , noBytes);
                blobSize += noBytes;
                noBytes = istream.read(byteBuf) ;
            }

            // close the input stream. 
            istream.close();

            // flush the output binary stream. 
            lobOutBinaryStream.flush();
        } else {
            // stream is null, column value must be  SQL NULL.
            // set the size to -1, on import columns will 
            // be interepreted as NULL, filename and offset are 
            // ignored.
            blobSize = -1;
        }
				
        // Encode a lob location information as string. This is 
        // stored in the main export file. It will be used 
        // to retrive this blob data on import. 
        // Format is : <code > <fileName>.<lobOffset>.<size of lob>/ </code>.
        // For a NULL blob, size will be written as -1

        String lobLocation = lobsFileName + "." + 
            lobFileOffset + "." +  blobSize + "/";
		
        // update the offset, this will be  where next 
        // large object data  will be written. 
        if (blobSize != -1)
            lobFileOffset += blobSize;

        return lobLocation;
    }

    /**
     * Writes the clob data in the given input Reader to an 
     * external lob export file, and return it's location 
     * information in the file as string. Location information 
     * is written in the main export file. 
     * @param ir   Reader that contains a clob column data.
     * @return Location where the column data written in the external file. 
     * @exception Exception  if any error occurs while writing the data.   
     */
    String writeCharColumnToExternalFile(Reader ir) 
        throws Exception
    {

        // read data from the input stream and write it to 
        // the lob export file and also calculate the amount
        // of data written in bytes. 

        long clobSize = 0;
        int noChars = 0 ;
        if (ir != null ) {
            noChars = ir.read(charBuf) ;
            while(noChars != -1) 
            {
                // characters data is converted to bytes using 
                // the user specified code set. 
                lobByteArrayStream.reset();
                lobCharStream.write(charBuf, 0 , noChars);
                lobCharStream.flush();
			
                clobSize += lobByteArrayStream.size();
                lobByteArrayStream.writeTo(lobOutBinaryStream);
                noChars  = ir.read(charBuf) ;
            }

            // close the input reader. 
            ir.close();
            // flush the output binary stream. 
            lobOutBinaryStream.flush();
        } else {
            // reader is null, the column value must be  SQL NULL.
            // set the size to -1, on import columns will 
            // be interepreted as NULL, filename and offset are 
            // ignored.
            clobSize = -1;
        }

        // Encode this lob location information as string. This will 
        // be written to the main export file. It will be used 
        // to retrive this blob data on import. 
        // Format is : <code > <fileName>.<lobOffset>.<size of lob>/ </code>.
        // For a NULL blob, size will be written as -1
        String lobLocation = lobsFileName + "." + 
            lobFileOffset + "." +  clobSize + "/";

        // update the offset, this will be  where next 
        // large object data  will be written. 		
        if (clobSize != -1)
            lobFileOffset += clobSize;
        return lobLocation;
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
    if (lobsInExtFile) {
        // close the streams associated with lob data.
        if (lobOutBinaryStream != null) {
            lobOutBinaryStream.flush();
            lobOutBinaryStream.close();
        }
        if (lobCharStream != null) 
            lobCharStream.close();
        if (lobByteArrayStream != null)
            lobByteArrayStream.close();
    }
    
//    System.err.print(new Date(System.currentTimeMillis()) + " ");
//    System.err.println("Export finished");
//    System.setErr(System.out);
  }


	/**
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

