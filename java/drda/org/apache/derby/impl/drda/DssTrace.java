/*

   Derby - Class org.apache.derby.impl.drda.DssTrace

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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
package org.apache.derby.impl.drda;

// Generic process and error tracing encapsulation.
// This class also traces a DRDA communications buffer.
// The value of the hex bytes are traced along with
// the ascii and ebcdic translations.
public class DssTrace
{
  // This class was implemented using character arrays to translate bytes
  // into ascii and ebcdic.  The goal was to be able to quickly index into the
  // arrays to find the characters.  Char arrays instead of strings were used as
  // much as possible in an attempt to help speed up performance.
  private static final String LIST_SEPARATOR = " # ";

  // trace representation for a java null.
  private static final String NULL_VALUE = "null";

  // An array of characters used to translate bytes to ascii.
  // The position in the array corresponds to the hex value of the
  // character
  private static final char asciiChar [] = {
    // 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //0
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //1
    ' ','!','"','#','$','%','&','\'','(',')','*','+',',','-','.','/', //2
    '0','1','2','3','4','5','6','7','8','9',':',';','<','=','>','?',  //3
    '@','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O',  //4
    'P','Q','R','S','T','U','V','W','X','Y','Z','[','\\',']','^','_', //5
    '`','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o',  //6
    'p','q','r','s','t','u','v','w','x','y','z','{','|','}','~','.',  //7
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //8
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //9
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //A
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //B
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //C
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //D
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //E
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.'   //F
  };


  // This mapping table associates a codepoint to a String describing the codepoint.
  // This is needed because the trace prints the
  // first codepoint in send and receive buffers.
  // This could be final but there is no need to create the mapping
  // if tracing isn't used.  So... this array will only be created when
  // the com buffer trace is started.  Note this ref is not protected
  // by final and care must be taken if it's value needs to change.
  private static CodePointNameTable codePointNameTable = null;

  // This column position header is used to mark offsets into the trace.
  private static final String colPosHeader =
  "       0 1 2 3 4 5 6 7   8 9 A B C D E F   0123456789ABCDEF  0123456789ABCDEF";

  // An array of characters used to translate bytes to ebcdic.
  // The position in the array corresponds to the hex value of the
  // character.
  private static final char ebcdicChar[] = {
    // 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //0
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //1
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //2
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //3
    ' ','.','.','.','.','.','.','.','.','.','.','.','<','(','+','|',  //4
    '&','.','.','.','.','.','.','.','.','.','!','$','*',')',';','.',  //5
    '-','/','.','.','.','.','.','.','.','.','|',',','%','_','>','?',  //6
    '.','.','.','.','.','.','.','.','.','`',':','#','@','\'','=','"', //7
    '.','a','b','c','d','e','f','g','h','i','.','.','.','.','.','.',  //8
    '.','j','k','l','m','n','o','p','q','r','.','.','.','.','.','.',  //9
    '.','~','s','t','u','v','w','x','y','z','.','.','.','.','.','.',  //A
    '.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',  //B
    '{','A','B','C','D','E','F','G','H','I','.','.','.','.','.','.',  //C
    '}','J','K','L','M','N','O','P','Q','R','.','.','.','.','.','.',  //D
    '\\','.','S','T','U','V','W','X','Y','Z','.','.','.','.','.','.', //E
    '0','1','2','3','4','5','6','7','8','9','.','.','.','.','.','.'   //F
  };


  // An array of characters representing hex numbers.
  private static final char hexDigit [] = {
    '0','1','2','3','4','5','6','7',
    '8','9','A','B','C','D','E','F'
  };


  // A PrintWriter is used in printing the trace.
  private java.io.PrintWriter comBufferWriter = null;


  // The receive header comes befor bytes which would be read from
  // a Stream.
  private static final String receiveHeader =
  "       RECEIVE BUFFER:                     (ASCII)           (EBCDIC)";


  // The send header comes before bytes which would be written to
  // a Stream.
  private static final String sendHeader =
  "       SEND BUFFER:                        (ASCII)           (EBCDIC)";


  // The space character is defined for convience.
  private static final char spaceChar = ' ';


  // This boolean indicates if the trace is on.
  // It has been declared private now but may be made public at
  // a later time.
  private boolean comBufferTraceOn = false;


  // The comBufferSync is an object used for serialization.
  // This separate object is used because this trace code may
  // get eventually placed into another class which performs
  // method entry and exit tracing.  Since each trace may be writing
  // to different logs, separate objects will be used to perform the
  // synchronization.
  private Boolean comBufferSync = new Boolean (true);


  // The zero character is defined for convinience.
  private static final char zeroChar = '0';

  // The recevie constant is used to indicate that the bytes were read to a Stream.
  // It indicates to this class that a receive header should be used.
  protected static final int TYPE_TRACE_RECEIVE = 2;

  // The send constant is used to indicate that the bytes were written to
  // a Stream.  It indicates to this class that a send header should be used.
  protected static final int TYPE_TRACE_SEND = 1;

  // Query if trace is on.
  // This is currently needed since the comBufferTrcOn flag is private.
  protected boolean isComBufferTraceOn()
  {
    // The trace flag indicates if tracing is on.
    return comBufferTraceOn;
  }

  // Start the communications buffer trace.
  // The name of the file to place the trace is passed to this method.
  // After calling this method, calls to isComBufferTraceOn() will return true.
  protected void startComBufferTrace (String fileName)
  {
    synchronized (comBufferSync) {
      try {
        // Only start the trace if it is off.
        if (comBufferTraceOn == false) {
          // The writer will be buffered for effeciency.
          comBufferWriter = new java.io.PrintWriter (new java.io.BufferedWriter (new java.io.FileWriter (fileName), 4096));
          // Turn on the trace flag.
          comBufferTraceOn = true;
          // initialize the codepoint name table if it is null.
          // this is done here so that the CodePointName objects
          // aren't created if the trace isn't used (save some memory).
          // this process should only be done once
          // since after the table is created the ref will
          // no longer be null.
          if (DssTrace.codePointNameTable == null) {
            codePointNameTable = new CodePointNameTable();
          }
        }
      }
      catch (java.io.IOException e) {
        // The IOException is currently ignored.  Handling should be added.
      }
    }
  }

  // Stop the communications buffer trace.
  // The trace file is flushed and closed.  After calling this method,
  // calls to isComBufferTraceOn () will return false.
  protected void stopComBufferTrace ()
  {
    synchronized (comBufferSync) {
      // Only stop the trace if it is actually on.
      if (comBufferTraceOn == true) {
        // Turn of the trace flag.
        comBufferTraceOn = false;
        // Flush and close the writer used for tracing.
        comBufferWriter.flush();
        comBufferWriter.close();
      }
    }
  }

  // Write the communication buffer data to the trace.
  // The data is passed in via a byte array.  The start and length of the data is given.
  // The type is needed to indicate if the data is part of the send or receive buffer.
  // The class name, method name, and trcPt number are also written to the trace.
  // Not much checking is performed on the parameters.  This is done to help performance.
  protected void writeComBufferData (byte[] buff,
                                         int offset,
                                         int len,
                                         int type,
                                         String className,
                                         String methodName,
                                         int trcPt)
  {
    // why don't we synchronize the method!!!

    // Grab the lock to make sure another thread doesn't try to
    // write data or close the writer.
    synchronized (comBufferSync) {

      // Only take action if the trace is on.
      if (comBufferTraceOn) {

        // Obtain an instance of the Calendar so a timestamp can be written.
        // this call seems to slow things down a bit.
        java.util.Calendar time = java.util.Calendar.getInstance();

        // Print the timestamp, class name, method name, thread name, and tracepoint.
        comBufferWriter.println ("       (" +
                                 time.get (java.util.Calendar.YEAR) +
                                 "." +
                                 (time.get (java.util.Calendar.MONTH) + 1) +
                                 "." +
                                 time.get (java.util.Calendar.DAY_OF_MONTH) +
                                 " " +
                                 time.get (java.util.Calendar.HOUR_OF_DAY) +
                                 ":" +
                                 time.get (java.util.Calendar.MINUTE) +
                                 ":" +
                                 time.get (java.util.Calendar.SECOND) +
                                 ") " +
                                 className +
                                 " " +
                                 methodName +
                                 " " +
                                 Thread.currentThread().getName() +
                                 " " +
                                 trcPt);

        // A newline is added for formatting.
        comBufferWriter.println();

        // The data will only be written if there is a non-zero positive length.
        if (len != 0) {
          String codePointName = null;
          // If the length <= 10, lookup the first codepoint so it's name can be printed???
          if (len >= 10) {
            // Get the int value of the two byte unsigned codepoint.
            int codePoint = getCodePoint (buff, offset+8);
            codePointName = codePointNameTable.lookup (codePoint);
          }

          if (codePointName == null) {
            // codePointName was still null so either < 10 bytes were given or
            // the codepoint wasn't found in the table.  Just print the plain send header.
            comBufferWriter.println (getHeader (type));
          }
          else {
            // codePointName isn't null so the name of the codepoint will be printed.
            printHeaderWithCodePointName (codePointName, type);
          }

          // Print the col position header in the trace.
          comBufferWriter.println (colPosHeader);

          // A char array will be used to translate the bytes to their character
          // representations along with ascii and ebcdic representations.
          char trcDump[] = new char[77];

          // bCounter, aCounter, eCounter are offsets used to help position the characters
          short bCounter = 7;
          short aCounter = 43;
          short eCounter = 61;

          // The lines will be counted starting at zero.  This is hard coded since we are
          // at the beginning.
          trcDump[0] = DssTrace.zeroChar;
          trcDump[1] = DssTrace.zeroChar;
          trcDump[2] = DssTrace.zeroChar;
          trcDump[3] = DssTrace.zeroChar;

          // The 0's are already in the trace so bump the line counter up a row.
          int lineCounter = 0x10;

          // Make sure the character array has all blanks in it.
          // Some of these blanks will be replaced later with values.
          // The 0's were not wrote over.
          for (int j = 4; j < 77; j++) {
            trcDump[j] = DssTrace.spaceChar;
          }

          // i will maintain the position in the byte array to be traced.
          int i = 0;

          do {
            // Get the unsigned value of the byte.
            //                  int num = b[off++] & 0xff;
            int num = (buff[offset] < 0)? buff[offset] + 256 : buff[offset]; // jev
            offset++;
            i++;
            // Place the characters representing the bytes in the array.
            trcDump[bCounter++] = DssTrace.hexDigit[((num >>> 4) & 0xf)];
            trcDump[bCounter++] = DssTrace.hexDigit[(num & 0xf)];

            // Place the ascii and ebcdc representations in the array.
            trcDump[aCounter++] = DssTrace.asciiChar[num];
            trcDump[eCounter++] = DssTrace.ebcdicChar[num];

            if (((i%8) == 0)) {
              if (((i%16) == 0)) {
                // Print the array each time 16 bytes are processed.
                comBufferWriter.println (trcDump);
                if (i != len) {
                  // Not yet at the end of the byte array.
                  if ((len - i) < 16) {
                    // This is the last line so blank it all out.
                    // This keeps the last line looking pretty in case
                    // < 16 bytes remain.
                    for (int j = 0; j < trcDump.length; j++) {
                      trcDump[j] = DssTrace.spaceChar;
                    }
                  }
                  // Reset the counters.
                  bCounter = 0;
                  aCounter = 43;
                  eCounter = 61;
                  // Reset the lineCounter if it starts to get too large.
                  if (lineCounter == 0xfff0) {
                    lineCounter = 0;
                  }
                  // Place the characters representing the line counter in the array.
                  trcDump[bCounter++] = DssTrace.hexDigit[((lineCounter >>> 12) & 0xf)];
                  trcDump[bCounter++] = DssTrace.hexDigit[((lineCounter >>> 8) & 0xf)];
                  trcDump[bCounter++] = DssTrace.hexDigit[((lineCounter >>> 4) & 0xf)];
                  trcDump[bCounter++] = DssTrace.hexDigit[(lineCounter & 0xf)];
                  bCounter += 3;
                  // Bump up the line counter.
                  lineCounter += 0x10;
                }
              }
              else {
                // 8 bytes were processed so move the counter to adjust for
                // spaces between the columns of bytes.
                bCounter += 2;
              }
            }
            // do this until we all the data has been traced.
          } while (i < len);

          // print the last line and add some blank lines to make it easier to read.
          if (len % 16 != 0) {
            comBufferWriter.println (trcDump);
          }
          comBufferWriter.println();
          comBufferWriter.println();
        }
        // Flush the writer.
        comBufferWriter.flush();
      }
    }
  }

  // Gets the int value of the two byte unsigned codepoint.
  private static int getCodePoint (byte[] buff, int offset)
  {
    return ((buff[offset++] & 0xff) << 8) +
      ((buff[offset] & 0xff) << 0);
  }

  private static String getHeader (int type)
  {
    switch (type) {
    case DssTrace.TYPE_TRACE_SEND:
      return DssTrace.sendHeader;
    case DssTrace.TYPE_TRACE_RECEIVE:
      return DssTrace.receiveHeader;
    default:
      //  throw new !!!
      return null;
    }
  }

  private static int getStartPosition (int type)
  {
    switch (type) {
    case DssTrace.TYPE_TRACE_SEND:
      return 20; // This is right after 'SEND BUFFER: '.
    case DssTrace.TYPE_TRACE_RECEIVE:
      return 23; // This is right after 'RECEIVE BUFFER: '.
    default:
      //  throw new !!!
      return 0;
    }
  }

  private void printHeaderWithCodePointName (String codePointName, int type)
  {
    // Create a char array so some of the characters
    // can be replaced with the name of the codepoint.
    char headerArray[] = DssTrace.getHeader(type).toCharArray();

    // At most, 16 character name will be used.  This is so
    // the headers on top of the ascii and ebcdic rows aren't shifted.
    int replaceLen = (codePointName.length() < 17) ? codePointName.length() : 16;

    int offset = getStartPosition (type);
    for (int i = 0; i < replaceLen; i++) {
      headerArray[offset++] = codePointName.charAt (i); // make sure charAt() starts at 0!!!
    }
    comBufferWriter.println (headerArray);
  }

}
