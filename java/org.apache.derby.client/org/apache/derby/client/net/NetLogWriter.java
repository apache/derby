/*

   Derby - Class org.apache.derby.client.net.NetLogWriter

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

package org.apache.derby.client.net;

// network traffic tracer.

import java.io.PrintWriter;
import org.apache.derby.client.am.ClientConnection;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.BasicClientDataSource;

// This class traces communication buffers for both sends and receives.
// The value of the hex bytes are traced along with the ascii and ebcdic translations.

public class NetLogWriter extends LogWriter {

    // The recevie constant is used to indicate that the bytes were read to a Stream.
    // It indicates to this class that a receive header should be used.
    static final int TYPE_TRACE_RECEIVE = 2;

    // The send constant is used to indicate that the bytes were written to
    // a Stream.  It indicates to this class that a send header should be used.
    static final int TYPE_TRACE_SEND = 1;

    //------------------------------ internal constants --------------------------

    // This class was implemented using character arrays to translate bytes
    // into ascii and ebcdic.  The goal was to be able to quickly index into the
    // arrays to find the characters.  Char arrays instead of strings were used as
    // much as possible in an attempt to help speed up performance.

    // An array of characters used to translate bytes to ascii.
    // The position in the array corresponds to the hex value of the character.
    private static final char asciiChar__ [] = {
        // 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //0
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //1
        ' ', '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', //2
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?', //3
        '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', //4
        'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '[', '\\', ']', '^', '_', //5
        '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', //6
        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~', '.', //7
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //8
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //9
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //A
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //B
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //C
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //D
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //E
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'   //F
    };

    // This column position header is used to mark offsets into the trace.
    private static final String colPosHeader__ =
            "       0 1 2 3 4 5 6 7   8 9 A B C D E F   0123456789ABCDEF  0123456789ABCDEF";

    // An array of characters used to translate bytes to ebcdic.
    // The position in the array corresponds to the hex value of the
    // character.
    private static final char ebcdicChar__[] = {
        // 0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //0
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //1
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //2
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //3
        ' ', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '<', '(', '+', '|', //4
        '&', '.', '.', '.', '.', '.', '.', '.', '.', '.', '!', '$', '*', ')', ';', '.', //5
        '-', '/', '.', '.', '.', '.', '.', '.', '.', '.', '|', ',', '%', '_', '>', '?', //6
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '`', ':', '#', '@', '\'', '=', '"', //7
        '.', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', '.', '.', '.', '.', '.', '.', //8
        '.', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', '.', '.', '.', '.', '.', '.', //9
        '.', '~', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '.', '.', '.', '.', '.', '.', //A
        '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', //B
        '{', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', '.', '.', '.', '.', '.', '.', //C
        '}', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', '.', '.', '.', '.', '.', '.', //D
        '\\', '.', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '.', '.', '.', '.', '.', '.', //E
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', '.', '.', '.', '.', '.'   //F
    };

    // An array of characters representing hex numbers.
    private static final char hexDigit__ [] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    // The receive header comes befor bytes which would be read from a stream.
    private static final String receiveHeader__ =
            "       RECEIVE BUFFER:                     (ASCII)           (EBCDIC)";

    // The send header comes before bytes which would be written to a stream.
    private static final String sendHeader__ =
            "       SEND BUFFER:                        (ASCII)           (EBCDIC)";

    private static final char spaceChar__ = ' ';

    private static final char zeroChar__ = '0';

    // This mapping table associates a codepoint to a String describing the codepoint.
    // This is needed because the trace prints the first codepoint in send and receive buffers.
    // This is created lazily because there is no need to create the mapping if tracing isn't used.
    // So this array will only be created when the com buffer trace is started.
    private static CodePointNameTable codePointNameTable__ = null;

    //-----------------------------internal state---------------------------------

    //-----------------------------constructors/finalizer-------------------------

    // One NetLogWriter object is created per data source, iff tracing is enabled.
    public NetLogWriter(PrintWriter printWriter, int traceLevel) {
        super(printWriter, traceLevel);

        // Initialize the codepoint name table if not previously initialized.
        // This is done lazily so that it is not created if the trace isn't used (save some init time).

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        if (codePointNameTable__ == null) {
            initCodePointTable();
        }
    }

    // synchonized so only one thread can initialize the table
    private synchronized void initCodePointTable() {
        if (codePointNameTable__ == null) {
            codePointNameTable__ = new CodePointNameTable();
        }
    }

    //------------------------------entry points----------------------------------

    // Specialization of LogWriter.traceConnectsExit()
    public void traceConnectsExit(ClientConnection connection) {
        NetConnection c = (NetConnection) connection;
        synchronized (printWriter_) {
            super.traceConnectsExit(c);
            dncnetprint("  PROTOCOL manager levels: { ");
            printWriter_.print("SQLAM=" + c.getSQLAM() + ", ");
            printWriter_.print("AGENT=" + c.getAGENT() + ", ");
            printWriter_.print("CMNTCPIP=" + c.getCMNTCPIP() + ", ");
            printWriter_.print("RDB=" + c.getRDB() + ", ");
            printWriter_.print("SECMGR=" + c.getSECMGR() + ", ");
            printWriter_.print("XAMGR=" + c.getXAMGR() + ", ");
            printWriter_.print("SYNCPTMGR=" + c.getSYNCPTMGR() + ", ");
            printWriter_.print("RSYNCMGR=" + c.getRSYNCMGR());
            printWriter_.println(" }");
            printWriter_.flush();
        }
    }

    public void traceConnectsResetExit(ClientConnection connection) {
        NetConnection c = (NetConnection) connection;
        synchronized (printWriter_) {
            super.traceConnectsResetExit(c);
            dncnetprint("  PROTOCOL manager levels: { ");
            printWriter_.print("SQLAM=" + c.getSQLAM() + ", ");
            printWriter_.print("AGENT=" + c.getAGENT() + ", ");
            printWriter_.print("CMNTCPIP=" + c.getCMNTCPIP() + ", ");
            printWriter_.print("RDB=" + c.getRDB() + ", ");
            printWriter_.print("SECMGR=" + c.getSECMGR() + ", ");
            printWriter_.print("XAMGR=" + c.getXAMGR() + ", ");
            printWriter_.print("SYNCPTMGR=" + c.getSYNCPTMGR() + ", ");
            printWriter_.print("RSYNCMGR=" + c.getRSYNCMGR());
            printWriter_.println(" }");
            printWriter_.flush();
        }
    }

    // Pass the connection handle and print it in the header
    // What exactly is supposed to be passed,  assume one complete DSS packet
    // Write the communication buffer data to the trace.
    // The data is passed in via a byte array.  The start and length of the data is given.
    // The type is needed to indicate if the data is part of the send or receive buffer.
    // The class name, method name, and trcPt number are also written to the trace.
    // Not much checking is performed on the parameters.  This is done to help performance.
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    synchronized void traceProtocolFlow(byte[] buff,
                                               int offset,
                                               int len,
                                               int type,
                                               String className,
                                               String methodName,
                                               int tracepoint) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        if (!loggingEnabled(BasicClientDataSource.TRACE_PROTOCOL_FLOWS)) {
            return;
        }
        synchronized (printWriter_) {
            tracepoint("[net]", tracepoint, className, methodName);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

            int fullLen = len;
            boolean printColPos = true;
            while (fullLen >= 2) { // format each DssHdr seperately
                // get the length of this DssHdr
                len = ((buff[offset] & 0xff) << 8) + (buff[offset + 1] & 0xff);

                // check for valid dss header or not all of dss block
                if ((len < 10) || (len > fullLen)) {
                    len = fullLen;
                }

                // subtract that length from the full length
                fullLen -= len;
                // The data will only be written if there is a non-zero positive length.
                if (len != 0) {
                    String codePointName = null;
                    // If the length <= 10, lookup the first codepoint so it's name can be printed
                    if (len >= 10) {
                        // Get the int value of the two byte unsigned codepoint.
                        int codePoint = getCodePoint(buff, offset + 8);
                        codePointName = codePointNameTable__.lookup(codePoint);

                        // if this is not a valid codepoint then format the entire buffer
                        // as one block.
                        if (codePointName == null) {
                            len += fullLen;
                            fullLen = 0;
                        }
                    }

                    if (!printColPos) { // not 1st Dss header of this buffer, write seperator
                        dncnetprintln("");
                    }

                    if (codePointName == null) {
                        // codePointName was still null so either < 10 bytes were given or
                        // the codepoint wasn't found in the table.  Just print the plain send header.
                        dncnetprintln(getHeader(type));
                    } else {
                        // codePointName isn't null so the name of the codepoint will be printed.
                        printHeaderWithCodePointName(codePointName, type);
                    }

                    // Print the col position header in the trace.
                    if (printColPos) { // first Dss header of buffer, need column position header
                        dncnetprintln(colPosHeader__);
                        printColPos = false;
                    }

                    // A char array will be used to translate the bytes to their character
                    // representations along with ascii and ebcdic representations.
                    char trcDump[] = new char[77];

                    // bCounter, aCounter, eCounter are offsets used to help position the characters
                    short bCounter = 7;
                    short aCounter = 43;
                    short eCounter = 61;

                    // The lines will be counted starting at zero.
                    // This is hard coded since we are at the beginning.
                    trcDump[0] = zeroChar__;
                    trcDump[1] = zeroChar__;
                    trcDump[2] = zeroChar__;
                    trcDump[3] = zeroChar__;

                    // The 0's are already in the trace so bump the line counter up a row.
                    int lineCounter = 0x10;

                    // Make sure the character array has all blanks in it.
                    // Some of these blanks will be replaced later with values.
                    // The 0's were not wrote over.
                    for (int j = 4; j < 77; j++) {
                        trcDump[j] = spaceChar__;
                    }

                    // i will maintain the position in the byte array to be traced.
                    int i = 0;

                    do {
                        // Get the unsigned value of the byte.
                        //                  int num = b[off++] & 0xff;
                        int num = (buff[offset] < 0) ? buff[offset] + 256 : buff[offset];
                        offset++;
                        i++;
                        // Place the characters representing the bytes in the array.
                        trcDump[bCounter++] = hexDigit__[((num >>> 4) & 0xf)];
                        trcDump[bCounter++] = hexDigit__[(num & 0xf)];

                        // Place the ascii and ebcdc representations in the array.
                        trcDump[aCounter++] = asciiChar__[num];
                        trcDump[eCounter++] = ebcdicChar__[num];

                        if (((i % 8) == 0)) {
                            if (((i % 16) == 0)) {
                                // Print the array each time 16 bytes are processed.
                                dncnetprintln(trcDump);
                                if (i != len) {
                                    // Not yet at the end of the byte array.
                                    if ((len - i) < 16) {
                                        // This is the last line so blank it all out.
                                        // This keeps the last line looking pretty in case
                                        // < 16 bytes remain.
                                        for (int j = 0; j < trcDump.length; j++) {
                                            trcDump[j] = spaceChar__;
                                        }
                                    }
                                    // Reset the counters.
                                    bCounter = 0;
                                    aCounter = 43;
                                    eCounter = 61;
                                    // Reset the lineCounter if it starts to get too large.
                                    if (lineCounter == 0x100000) {
                                        lineCounter = 0;
                                    }
                                    // Place the characters representing the line counter in the array.
                                    trcDump[bCounter++] = hexDigit__[((lineCounter >>> 12) & 0xf)];
                                    trcDump[bCounter++] = hexDigit__[((lineCounter >>> 8) & 0xf)];
                                    trcDump[bCounter++] = hexDigit__[((lineCounter >>> 4) & 0xf)];
                                    trcDump[bCounter++] = hexDigit__[(lineCounter & 0xf)];
                                    bCounter += 3;
                                    // Bump up the line counter.
                                    lineCounter += 0x10;
                                }
                            } else {
                                // 8 bytes were processed so move the counter to adjust for
                                // spaces between the columns of bytes.
                                bCounter += 2;
                            }
                        }
                        // do this until we all the data has been traced.
                    } while (i < len);

                    // print the last line and add some blank lines to make it easier to read.
                    if (len % 16 != 0) {
                        dncnetprintln(trcDump);
                    }
                }
            }
            dncnetprintln("");
        }
    }

    // Gets the int value of the two byte unsigned codepoint.
    private static int getCodePoint(byte[] buff, int offset) {
        return ((buff[offset++] & 0xff) << 8) +
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                (buff[offset] & 0xff);
    }

    private static String getHeader(int type) {
        switch (type) {
        case TYPE_TRACE_SEND:
            return sendHeader__;
        case TYPE_TRACE_RECEIVE:
            return receiveHeader__;
        default:
            return null;
        }
    }

    private static int getStartPosition(int type) {
        switch (type) {
        case TYPE_TRACE_SEND:
            return 20; // This is right after 'SEND BUFFER: '.
        case TYPE_TRACE_RECEIVE:
            return 23; // This is right after 'RECEIVE BUFFER: '.
        default:
            return 0;
        }
    }

    private void printHeaderWithCodePointName(String codePointName, int type) {
        // Create a char array so some of the characters
        // can be replaced with the name of the codepoint.
        char headerArray[] = getHeader(type).toCharArray();

        // At most, 16 character name will be used.  This is so
        // the headers on top of the ascii and ebcdic rows aren't shifted.
        int replaceLen = (codePointName.length() < 17) ? codePointName.length() : 16;

        int offset = getStartPosition(type);
        for (int i = 0; i < replaceLen; i++) {
            headerArray[offset++] = codePointName.charAt(i);
        }
        dncnetprintln(headerArray);
    }

    private void dncnetprint(String s) {
        synchronized (printWriter_) {
            printWriter_.print("[derby] " + s);
            printWriter_.flush();
        }
    }

    private void dncnetprintln(String s) {
        synchronized (printWriter_) {
            printWriter_.println("[derby] " + s);
            printWriter_.flush();
        }
    }

    private void dncnetprintln(char[] s) {
        synchronized (printWriter_) {
            printWriter_.print("[derby] ");
            printWriter_.println(s);
            printWriter_.flush();
        }
    }
}
