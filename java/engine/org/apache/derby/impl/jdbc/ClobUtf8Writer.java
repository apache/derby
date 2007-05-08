/* 
   Derby - Class org.apache.derby.impl.jdbc.ClobUtf8Writer
 
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

package org.apache.derby.impl.jdbc;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.SQLException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;

/**
 * Writer implementation for Clob.
 */
final class ClobUtf8Writer extends Writer {
    private ClobStreamControl control;    
    private long pos; // Position in bytes.
    private boolean closed;
    
    /**
     * Constructor.
     * @param control 
     * @param pos 
     */
    ClobUtf8Writer(ClobStreamControl control, long pos) {
        this.control = control;
        this.pos = pos;
        closed = false;
    }    

    /**
     * Flushes the stream.  If the stream has saved any characters from the
     * various write() methods in a buffer, write them immediately to their
     * intended destination.  Then, if that destination is another character or
     * byte stream, flush it.  Thus one flush() invocation will flush all the
     * buffers in a chain of Writers and OutputStreams.
     * 
     * <p> If the intended destination of this stream is an abstraction provided
     * by the underlying operating system, for example a file, then flushing the
     * stream guarantees only that bytes previously written to the stream are
     * passed to the operating system for writing; it does not guarantee that
     * they are actually written to a physical device such as a disk drive.
     * 
     * @throws IOException
     *          If an I/O error occurs
     */
    public void flush() throws IOException {
        if (closed)
            throw new IOException (
                MessageService.getTextMessage (SQLState.LANG_STREAM_CLOSED));
        //no op
    }

    /**
     * Closes the stream, flushing it first. Once the stream has been closed,
     * further write() or flush() invocations will cause an IOException to be
     * thrown. Closing a previously closed stream has no effect.
     */
    public void close() {
        closed = true;
    }

    /**
     * Writes a portion of an array of characters.
     * 
     * @param cbuf
     *         Array of characters
     * @param off
     *         Offset from which to start writing characters
     * @param len
     *         Number of characters to write
     * @throws IOException
     *          If an I/O error occurs
     */
    public void write(char[] cbuf, int off, int len) throws IOException {
        if (closed)
            throw new IOException (
                MessageService.getTextMessage (SQLState.LANG_STREAM_CLOSED));
        try {
            long ret = control.insertString (String.copyValueOf (
                                                    cbuf, off, len), 
                                              pos);
            if (ret > 0)
                pos += ret;
        }
        catch (SQLException e) {
            IOException ioe = new IOException (e.getMessage());
            ioe.initCause (e);
            throw ioe;
        }
    }
}
