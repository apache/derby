/*

   Class org.apache.derby.optional.lucene.LuceneSupport

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

package org.apache.derby.optional.lucene;

import java.io.EOFException;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

/**
 * <p>
 * Wrapper for a StorageRandomAccessFile which can serve as a
 * Lucene IndexInput.
 * </p>
 */
class DerbyIndexInput   extends IndexInput
{
    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////

    // constructor state
    private StorageFile                     _file;
    private StorageRandomAccessFile _sraf;
    private final ArrayList<IndexInput> _slices = new ArrayList<IndexInput>();
    private final long _offset;
    private final long _length;

    // mutable state
    private boolean _closed = false;

    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////////////////

    /** Construct from a StorageRandomAccessFile */
    DerbyIndexInput( StorageFile file )
        throws IOException
    {
        this(file, file.getPath(), 0L, null);
    }

    /**
     * Create a DerbyIndexInput that reads data from a StorageFile.
     *
     * @param file the file to read from
     * @param description a description of the file (will be returned
     *                    from {@code toString()})
     * @param offset where to start reading in the file
     * @param length how much of the file to read, {@code null} means
     *               read till end of file
     * @throws IOException if an I/O error occurs
     */
    private DerbyIndexInput(StorageFile file, String description,
                            long offset, Long length)
        throws IOException
    {
        super(description);

        setConstructorFields( file );

        _offset = offset;
        if (length == null) {
            _length = _sraf.length() - offset;
        } else {
            _length = length;
        }

    }

    /** Set the constructor fields */
    private void    setConstructorFields( final StorageFile file )
        throws IOException
    {
        try {
            AccessController.doPrivileged
            (
             new PrivilegedExceptionAction<Void>()
             {
                public Void run() throws IOException
                {
                    _file = file;
                    _sraf = _file.getRandomAccessFile( "r" );

                    return null;
                }
             }
             );
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  IndexInput METHODS
    //
    /////////////////////////////////////////////////////////////////////

    public  IndexInput  clone()
    {
        try {
            // A clone is a slice that covers the entire range of this
            // index input instance.
            IndexInput clone = slice(_file.getPath(), 0L, _length);

            clone.seek( getFilePointer() );

            return clone;
        }
        catch (IOException ioe) { throw wrap( ioe ); }
    }

    public IndexInput slice(String sliceDescription, long offset, long length)
        throws IOException
    {
        checkIfClosed();

        if (offset < 0 || length < 0 || offset > _length - length) {
            throw new IllegalArgumentException();
        }

        DerbyIndexInput slice = new DerbyIndexInput(
                _file, sliceDescription, _offset + offset, length);
        _slices.add(slice);
        slice.seek(0L);
        return slice;
    }

    public void close() throws IOException
    {
        if ( !_closed )
        {
            _closed = true;
            _sraf.close();

            for ( IndexInput slice : _slices ) { slice.close(); }
            _slices.clear();

            _file = null;
            _sraf = null;
        }
    }

    public long getFilePointer()
    {
        checkIfClosed();

        try {
            return _sraf.getFilePointer() - _offset;
        }
        catch (IOException ioe) { throw wrap( ioe ); }
    }

    public long length()
    {
        checkIfClosed();
        return _length;
    }

    public void seek( long pos )    throws IOException
    {
        checkIfClosed();
        _sraf.seek( _offset + pos );
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  DataInput METHODS
    //
    /////////////////////////////////////////////////////////////////////

    public byte readByte()  throws IOException
    {
        checkEndOfFile(1);
        return _sraf.readByte();
    }

    public void readBytes( byte[] b, int offset, int len )
        throws IOException
    {
        checkEndOfFile(len);

        int     bytesRead = 0;
        while ( bytesRead < len )
        {
            int increment = _sraf.read( b, offset + bytesRead , len - bytesRead );
            if ( increment < 0 ) { break; }

            bytesRead += increment;
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * Verify that we can read {@code length} bytes without hitting end
     * of file (or end of the slice represented by this instance).
     *
     * @param length the number of bytes we need
     * @throws EOFException if the requested number of bytes is not available
     * @throws AlreadyClosedException if this object has been closed
     */
    private void checkEndOfFile(int length) throws EOFException {
        // getFilePointer() calls checkIfClosed(), so no need to call it
        // explicitly here.
        long available = _length - getFilePointer();
        if (length > available) {
            throw new EOFException();
        }
    }

    /** Raise a Lucene error if this object has been closed */
    private void    checkIfClosed()
    {
        if ( _closed )
        {
            throw new AlreadyClosedException( toString() );
        }
    }

    /** Wrap an exception in a Runtime exception */
    private RuntimeException    wrap( Throwable t )
    {
        return new RuntimeException( t.getMessage(), t );
    }

}
