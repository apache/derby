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
public  class DerbyIndexInput   extends IndexInput
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
    private final   ArrayList<DerbyIndexInput>  _clones = new ArrayList<DerbyIndexInput>();

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
        super( file.getPath() );

        setConstructorFields( file );
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
        checkIfClosed();
        
        try {
            DerbyIndexInput clone = new DerbyIndexInput( _file );
            _clones.add( clone );

            clone.seek( getFilePointer() );

            return clone;
        }
        catch (IOException ioe) { throw wrap( ioe ); }
    }

    public void close() throws IOException
    {
        if ( !_closed )
        {
            _closed = true;
            _sraf.close();

            for ( DerbyIndexInput clone : _clones ) { clone.close(); }
            _clones.clear();

            _file = null;
            _sraf = null;
        }
    }

    public long getFilePointer()
    {
        checkIfClosed();

        try {
            return _sraf.getFilePointer();
        }
        catch (IOException ioe) { throw wrap( ioe ); }
    }

    public long length()
    {
        checkIfClosed();

        try {
            return _sraf.length();
        }
        catch (IOException ioe) { throw wrap( ioe ); }
    }

    public void seek( long pos )    throws IOException
    {
        checkIfClosed();
        _sraf.seek( pos );
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  DataInput METHODS
    //
    /////////////////////////////////////////////////////////////////////

    public byte readByte()  throws IOException
    {
        checkIfClosed();
        return _sraf.readByte();
    }

    public void readBytes( byte[] b, int offset, int len )
        throws IOException
    {
        checkIfClosed();

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

    /** Wrap an exception in an IOException */
    private IOException wrapWithIOException( Throwable t )
    {
        return new IOException( t.getMessage(), t );
    }
    
}
