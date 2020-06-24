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
import java.util.zip.CRC32;

import org.apache.lucene.store.IndexOutput;

import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;

/**
 * <p>
 * Wrapper for a StorageRandomAccessFile which can serve as a
 * Lucene IndexOutput.
 * </p>
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6621
class DerbyIndexOutput   extends IndexOutput
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
    private DerbyLuceneDir              _parentDir;
    private StorageRandomAccessFile _sraf;
    private final CRC32 _crc = new CRC32();

    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////////////////

    /** Construct from a StorageRandomAccessFile */
    DerbyIndexOutput( StorageFile file, DerbyLuceneDir parentDir )
        throws IOException
    {
        _file = file;
        _parentDir = parentDir;
        _sraf = _file.getRandomAccessFile( "rw" );
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  IndexOutput METHODS
    //
    /////////////////////////////////////////////////////////////////////

    public void close() throws IOException
    {
        _sraf.close();
        _parentDir.removeIndexOutput( _file.getName() );

        _file = null;
        _parentDir = null;
        _sraf = null;
    }

    public long getFilePointer()
    {
        try {
            return _sraf.getFilePointer();
        }
        catch (IOException ioe) { throw wrap( ioe ); }
    }

    @Deprecated
    public void seek( long pos )    throws IOException
    {
        _sraf.seek( pos );
    }

    public  void flush()    throws IOException
    {
        _sraf.sync();
    }

    public long length()    throws IOException
    {
        return _sraf.length();
    }

    public long getChecksum()
    {
        return _crc.getValue();
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  DataOutput METHODS
    //
    /////////////////////////////////////////////////////////////////////

    public void writeByte(byte b)   throws IOException
    {
        _sraf.writeByte( b );
        _crc.update(b);
    }

    public void writeBytes(byte[] b, int offset, int length)
        throws IOException
    {
        _sraf.write( b, offset, length );
        _crc.update(b, offset, length);
    }


    /////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////

    /** Wrap an exception in a Runtime exception */
    private RuntimeException    wrap( Throwable t )
    {
        return new RuntimeException( t.getMessage(), t );
    }
    
}
