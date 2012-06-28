/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.helpers.DummyBlob

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.compatibility.helpers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * A crude Blob implementation for datatype testing.
 */
public class DummyBlob
        implements Blob {
    private	byte[]	_bytes;

    public	DummyBlob( byte[] bytes )
    {
        _bytes = bytes;
    }

    public	InputStream	getBinaryStream()
    {
        return new ByteArrayInputStream( _bytes );
    }

    public	byte[]	getBytes( long position, int length ) { return _bytes; }

    public	long	length() { return (long) _bytes.length; }

    public	long	position( Blob pattern, long start ) { return 0L; }
    public	long	position( byte[] pattern, long start ) { return 0L; }

    public	boolean	equals( Object other )
    {
        if ( other == null ) { return false; }
        if ( !( other instanceof Blob ) ) { return false; }

        Blob	that = (Blob) other;

        try {
            if ( this.length() != that.length() ) { return false; }

            InputStream	thisStream = this.getBinaryStream();
            InputStream	thatStream = that.getBinaryStream();

            while( true )
            {
                int		nextByte = thisStream.read();

                if ( nextByte < 0 ) { break; }
                if ( nextByte != thatStream.read() ) { return false; }
            }
        }
        catch (Exception e)
        {
            System.err.println( e.getMessage() );
            e.printStackTrace(System.err);
            return false;
        }

        return true;
    }

    public int setBytes(long arg0, byte[] arg1) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public int setBytes(long arg0, byte[] arg1, int arg2, int arg3)
            throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public OutputStream setBinaryStream(long arg0) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public void truncate(long arg0) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public void free() throws SQLException {
        _bytes = null;
    }

    public InputStream getBinaryStream(long pos, long length)
            throws SQLException {
        return new ByteArrayInputStream(_bytes, (int)pos -1, (int)length);
    }
}
