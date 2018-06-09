/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.helpers.DummyClob

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
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * A crude Clob implementation for datatype testing.
 */
public class DummyClob
        implements Clob {
    private	String	_contents;

    public	DummyClob(String contents)
    {
        _contents = contents;
    }

    public	InputStream	getAsciiStream()
    {
        try {
            return new ByteArrayInputStream( _contents.getBytes( "UTF-8" ) );
        }
        catch (Exception e) { return null; }
    }

    public	Reader	getCharacterStream()
    {
        return new StringReader(_contents);
    }

    public	String	getSubString( long position, int length )
    {
        return _contents.substring( (int) position -1, length );
    }

    public	long	length() { return (long) _contents.length(); }

    public	long	position( Clob searchstr, long start ) { return 0L; }
    public	long	position( String searchstr, long start ) { return 0L; }

    public	boolean	equals( Object other )
    {
        if ( other == null ) { return false; }
        if ( !( other instanceof Clob ) ) { return false; }

        Clob	that = (Clob) other;

        try {
            if ( this.length() != that.length() ) { return false; }

            InputStream	thisStream = this.getAsciiStream();
            InputStream	thatStream = that.getAsciiStream();

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

    public int setString(long arg0, String arg1) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public int setString(long arg0, String arg1, int arg2, int arg3) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public OutputStream setAsciiStream(long arg0) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public Writer setCharacterStream(long arg0) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public void truncate(long arg0) throws SQLException {
        throw new SQLException("not implemented for this test");
    }

    public void free() throws SQLException {
        _contents = null;
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(
                _contents.substring((int)pos -1, (int)(pos + length)));
    }
}
