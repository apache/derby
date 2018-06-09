/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.FullName

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A Comparable UDT for tests.
 */
public class FullName implements Comparable<FullName>, Externalizable
{
    // initial version id
    private static final int FIRST_VERSION = 0;

    private String  _firstName;
    private String  _lastName;

    // 0-arg constructor needed for UDT contract
    public  FullName() {}

    public FullName( String firstName, String lastName )
    {
        _firstName = firstName;
        _lastName = lastName;
    }

    public  String  firstName() { return _firstName; }
    public  String  lastName() { return _lastName; }

        // methods to be registered as functions
    public static FullName makeFullName( String firstName, String lastName ) { return new FullName( firstName, lastName ); }

    public  String  toString()
    {
        return _firstName + " " + _lastName;
    }

    // Externalizable implementation
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        // first write the version id
        out.writeInt( FIRST_VERSION );

        // now write the state
        out.writeObject( _firstName );
        out.writeObject( _lastName );
    }  
    public void readExternal( ObjectInput in )throws IOException, ClassNotFoundException
    {
        // read the version id
        int oldVersion = in.readInt();

        _firstName = (String) in.readObject();
        _lastName = (String) in.readObject();
    }

    // Comparable implementation
    public  int compareTo( FullName that )
    {
        if ( that == null ) { return 1; }

        int     result = this._lastName.compareTo( that._lastName );
        if ( result != 0 ) { return result; }

        return this._firstName.compareTo( that._firstName );
    }
    public  boolean equals( Object other )
    {
        if ( other == null ) { return false; }
        else if ( !(other instanceof FullName) ) { return false; }
        else { return (compareTo( (FullName) other ) == 0); }
    }
    public  int hashCode() { return toString().hashCode(); }

}

