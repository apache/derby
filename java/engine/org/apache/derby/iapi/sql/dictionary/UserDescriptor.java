/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.UserDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * A Descriptor for a user stored in SYSUSERS.
 */
public final class  UserDescriptor extends TupleDescriptor 
{
	private String _userName;
	private String _hashingScheme;
    private char[] _password;
    private Timestamp _lastModified;
	
	/**
	 * Constructor for a UserDescriptor.
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in.
	 * @param userName  Name of the user.
	 * @param hashingScheme How the password was hashed.
	 * @param password  The user's password.
	 * @param lastModified  Time that the password was last modified.
	 */
    UserDescriptor
        (
         DataDictionary dataDictionary,
         String userName,
         String hashingScheme,
         char[] password,
         Timestamp lastModified
         )
	{
		super( dataDictionary );

        _userName = userName;
        _hashingScheme = hashingScheme;

        if ( password == null ) { _password = null; }
        else
        {
            // copy the password because the caller will 0 it out
            _password = new char[ password.length ];
            System.arraycopy( password, 0, _password, 0, password.length );
        }
        
        _lastModified = lastModified;
	}

	public String getUserName(){ return _userName; }
	public String getHashingScheme()    { return _hashingScheme; }
    public  Timestamp   getLastModified()   { return _lastModified; }

    /**
     * <p>
     * Zero the password after getting it so that the char[] can't be memory-sniffed.
     * </p>
     */
	public char[]   getAndZeroPassword()
	{
		int length = _password.length;
        char[] retval = new char[ length ];
        System.arraycopy( _password, 0, retval, 0, length );
        Arrays.fill( _password, (char) 0 );

        return retval;
	}

	//
	// class interface
	//

	
	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType() { return "User"; }

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return _userName; }



}
