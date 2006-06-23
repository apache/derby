/*

   Derby - Class org.apache.derby.iapi.services.crypto.CipherFactoryBuilder

   Copyright 1998, 2006 The Apache Software Foundation or its licensors, as applicable.

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


package org.apache.derby.iapi.services.crypto;
import org.apache.derby.iapi.error.StandardException;
import java.util.Properties;


/*
 * Interface to create instances of the cipher factory 
 * based on the user specified encryption properties.
 */

public interface CipherFactoryBuilder
{

    /**
     * Create an instance of the cipher factory.
     *
     * @param create    true, if the database is getting configured 
     *                  for encryption.
     * @param props	    encryption properties/attributes to use
     *                  for creating the cipher factory.
     * @param newAttrs  true, if cipher factory has to be created using 
     *                  the new attributes specified by the user. 
     *                 
     */
    public CipherFactory createCipherFactory(boolean create, 
                                             Properties props, 
                                             boolean newAttrs) 
        throws StandardException;
}
