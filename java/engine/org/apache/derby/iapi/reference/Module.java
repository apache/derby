/*

   Derby - Class org.apache.derby.iapi.reference.Module

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.reference;

public interface Module {

	String CacheFactory = "org.apache.derby.iapi.services.cache.CacheFactory";
	String CipherFactory = "org.apache.derby.iapi.services.crypto.CipherFactory";
	String ClassFactory = "org.apache.derby.iapi.services.loader.ClassFactory";
	String DaemonFactory = "org.apache.derby.iapi.services.daemon.DaemonFactory";
	String JavaFactory ="org.apache.derby.iapi.services.compiler.JavaFactory";
	String LockFactory = "org.apache.derby.iapi.services.locks.LockFactory";
	String PropertyFactory = "org.apache.derby.iapi.services.property.PropertyFactory";
	String ResourceAdapter = "org.apache.derby.iapi.jdbc.ResourceAdapter";

}
