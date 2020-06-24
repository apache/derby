/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.PrivilegedSQLObject

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

import org.apache.derby.iapi.sql.depend.Provider;

/**
 * This is a descriptor for schema object which can have privileges granted on it.
 */
public abstract class PrivilegedSQLObject
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    extends UniqueSQLObjectDescriptor
    implements Provider
{
    /** Pass-through constructors */
    public  PrivilegedSQLObject() { super(); }
    public  PrivilegedSQLObject( DataDictionary dd ) { super( dd ); }
    
    /** Get the type of the object for storage in SYS.SYSPERMS */
    public abstract String getObjectTypeName();
    
}
