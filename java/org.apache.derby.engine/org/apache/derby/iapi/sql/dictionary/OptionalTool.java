/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.OptionalTool

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

import java.sql.SQLException;

/**
 * <p>
 * Interface implemented by optional tools which can be loaded and unloaded.
 * In addition to the methods listed here, an OptionalTool must have a public no-arg
 * constructor so that it can be instantiated by the DataDictionary.
 * </p>
 */
public  interface   OptionalTool
{
    /** Load the tool, giving it optional configuration parameters */
    public  void    loadTool( String... configurationParameters )
        throws SQLException;
    
    /** Unload the tool, giving it optional configuration parameters */
    public  void    unloadTool( String... configurationParameters )
        throws SQLException;
    
}

