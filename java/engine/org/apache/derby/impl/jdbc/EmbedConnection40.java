/*
 
   Derby - Class org.apache.derby.impl.jdbc.EmbedConnection40
 
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

package org.apache.derby.impl.jdbc;

import java.sql.Array;
import java.sql.SQLClientInfoException;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.EngineConnection40;
import org.apache.derby.iapi.jdbc.FailedProperties40;

public class EmbedConnection40
        extends EmbedConnection implements EngineConnection40 {
    
    /** Creates a new instance of EmbedConnection40 */
    public EmbedConnection40(EmbedConnection inputConnection) {
        super(inputConnection);
    }
    
    public EmbedConnection40(
        InternalDriver driver,
        String url,
        Properties info)
        throws SQLException {
        super(driver, url, info);
    }
    
}
