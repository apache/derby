/*
 
   Derby - Class org.apache.derby.jdbc.EmbeddedXADataSource40
 
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

package org.apache.derby.jdbc;

/**
   <P>
   This is a vacuous, deprecated class. At one time, it had real behavior and helped us support
   separate datasources for Java 5 and Java 6.
   Now that we no longer support Java 5, all functionality has migrated into the superclass, EmbeddedXADataSource.
   This class is preserved for backward compatibility reasons.
   </P>
   @deprecated Use {@link EmbeddedXADataSource} instead.
 */
public class EmbeddedXADataSource40 extends EmbeddedXADataSource
    implements javax.sql.XADataSource /* compile-time check for 4.1 extension */
{
   private static final long serialVersionUID = 4048303427908481258L;

    /** Creates a new instance of EmbeddedXADataSource40 */
    public EmbeddedXADataSource40() { super(); }
    
}
