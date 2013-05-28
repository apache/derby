/*

   Derby - Class org.apache.derby.jdbc.BasicEmbeddedDataSource40

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

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import org.apache.derby.impl.jdbc.Util;

/**
 *
 * This datasource is suitable for an application using embedded Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicEmbeddedDataSource40 is similar to EmbeddedDataSource40, but does
 * not support JNDI naming, i.e. it does not implement
 * {@code javax.naming.Referenceable}.
 *
 * @see EmbeddedDataSource40
 */
 public class BasicEmbeddedDataSource40 extends EmbeddedBaseDataSource
    implements javax.sql.DataSource {

   private static final long serialVersionUID = -4945135214995641182L;

    public BasicEmbeddedDataSource40() {}

}
