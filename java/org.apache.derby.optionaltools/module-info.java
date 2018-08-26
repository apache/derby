/*

   Derby - Class module-info

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

module org.apache.derby.optionaltools
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;
    requires java.sql;

    requires org.apache.derby.engine;
    requires org.apache.derby.tools;
    requires org.apache.derby.commons;
    
    //
    // OPTIONAL IMPORTS
    //
    // REQUIRED AT COMPILE-TIME.
    // OPTIONAL AT RUN-TIME.
    //
    requires static json.simple;
    requires static lucene.analyzers.common;
    requires static lucene.core;
    requires static lucene.queryparser;

    //
    // DERBY PUBLIC API
    //
    // VISIBLE TO ALL CLASSES AT RUNTIME.
    //
    exports org.apache.derby.optional.api;

    //
    // SUPPORT MODULE LOOKUP
    //
    provides org.apache.derby.shared.api.DerbyModuleAPI
        with org.apache.derby.optional.info.DerbyModule;

}
