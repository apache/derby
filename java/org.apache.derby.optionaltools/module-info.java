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

/**
 * <p>
 * An extra set of tools supports metadata introspection, access to other
 * vendors' databases, full-text search, and the importing of
 * JSON-formatted data. Optional functionality includes:
 * </p>
 * 
 * <ul>
 *  <li><b><font color="gray">json.simple</font></b> - To import
 *  JSON-formatted data, include the <i>JSON.simple</i> jar file available from
 *   <a href="https://code.google.com/archive/p/json-simple/">https://code.google.com/archive/p/json-simple/</a>.</li>
 *  <li><b><font color="gray">lucene.core, lucene.queryparser, lucene.analyzers.common</font></b> - To run full-text
 *  searches, include the <i>lucene-core-4.5.0.jar</i>,
 *  <i>lucene-queryparser-4.5.0.jar</i>, and <i>lucene-analyzers-common-4.5.0.jar</i>
 *   modules available from the
 *   <a href="https://lucene.apache.org/">Apache Lucene project</a>.</li>
 *  <li><b><font color="gray">org.apache.derby.locale_*</font></b> - Include
 *  these modules for non-English diagnostic messages.</li>
 *  <li><b><font color="gray">org.osgi.framework</font></b> - See the
 *  header comment on the engine module.</li>
 *  <li><b><font color="gray">java.management</font></b> - See the
 *  header comment on the engine module.</li>
 *  <li><b><font color="gray">java.naming</font></b> - This
 *  module supports JNDI lookup of LDAP authenticators when running with
 *  the embedded driver.</li>
 * </ul>
 * 
 * <p><b>Module Diagram:</b></p>
 *
 * <div style="text-align:center;">
 *   <img
 *     src="resources/optionaltools.svg"
 *     alt="module diagram for org.apache.derby.optionaltools"
 *     border="2"
 *   />
 * </div>
 *
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

    //
    // OPEN TO REFLECTIVE ACCESS FROM THE ENGINE
    //
    opens org.apache.derby.optional.lucene to org.apache.derby.engine;
    
    //
    // FIXME! EXPOSED SO THAT THESE PACKAGES CAN BE ACCESSED
    // BY THE QUERY PLANS WHICH ARE CODE-GENERATED
    // INTO THE UNNAMED MODULE.
    //
    exports org.apache.derby.optional.lucene;
    opens org.apache.derby.optional.json;
    opens org.apache.derby.optional.dump;
}
