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
 * All configurations need this utility module.
 * </p>
 * 
 * <p><b>Module Diagram:</b></p>
 *
 * <div style="text-align:center;">
 *   <img
 *     src="resources/commons.svg"
 *     alt="module diagram for org.apache.derby.commons"
 *     border="2"
 *   />
 * </div>
 *
 */
module org.apache.derby.commons
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;
    requires java.sql;

    //
    // PUBLIC API
    //
    exports org.apache.derby.shared.api;
    exports org.apache.derby.shared.common.security;

    //
    // FOR FINDING ALL DERBY MODULES
    //
    uses org.apache.derby.shared.api.DerbyModuleAPI;
    
    //
    // SUPPORT MODULE LOOKUP
    //
    provides org.apache.derby.shared.api.DerbyModuleAPI
        with org.apache.derby.info.shared.DerbyModule;

    //
    // FIXME! EXPOSED SO THAT THESE PACKAGES CAN BE ACCESSED
    // BY THE QUERY PLANS WHICH ARE CODE-GENERATED
    // INTO THE UNNAMED MODULE.
    //
    exports org.apache.derby.shared.common.error;


    //
    // DERBY INTERNAL EXPORTS
    //
    // ONLY VISIBLE TO OTHER DERBY MODULES.
    //
    exports org.apache.derby.shared.common.drda to
        org.apache.derby.client,
        org.apache.derby.server,
        org.apache.derby.optionaltools;

    exports org.apache.derby.shared.common.i18n to
        org.apache.derby.engine,
        org.apache.derby.client,
        org.apache.derby.server,
        org.apache.derby.tools,
        org.apache.derby.tests;

    exports org.apache.derby.shared.common.info to
        org.apache.derby.engine,
        org.apache.derby.client,
        org.apache.derby.server,
        org.apache.derby.tools,
        org.apache.derby.tests;

    exports org.apache.derby.shared.common.reference to
        org.apache.derby.engine,
        org.apache.derby.client,
        org.apache.derby.server,
        org.apache.derby.tools,
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.shared.common.sanity to
        org.apache.derby.engine,
        org.apache.derby.client,
        org.apache.derby.server,
        org.apache.derby.tests;

    exports org.apache.derby.shared.common.stream to
        org.apache.derby.engine,
        org.apache.derby.server,
        org.apache.derby.tests;

    exports org.apache.derby.shared.common.util to
        org.apache.derby.engine;

    //
    // STANZAS FOR USE WHEN QUERY PLANS ARE GENERATED INTO
    // SOME MODULE OTHER THAN THE UNNAMED MODULE.
    //
    //
    // QUERY PLANS REFERENCE StandardException
    //
    //    exports org.apache.derby.shared.common.error to
    //        org.apache.derby.engine,
    //        org.apache.derby.client,
    //        org.apache.derby.server,
    //        org.apache.derby.tools,
    //        org.apache.derby.optionaltools,
    //        org.apache.derby.tests;
}
