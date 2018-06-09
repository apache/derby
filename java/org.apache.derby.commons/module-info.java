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

module org.apache.derby.commons
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;
    requires java.sql;

    //
    // FOR LOADING MESSAGE LOCALIZATIONS FROM
    // OTHER MODULES.
    //
    uses org.apache.derby.loc.client.spi.clientmessagesProvider;

    //
    // DERBY INTERNAL EXPORTS
    //
    // ONLY VISIBLE TO OTHER DERBY MODULES.
    //
    exports org.apache.derby.loc.client.spi to
        org.apache.derby.client;

    exports org.apache.derby.shared.common.drda to
        org.apache.derby.client,
        org.apache.derby.server,
        org.apache.derby.optionaltools;

    exports org.apache.derby.shared.common.error to
        org.apache.derby.engine,
        org.apache.derby.client,
        org.apache.derby.server,
        org.apache.derby.tools,
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

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

    exports org.apache.derby.shared.common.security to
        org.apache.derby.engine,
        org.apache.derby.server,
        org.apache.derby.tests;

    exports org.apache.derby.shared.common.stream to
        org.apache.derby.engine,
        org.apache.derby.server,
        org.apache.derby.tests;

    exports org.apache.derby.shared.common.util to
        org.apache.derby.engine;
}
