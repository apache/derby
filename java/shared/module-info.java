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
