module org.apache.derby.client
{
    //
    // MANDATORY IMPORTS
    //
    // REQUIRED AT COMPILE-TIME AND AT RUN-TIME.
    //
    requires java.base;
    requires java.logging;
    requires java.sql;

    requires org.apache.derby.commons;

    //
    // OPTIONAL IMPORTS
    //
    // REQUIRED AT COMPILE-TIME.
    // OPTIONAL AT RUN-TIME.
    //
    requires static java.naming;

    //
    // DERBY INTERNAL EXPORTS
    //
    // ONLY VISIBLE TO OTHER DERBY MODULES.
    //
    exports org.apache.derby.client to
        org.apache.derby.tools;

    exports org.apache.derby.client.am to
        org.apache.derby.tests;

    exports org.apache.derby.client.am.stmtcache to
        org.apache.derby.tests;

    exports org.apache.derby.client.net to
        org.apache.derby.tests;
}
