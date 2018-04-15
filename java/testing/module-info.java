module org.apache.derby.tests
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;
    requires java.logging;
    requires java.management;
    requires java.naming;
    requires java.sql;
    requires java.xml;

    requires org.apache.derby.commons;
    requires org.apache.derby.engine;
    requires org.apache.derby.server;
    requires org.apache.derby.client;
    requires org.apache.derby.tools;
    requires org.apache.derby.optionaltools;
    requires org.apache.derby.runner;

    requires junit;

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
}
