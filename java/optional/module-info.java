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
}
