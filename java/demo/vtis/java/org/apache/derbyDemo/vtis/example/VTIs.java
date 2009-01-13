/*

Derby - Class org.apache.derbyDemo.vtis.example.VTIs

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

package org.apache.derbyDemo.vtis.example;

import java.lang.reflect.*;
import java.sql.*;

import org.apache.derbyDemo.vtis.core.*;

/**
 * <p>
 * This is a set of table functions based on the annotations and helper logic
 * provided with this demo code.
 * </p>
 *
 */
public  class   VTIs
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // XML VTIs
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * <p>
     * This is a vanilla XML VTI with no special processing for the formatting
     * of accessDates or nulls.
     * </p>
     */
    @XMLRow
        (
         rowTag = "Visitor",
         childTags = { "IP", "accessDate", "request", "statusCode", "fileSize", "referrer", "userAgent" },
         childTypes = { "varchar(30)", "varchar(30)", "clob", "int", "varchar( 10 )", "varchar(30)", "clob" },
         vtiClassName = "org.apache.derbyDemo.vtis.core.XmlVTI"
         )
    public  static  ResultSet   apacheVanillaLogFile( String xmlResource ) throws SQLException
    { return XmlVTI.instantiateVTI( xmlResource ); }

    /**
     * <p>
     * This is an XML VTI which handles the Apache server's formatting of accessDate and
     * nulls. This allows us to represent accessDate as a timestamp and to
     * expose nulls in the log.
     * </p>
     */
    @XMLRow
        (
         rowTag = "Visitor",
         childTags = { "IP", "accessDate", "request", "statusCode", "fileSize", "referrer", "userAgent" },
         childTypes = { "varchar(30)", "timestamp", "clob", "int", "int", "varchar(30)", "clob" },
         vtiClassName = "org.apache.derbyDemo.vtis.example.ApacheServerLogVTI"
         )
    public  static  ResultSet   apacheNaturalLogFile( String xmlResource ) throws SQLException
    { return XmlVTI.instantiateVTI( xmlResource ); }

    /**
     * <p>
     * This is a vanilla XML VTI for reading a Derby JIRA report.
     * </p>
     */
    @XMLRow
        (
         rowTag = "item",
         childTags = { "key", "type", "priority", "status", "component", "title", "reporter", "assignee", "resolution", "created", "updated", "votes", "version", "fixVersion" },
         childTypes = { "varchar(12)", "varchar(10)", "varchar(10)", "varchar(10)", "varchar(50)", "varchar(200)", "varchar(50)", "varchar(50)", "varchar(20)", "varchar(50)", "varchar(50)", "integer", "varchar(200)", "varchar(200)" },
         vtiClassName = "org.apache.derbyDemo.vtis.core.XmlVTI"
         )
    public  static  ResultSet   apacheVanillaJiraReport( String xmlResource ) throws SQLException
    { return XmlVTI.instantiateVTI( xmlResource ); }

    /**
     * <p>
     * ThisXML VTI treats keys as integers when parsing Derby JIRA reports.
     * </p>
     */
    @XMLRow
        (
         rowTag = "item",
         childTags = { "key", "type", "priority", "status", "component", "customfieldvalue", "title" },
         childTypes = { "int", "varchar(10)", "varchar(10)", "varchar(10)", "varchar(50)", "varchar(200)", "varchar(200)" },
         vtiClassName = "org.apache.derbyDemo.vtis.example.DerbyJiraReportVTI"
         )
    public  static  ResultSet   apacheNaturalJiraReport( String xmlResource ) throws SQLException
    { return XmlVTI.instantiateVTI( xmlResource ); }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Query VTIs
    //
    ///////////////////////////////////////////////////////////////////////////////////
    
    /**
     * <p>
     * This simple VTI siphons a table out of a MySQL database.
     * </p>
     */
    @QueryRow
        (
         jdbcDriverName = "com.mysql.jdbc.Driver",
         query = "select * from CountryLanguage"
         )
    public  static  ResultSet   countryLanguage( String connectionURL ) throws SQLException
    { return QueryVTIHelper.instantiateQueryRowVTI( connectionURL ); }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////


}
