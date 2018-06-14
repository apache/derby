/*

   Derby - Class org.apache.derby.vti.VTIContext

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

package org.apache.derby.vti;

/**
 * <p>
 * Context parameter which is passed to an AwareVTI.
 * </p>
 */
public class VTIContext
{
    /////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////
    
    /////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////

    private String  _vtiSchema;
    private String  _vtiTable;
    private String  _statementText;
    
    /////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct from pieces.
     * </p>
     *
     * @param   vtiSchema   Name of the schema holding the table function.
     * @param   vtiTable      Name of the table function.
     * @param   statementText   Text of the statement which is invoking the table function.
     */
    public  VTIContext
        (
         String vtiSchema,
         String vtiTable,
         String statementText
         )
    {
        _vtiSchema = vtiSchema;
        _vtiTable = vtiTable;
        _statementText = statementText;
    }
    
    /////////////////////////////////////////////////////////////////
    //
    //  PUBLIC BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////

    /**
     * Return the name of the schema holding the table function
     *
     * @return the name of the schema holding this table function
     */
    public  String  vtiSchema() { return _vtiSchema; }

    /**
     * Return the unqualified table function name
     *
     * @return the (unqualified) name of this table function
     */
    public  String  vtiTable()  { return _vtiTable; }

    /**
     * Return the text of the statement which invoked the table function
     *
     * @return the text of the statement which invoked this table function
     */
    public  String  statementText() { return _statementText; }
    
}
