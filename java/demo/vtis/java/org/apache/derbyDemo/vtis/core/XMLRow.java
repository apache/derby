/*

Derby - Class org.apache.derbyDemo.vtis.core.XMLRow

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

package org.apache.derbyDemo.vtis.core;

import java.lang.annotation.*;

/**
 * <p>
 * This is an Annotation describing how to decompose an XML file into
 * relational rows.
 * </p>
 *
  */
@Retention( value=RetentionPolicy.RUNTIME )
public  @interface  XMLRow
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** The tag name of Elements which should be mapped to SQL rows */
    String      rowTag();
    
    /** The columns in a row, including the column names and their lengths */
    String[]   childTags();    
    
    /** The types of the columns corresponding to the child tags*/
    String[]   childTypes();    

    /** The name of the VTI class which produces the rows */
    String      vtiClassName();
    
}
