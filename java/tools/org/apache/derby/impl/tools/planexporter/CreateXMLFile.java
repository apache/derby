/*

   Derby - Class org.apache.derby.impl.tools.planexporter.CreateXMLFile

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

package org.apache.derby.impl.tools.planexporter;

import java.io.IOException;
import java.io.Writer;

/**
 * This class is to create the final xml file, that will be used
 * by the Graphical Query Explainer.
 * This is called from org.apache.derby.tools.PlanExporter.
 */
public class CreateXMLFile {

    AccessDatabase access;

    public CreateXMLFile(AccessDatabase access) {
        this.access = access;
    }

    /**
     * @param stmt statement executed
     * @param time time which the statement was executed
     * @param out where to write the XML file
     * @param xsl_sheet_name name of the style sheet
     * @throws IOException
     */
    public void writeTheXMLFile(String stmt, String time,
                                Writer out, String xsl_sheet_name)
    throws IOException {

        String defaultXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        String embedXSL="";
        if(xsl_sheet_name != null)
            embedXSL ="<?xml-stylesheet type=\"text/xsl\" href=\""
                        +xsl_sheet_name+"\"?>\n";
        String comment = "<!-- Apache Derby Query Explainer (DERBY-4587)-->\n";
        String parentTagStart = "<plan>\n";
        String parentTagEnd = "</plan>\n";
        String childTagStart = "<details>\n";
        String childTagEnd = "</details>\n";

        out.write(defaultXML);

        out.write(embedXSL);
        out.write(comment);
        out.write(parentTagStart);

        out.write(access.indent(0));
        out.write(stmt);

        out.write(access.indent(0));
        out.write(time);

        out.write(access.indent(0));
        out.write(access.stmtID());

        out.write(access.indent(0));
        out.write(childTagStart);

        out.write(access.getXmlString());

        out.write(access.indent(0));
        out.write(childTagEnd);

        out.write(parentTagEnd);
    }
}
