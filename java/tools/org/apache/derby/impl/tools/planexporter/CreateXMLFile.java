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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

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
     * @param data large xml data string array
     * @param file_name name of the file to be written
     * @param xsl_sheet_name name of the style sheet
     * @throws PrivilegedActionException
     * @throws IOException
     * @throws PrivilegedActionException
     */
    public void writeTheXMLFile(String stmt, String time,
            TreeNode[] data, final String file_name, String xsl_sheet_name)
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

        FileOutputStream fos;
        try {
            fos = (FileOutputStream) AccessController.doPrivileged(
                    new PrivilegedExceptionAction() {
                        public Object run() throws IOException {
                            return new FileOutputStream(file_name);
                        }
                    });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }

        Writer out = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));

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
        out.close();
    }
}
