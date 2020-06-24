/*

   Derby - Class org.apache.derby.impl.tools.planexporter.CreateHTMLFile

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

import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.stream.StreamResult;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;

/**
 * This class is used by PlanExporter tool (DERBY-4587)
 * in order to create HTML output of a query plan
 * using a plain XSL style sheet and a XML data of
 * a query plan.
 */
public class CreateHTMLFile {
	
	private static String xslStyleSheetName ="resources/vanilla_html.xsl";//default xsl

	/**
	 * 
	 * @param XMLFileName name of the XML file
	 * @param XSLSheetName name of the XSL file
	 * @param HTMLFile name of the HTML file
	 * @param def whether to use the default XSL or not
	 * @throws Exception
	 */
    public void getHTML(String XMLFileName, String XSLSheetName,
            String HTMLFile, boolean def) throws Exception{
//IC see: https://issues.apache.org/jira/browse/DERBY-4587
//IC see: https://issues.apache.org/jira/browse/DERBY-4758

        if(!(HTMLFile.toUpperCase()).endsWith(".HTML"))
            HTMLFile +=".html";

        TransformerFactory transFactory = TransformerFactory.newInstance();
        Transformer transformer;

        if(def){
            URL url=getClass().getResource(XSLSheetName);
            transformer =
                transFactory.newTransformer(new StreamSource(url.openStream()));
        }
        else{
            File style=new File(XSLSheetName);
            if(style.exists())
                transformer =
                    transFactory.newTransformer(new StreamSource(XSLSheetName));
            else{
                URL url=getClass().getResource(xslStyleSheetName);
                transformer =
                    transFactory.newTransformer(new StreamSource(url.openStream()));
            }
        }

        transformer.transform(new StreamSource(XMLFileName),
                new StreamResult(new FileOutputStream(HTMLFile)));
    }
}
