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

    public void getHTML(String XMLFileName, String XSLSheetName,
            String HTMLFile, boolean def) throws Exception{

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
                URL url=getClass().getResource("resources/vanilla_html.xsl");
                transformer =
                    transFactory.newTransformer(new StreamSource(url.openStream()));
            }
        }

        transformer.transform(new StreamSource(XMLFileName),
                new StreamResult(new FileOutputStream(HTMLFile)));
    }
}
