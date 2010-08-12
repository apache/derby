package org.apache.derby.impl.tools.planexporter;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import org.apache.derby.impl.tools.planexporter.AccessDatabase;

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
    throws IOException, PrivilegedActionException {

        String defaultXML = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n";
        String embedXSL="";
        if(xsl_sheet_name != null)
            embedXSL ="<?xml-stylesheet type=\"text/xsl\" href=\""
                        +xsl_sheet_name+"\"?>\n";
        String comment = "<!-- Apache Derby Query Explainer (DERBY-4587)-->\n";
        String parentTagStart = "<plan>\n";
        String parentTagEnd = "</plan>\n";
        String childTagStart = "<details>\n";
        String childTagEnd = "</details>\n";

        DataOutputStream dos =
            new DataOutputStream(
                    new BufferedOutputStream(
                            (OutputStream)AccessController.doPrivileged
                            (new java.security.PrivilegedExceptionAction(){
                                public Object run() throws IOException{
                                    return new FileOutputStream(file_name);
                                }
                            })));

        dos.write(defaultXML.getBytes());
        dos.write(embedXSL.getBytes());
        dos.write(comment.getBytes());
        dos.write(parentTagStart.getBytes());
        dos.write((access.indent(0)+stmt).getBytes());
        dos.write((access.indent(0)+time).getBytes());
        dos.write((access.indent(0)+access.stmtID()).getBytes());
        dos.write((access.indent(0)+childTagStart).getBytes());
        dos.write(access.getXmlString().getBytes());
        dos.write((access.indent(0)+childTagEnd).getBytes());
        dos.write(parentTagEnd.getBytes());
        dos.close();
    }
}
