/*

   Derby - Class org.apache.derby.tools.PlanExporter

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

package org.apache.derby.tools;

import java.io.File;
import java.security.AccessController;
import org.apache.derby.impl.tools.planexporter.AccessDatabase;
import org.apache.derby.impl.tools.planexporter.CreateHTMLFile;
import org.apache.derby.impl.tools.planexporter.CreateXMLFile;

/**
 * This class is the main entry point to the tool Graphical Query Explainer.
 */
public class PlanExporter {

    private static String dbURL = null; //connection URL
    private static String xslStyleSheetName ="resources/vanilla_html.xsl";//default xsl
    private static final int XML=1;
    private static final int HTML=2;
    private static final int XSL=3;

    /**
     * @param args
     * 1) database URL eg: jdbc:derby:myDB ---------
     * 2) database schema -----------------------------
     * 3) statement ID (36 characters) ----------------
     * and user specified arguments.
     */
    public static void main(String[] args) {

        try{
            if(args.length>4 && args.length<10 ){
                dbURL = args[0];

                AccessDatabase access = new AccessDatabase(dbURL, args[1], args[2]);
                access.createConnection();
                
                if(access.verifySchemaExistance()){
                
                	if(access.initializeDataArray()){
                		access.createXMLFragment();
                		access.markTheDepth();
                		String stmt=access.statement();
                		String time=access.time();
                		access.closeConnection();

                		//advanced XSL feature
                		//possible occurrences are
                		//-adv -xml {path} -xsl {path} or
                		//-adv -xsl {path} -xml {path}
                		if(args.length==8 &&
                				args[3].equalsIgnoreCase("-adv")){
                			int opt1=selectArg(args[4]);
                			int opt2=selectArg(args[6]);
                			if(opt1==1 && opt2==3){
                				if(args[7].toUpperCase().endsWith(".XSL"))
                					generateXML(access,args[5],stmt,time,args[7]);
                				else
                					generateXML(access,args[5],stmt,time,args[7]+".xsl");
                			}
                			else if(opt1==3 && opt2==1){
                				if(args[5].toUpperCase().endsWith(".XSL"))
                					generateXML(access,args[7],stmt,time,args[5]);
                				else
                					generateXML(access,args[7],stmt,time,args[5]+".xsl");
                			}
                			else
                				printHelp();
                		}
                		//possible occurrences are -xml {path} or -html {path}
                		else if(args.length==5){
                			int opt=selectArg(args[3]);
                			if(opt==0 || opt==3)
                				printHelp();
                			else if(opt==1)
                				generateXML(access,args[4],stmt,time,null);
                			else{
                				generateXML(access,"temp.xml",stmt,time,null);
                				generateHTML("temp.xml",args[4],xslStyleSheetName,true);
                				deleteFile("temp.xml");
                			}
                		}
                		//possible occurrences are
                		//-xml {path} and -html {path}
                		//-html {path} and -xml {path}
                		//-html {path} and -xsl {path}
                		//-xsl {path} and -html {path}
                		else if(args.length==7){
                			int opt1=selectArg(args[3]);
                			int opt2=selectArg(args[5]);
                			if(opt1==0 || opt2==0)
                				printHelp();
                			else if(opt1==1 && opt2==2){
                				generateXML(access,args[4],stmt,time,null);
                				generateHTML(args[4],args[6],xslStyleSheetName,true);
                			}
                			else if(opt1==2 && opt2==1){
                				generateXML(access,args[6],stmt,time,null);
                				generateHTML(args[6],args[4],xslStyleSheetName,true);
                			}
                			else if(opt1==2 && opt2==3){
                				generateXML(access,"temp.xml",stmt,time,null);
                				generateHTML("temp.xml",args[4],args[6],false);
                				deleteFile("temp.xml");
                			}
                			else if(opt1==3 && opt2==2){
                				generateXML(access,"temp.xml",stmt,time,null);
                				generateHTML("temp.xml",args[6],args[4],false);
                				deleteFile("temp.xml");
                			}
                			else
                				printHelp();
                		}
                		//possible occurrences are
                		//-xml {path} and -html {path} and -xsl {path}
                		//-html {path} and -xsl {path} and -xml {path}
                		//-xsl {path} and -xml {path} and -html {path}
                		//-xml {path} and -xsl {path} and -html {path}
                		//-html {path} and -xml {path} and -xsl {path}
                		//-xsl {path} and -html {path} and -xml {path}
                		else if(args.length==9){
                			int opt1=selectArg(args[3]);
                			int opt2=selectArg(args[5]);
                			int opt3=selectArg(args[7]);
                			if(opt1==0 || opt2==0 || opt3==0)
                				printHelp();
                			else if(opt1==1 && opt2==2 && opt3==3){
                				generateXML(access,args[4],stmt,time,null);
                				generateHTML(args[4],args[6],args[8],false);
                			}
                			else if(opt1==2 && opt2==3 && opt3==1){
                				generateXML(access,args[8],stmt,time,null);
                				generateHTML(args[8],args[4],args[6],false);
                			}
                			else if(opt1==3 && opt2==1 && opt3==2){
                				generateXML(access,args[6],stmt,time,null);
                				generateHTML(args[6],args[8],args[4],false);
                			}
                			else if(opt1==1 && opt2==3 && opt3==2){
                				generateXML(access,args[4],stmt,time,null);
                				generateHTML(args[4],args[8],args[6],false);
                			}
                			else if(opt1==2 && opt2==1 && opt3==3){
                				generateXML(access,args[6],stmt,time,null);
                				generateHTML(args[6],args[4],args[8],false);
                			}
                			else if(opt1==3 && opt2==2 && opt3==1){
                				generateXML(access,args[8],stmt,time,null);
                				generateHTML(args[8],args[6],args[4],false);
                			}
                			else
                				printHelp();
                		}
                		else
                			printHelp();
                	}
                	else{
                		System.out.println(
                				"====================================================\n" +
                				"--- An Error Occured: No Statistics has Captured ---\n" +
                				"-- Possible reasons:                              --\n" +
                				"-- 1) The statement executed is a DDL statement.  --\n" +
                				"-- Statistics will not capture for DDL statements --\n" +
                				"-- by the Derby.                                  --\n" +
                				"-- 2) The statement ID entered is incorrect.      --\n" +
                				"-- 3) Schema specified does not exist.            --\n" +
                				"====================================================\n"
                		);
                	}
                }
                else{
                	System.out.println(
            				"====================================================\n" +
            				"---              An Error Occured:               ---\n" +
            				"-------   Specified Schema does not exist.   -------\n" +
            				"======================================================");
                }
            }
            else
                printHelp();

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * Reading the user's option
     * @param arg user's option
     * @return the argument type
     */
    private static int selectArg(String arg){
        if(arg.equalsIgnoreCase("-xml"))
            return XML;
        else if(arg.equalsIgnoreCase("-html"))
            return HTML;
        else if(arg.equalsIgnoreCase("-xsl"))
            return XSL;
        else
            return 0;
    }

    /**
     *
     * @param access instance of AccessDatabase class
     * @param arg path of XML
     * @param stmt statement executed
     * @param time time which the statement was executed
     * @param xsl name of the style sheet
     * @throws Exception
     */
    private static void generateXML(AccessDatabase access,
            String arg, String stmt, String time, String xsl) throws Exception{
        CreateXMLFile xmlFile = new CreateXMLFile(access);

        if(arg.toUpperCase().endsWith(".XML")){
            xmlFile.writeTheXMLFile(stmt, time,
                    access.getData(),
                    arg, xsl);
        }
        else{
            xmlFile.writeTheXMLFile(stmt, time,
                    access.getData(),
                    arg.concat(".xml"),
                    xsl);
        }
    }

    /**
     *
     * @param arg path to xml
     * @param path path of HTML
     * @param style path to xsl
     * @param def whether the default xsl or not
     * @throws Exception
     */
    private static void generateHTML(String arg, String path,
            String style, boolean def) throws Exception{
        CreateHTMLFile htmlFile = new CreateHTMLFile();

        if(arg.toUpperCase().endsWith(".XML")){
            htmlFile.getHTML(arg, style, path, def);
        }
        else{
            htmlFile.getHTML(arg.concat(".xml"), style, path, def);
        }
    }

    private static void printHelp(){
        System.out.println
        (
                "================================================\n" +
                "-------------- PlanExporter Tool ---------------\n" +
                "--   You can pass 5 arguments (minimum), or   --\n" +
                "--       7 arguments or 8 arguments or        --\n" +
                "-----------  9 arguments (maximum)  ------------\n" +
                "--         separated by a space.              --\n" +
                "---------------Mandatory Arguments--------------\n" +
                "1) database URL --------------------------------\n" +
                "2) database schema -----------------------------\n" +
                "3) statement ID (36 characters) ----------------\n" +
                "---------------Optional Arguments---------------\n" +
                "-----------Choose at least one option-----------\n" +
                "a) -xml {pathToXML} or -html {pathToHTML} ------\n" +
                "b) -xml {pathToXML} -html {pathToHTML} ---------\n" +
                "c) -xsl {pathToXSL} -html {pathToHTML} ---------\n" +
                "d) -xml {pathToXML} -xsl {pathToXSL} -----------\n" +
                "      -html {pathToHTML} -----------------------\n" +
                "e) -adv -xml {pathToXML} -xsl {pathToXSL} ------\n" +
                "================================================\n"
        );
    }

    private static void deleteFile(final String fileName)
    {
        AccessController.doPrivileged
        (new java.security.PrivilegedAction() {

            public Object run() {
                File delFile = new File(fileName);
                if (!delFile.exists())
                    return null;
                delFile.delete();
                return null;
            }
        }
        );

    }

}
