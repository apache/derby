/**
 * This class is the main entry point to the tool Graphical Query Explainer.
 * 
 */
package org.apache.derby.tools;

import java.io.File;
import java.security.AccessController;
import org.apache.derby.impl.tools.planexporter.AccessDatabase;
import org.apache.derby.impl.tools.planexporter.CreateHTMLFile;
import org.apache.derby.impl.tools.planexporter.CreateXMLFile;

/**
 * @author Nirmal
 *
 */
public class PlanExporter {

	private static String dbURL = null; //connection URL
	private static String xslStyleSheetName ="resources/vanilla_html.xsl";//default xsl
	private static final int xml=1;
	private static final int html=2;
	private static final int xsl=3;

	/**
	 * @param args 
	 * 1) database string eg: jdbc:derby:myDB --------- 
	 * 2) username ------------------------------------
	 * 3) password ------------------------------------
	 * 4) database schema -----------------------------
	 * 5) statement ID (36 characters) ----------------
	 * and user specified arguments.
	 */
	public static void main(String[] args) {

		try{
			if(args.length>6 && args.length<12 ){
				dbURL = args[0]+";create=false"+
				";user="+args[1]+";password="+args[2];

				AccessDatabase access = new AccessDatabase(dbURL, args[3], args[4]);
				access.createConnection();
				if(access.initializeDataArray()){
					access.createXMLFragment();
					access.markTheDepth();
					String stmt=access.statement();
					access.shutdown();
					
					//advanced XSL feature
					//possible occurrences are
					//-adv -xml {path} -xsl {path} or
					//-adv -xsl {path} -xml {path}
					if(args.length==10 && 
						args[5].equalsIgnoreCase("-adv")){
						int opt1=selectArg(args[6]);
						int opt2=selectArg(args[8]);
						if(opt1==1 && opt2==3){
							if(args[9].endsWith(".xsl")||
									args[9].endsWith(".XSL"))
								generateXML(access,args[7],stmt,args[9]);
							else
								generateXML(access,args[7],stmt,args[9]+".xsl");
						}
						else if(opt1==3 && opt2==1){
							if(args[7].endsWith(".xsl")||
									args[7].endsWith(".XSL"))
								generateXML(access,args[9],stmt,args[7]);
							else
								generateXML(access,args[9],stmt,args[7]+".xsl");
						}
						else
							printHelp();
					}
					//possible occurrences are -xml {path} or -html {path} 
					else if(args.length==7){
						int opt=selectArg(args[5]);
						if(opt==0 || opt==3)
							printHelp();
						else if(opt==1)
							generateXML(access,args[6],stmt,null);
						else{
							generateXML(access,"temp.xml",stmt,null);
							generateHTML("temp.xml",args[6],xslStyleSheetName,true);
							deleteFile("temp.xml");
						}
					}
					//possible occurrences are
					//-xml {path} and -html {path}
					//-html {path} and -xml {path}
					//-html {path} and -xsl {path}
					//-xsl {path} and -html {path}
					else if(args.length==9){
						int opt1=selectArg(args[5]);
						int opt2=selectArg(args[7]);
						if(opt1==0 || opt2==0)
							printHelp();
						else if(opt1==1 && opt2==2){
							generateXML(access,args[6],stmt,null);
							generateHTML(args[6],args[8],xslStyleSheetName,true);
						}
						else if(opt1==2 && opt2==1){
							generateXML(access,args[8],stmt,null);
							generateHTML(args[8],args[6],xslStyleSheetName,true);
						}
						else if(opt1==2 && opt2==3){
							generateXML(access,"temp.xml",stmt,null);
							generateHTML("temp.xml",args[6],args[8],false);
							deleteFile("temp.xml");
						}
						else if(opt1==3 && opt2==2){
							generateXML(access,"temp.xml",stmt,null);
							generateHTML("temp.xml",args[8],args[6],false);
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
					else{
						int opt1=selectArg(args[5]);
						int opt2=selectArg(args[7]);
						int opt3=selectArg(args[9]);
						if(opt1==0 || opt2==0 || opt3==0)
							printHelp();
						else if(opt1==1 && opt2==2 && opt3==3){
							generateXML(access,args[6],stmt,null);
							generateHTML(args[6],args[8],args[10],false);
						}
						else if(opt1==2 && opt2==3 && opt3==1){
							generateXML(access,args[10],stmt,null);
							generateHTML(args[10],args[6],args[8],false);
						}
						else if(opt1==3 && opt2==1 && opt3==2){
							generateXML(access,args[8],stmt,null);
							generateHTML(args[8],args[10],args[6],false);
						}
						else if(opt1==1 && opt2==3 && opt3==2){
							generateXML(access,args[6],stmt,null);
							generateHTML(args[6],args[10],args[8],false);
						}
						else if(opt1==2 && opt2==1 && opt3==3){
							generateXML(access,args[8],stmt,null);
							generateHTML(args[8],args[6],args[10],false);
						}
						else if(opt1==3 && opt2==2 && opt3==1){
							generateXML(access,args[10],stmt,null);
							generateHTML(args[10],args[8],args[6],false);
						}
						else
							printHelp();
					}					
				}
				else{
					System.out.println(
							"====================================================\n" +
							"--- An Error Occured: No Statistics has Captured ---\n" +
							"-- Possible reasons: 							   --\n" +
							"-- 1) The statement executed is a DDL statement.  --\n" +
							"-- Statistics will not capture for DDL statements --\n" +
							"-- by the Derby.                                  --\n" +
							"-- 2) The statement ID entered is incorrect.	   --\n" +
							"====================================================\n"
					);
				}
			}
			else{
				printHelp();
			}
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
			return xml;
		else if(arg.equalsIgnoreCase("-html"))
			return html;
		else if(arg.equalsIgnoreCase("-xsl"))
			return xsl;
		else
			return 0;
	}

	/**
	 * 
	 * @param access instance of AccessDatabase class
	 * @param arg path of XML
	 * @param stmt statement executed
	 * @param xsl name of the style sheet
	 * @throws Exception
	 */
	private static void generateXML(AccessDatabase access, 
			String arg, String stmt,String xsl) throws Exception{
		CreateXMLFile xmlFile = new CreateXMLFile(access);

		if(arg.endsWith(".xml") || arg.endsWith(".XML")){
			xmlFile.writeTheXMLFile(stmt,
					access.getData(),  
					arg, xsl);
		}
		else{
			xmlFile.writeTheXMLFile(stmt,
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

		if(arg.endsWith(".xml") || arg.endsWith(".XML")){
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
				"--   You can pass 7 arguments (minimum), or   --\n" +
				"--       9 arguments or 10 arguments or       --\n" +
				"-----------  11 arguments (maximum)  -----------\n" +
				"--         separated by a space.              --\n" +
				"---------------Mandatory Arguments--------------\n" +
				"1) database string eg: jdbc:derby:myDB ---------\n" +
				"2) username ------------------------------------\n" +
				"3) password ------------------------------------\n" +
				"4) database schema -----------------------------\n" +
				"5) statement ID (36 characters) ----------------\n" +
				"---------------Optional Arguments---------------\n" +
				"-----------Choose at least one option-----------\n" +
				"6) -xml {pathToXML} or -html {pathToHTML} ------\n" +
				"7) -xml {pathToXML} -html {pathToHTML} ---------\n" +
				"8) -xsl {pathToXSL} -html {pathToHTML} ---------\n" +
				"9) -xml {pathToXML} -xsl {pathToXSL} -----------\n" +
				"      -html {pathToHTML} -----------------------\n" +
				"10) -adv -xml {pathToXML} -xsl {pathToXSL} -----\n" +
				"================================================\n"
		);
	}

	public static void deleteFile(final String fileName) 
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
