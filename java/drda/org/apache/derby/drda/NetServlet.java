/*

   Derby - Class org.apache.derby.drda.NetServlet

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

package org.apache.derby.drda;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;

/**
    This servlet can be used to start Derby Network Server from a remote location.
    <P>
    These servlet configuration parameters are understood by this servlet.
    <UL>
    <LI><PRE>portNumber</PRE> - Port number to use. The default is 1527.</LI>
    <LI><PRE>startNetworkServerOnInit</PRE> - Starts the Derby Network Server at servlet 
            initialization if 'true'.</LI>
    <LI><PRE>tracingDirectory</PRE> - Directory for trace files</LI>
    </UL>

*/
public class NetServlet extends HttpServlet {
    private final static int MAX_CONNECT_TRYS = 20;
    private final static String SERVLET_PROP_MESSAGES =  "org.apache.derby.loc.drda.servlet";
    private final static String SERVLET_ADDRESS = "derbynet";
    private final static String[] knownLang =
    { "cs","en","es","de_DE","fr","hu","it", "ja_JP","ko_KR","pl","pt_BR","ru","zh_CN","zh_TW" };

    // set at initialization
    private String host = "localhost";
    private int portNumber = 1527;

    // can be overridden by trips through doGet()
    private volatile String tracingDirectory;
    private volatile boolean logStatus= false;  /* Logging off */
    private volatile boolean traceStatus = false;   /* Tracing off */

    private final static int NOT_GIVEN = -2;
    private final static int INVALID = -3;

    private NetworkServerControl server;

    /**
        Initialize the servlet.
        Configuration parameters:
        <UL>
        <LI><PRE>portNumber</PRE> - Port number</LI>
        <LI><PRE>host</PRE> - Host name</LI>
        <LI><PRE>traceDirectory</PRE> - location of trace directory</LI>
        <LI><PRE>startNetworkServerOnInit</PRE> - start the server on initialization</LI>
        </UL>
    */
    public void init(ServletConfig config)
        throws ServletException
    {
        
        String port = config.getInitParameter("portNumber");
        if (port != null) {
            int p = Integer.parseInt(port);
            if (p > 0)
                portNumber = p;
        }
        String hostName = config.getInitParameter("host");
        if (hostName != null)
            host = hostName;

        this.tracingDirectory = config.getInitParameter("tracingDirectory");
        
        if ( this.tracingDirectory == null ) {
            this.tracingDirectory = "";
        }

        String startup = config.getInitParameter("startNetworkServerOnInit");

        // test if the server is already running
        try {
            //don't send output to console
            if (server == null) {
                server = new NetworkServerControl(InetAddress.getByName(host), portNumber);
                // assert this.tracingDirectory != null
                if  ( ! this.tracingDirectory.trim().equals("")) {
                    server.setTraceDirectory(this.tracingDirectory);
                }
            }
            
            if (isServerStarted(server,1))
                return;
        } catch (Exception e) {}

        if (startup != null) {
            boolean start = Boolean.valueOf(startup).booleanValue();
            if (start)
            {
                LocalizedResource langUtil = new LocalizedResource(SERVLET_PROP_MESSAGES);
                runServer(langUtil, null, null);
                return;
            }
        }
    }

    /**
        Get the form of NetServlet. Provides buttons and forms to control the
        Network server.
    */
    public synchronized void doGet (HttpServletRequest request,
                                    HttpServletResponse response)
            throws ServletException, IOException
    {
        String logOnMessage;
        String logOffMessage;
        String traceOnMessage;
        String traceOffMessage;
        String traceOnOffMessage;
        String startMessage;
        String stopMessage;
        String returnMessage;
        String traceSessionMessage;
        String traceDirMessage;
        String netParamMessage;

        LocalizedResource langUtil;
        String locale[] = new String[ 1 ];
        
        langUtil = getCurrentAppUI(request, locale);
        response.setContentType("text/html; charset=UTF-8");
        
        //prevent caching of the servlet since contents can change - beetle 4649
        response.setHeader("Cache-Control", "no-cache,no-store");

        String formTarget = request.getContextPath() + request.getServletPath();
        String formHeader = "<form enctype='multipart/form-data; charset=UTF-8'"
                + " action='" + formTarget + "'>";

        PrintWriter out = new PrintWriter
            ( new OutputStreamWriter(response.getOutputStream(), "UTF8"),true );
        
        //inialize messages
        logOnMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_LogOn"));
        logOffMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_LogOff"));
        traceOnMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_TraceOn"));
        traceOffMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_TraceOff"));
        startMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_Start"));
        stopMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_Stop"));
        traceSessionMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_TraceSessButton"));
        traceOnOffMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_TraceOnOff"));
        returnMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_Return"));
        traceDirMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_TraceDir"));
        netParamMessage = escapeSingleQuotes(langUtil.getTextMessage("SRV_NetParam"));

        printBanner(langUtil, out);
        // set up a server we can use
        if (server == null) {
            try {
                server = new NetworkServerControl();
            }catch (Exception e) {
                printErrorForm(langUtil, e, returnMessage, out);
                return;
            }
        }
        server.setClientLocale( locale[ 0 ] );
        String form = getForm(request);
        String doAction = getDoAction(request);
        // if doAction is set, use it to determine form
        if (doAction != null )
        {
            if (doAction.equals(traceOnOffMessage))
                form = traceSessionMessage;
            else
                form = doAction;
        }
        // if no form, determine form based on server status
        boolean serverStatus = getServerStatus();
        if (form == null)
        {
            if (serverStatus)
                form = startMessage;
            else
                form = stopMessage;
        }
        else if (form.equals(startMessage))
        {
            if (!serverStatus)  {
                runServer(langUtil, returnMessage, out);
            }
        }
        else if (form.equals(stopMessage))
        {
            if (serverStatus)   {
                shutdownServer(langUtil, returnMessage, out);
            }
            setDefaults();
                    
        }
        else if (form.equals(returnMessage))
        {
            // check if server is still running and use that to determine which form
            if (serverStatus)
            {
                form = startMessage;
            }
            else
            {
                form = stopMessage;
            }
        }

        out.println( formHeader);
        // display forms

        form = escapeSingleQuotes(form);
        doAction = escapeSingleQuotes(doAction);
        if (form.equals(startMessage))
        {
            String logButton = getLogging(request);
            String traceButton = getTrace(request);
            if (logButton !=  null && logButton.equals(logOnMessage))
            {
                if (logging(langUtil, true, returnMessage, out))
                    logStatus = true;
            }
            if (logButton !=  null && logButton.equals(logOffMessage))
            {
                if (logging(langUtil, false, returnMessage, out))
                    logStatus = false;
            }
            if (traceButton !=  null && traceButton.equals(traceOnMessage))
            {
                if (traceAll(langUtil, true, returnMessage, out))
                    traceStatus = true;
            }
            if (traceButton !=  null && traceButton.equals(traceOffMessage))
            {
                if (traceAll(langUtil, false, returnMessage, out))
                    traceStatus = false;
            }
            displayCurrentStatus(langUtil, returnMessage, out);
            out.println( "<h4>"+langUtil.getTextMessage("SRV_StopButton")+"</h4>" );
            out.println( "<INPUT type=submit name=form value='"+ stopMessage + "'>" );

            out.println( "<h4>"+langUtil.getTextMessage("SRV_LogButton2")+"</h4>" );

            if (logStatus)
            {
                out.println( "<INPUT type=submit name=logform value='"+logOffMessage + "'>" );
            }
            else
            {
                out.println( "<INPUT type=submit name=logform value='"+logOnMessage + "'>" );
            }
            out.println( "<h4>"+langUtil.getTextMessage("SRV_TraceButton2")+"</h4>" );
            if (traceStatus)
            {
                out.println( "<INPUT type=submit name=traceform value='"+traceOffMessage+ "'>" );
            }
            else
            {
                out.println( "<INPUT type=submit name=traceform value='"+traceOnMessage + "'>" );
            }

            out.println( "<h4>"+langUtil.getTextMessage("SRV_TraceSession")+"</h4>" );
            out.println( "<INPUT type=submit name=form value='"+ traceSessionMessage + "'>" );
            out.println( "<h4>"+langUtil.getTextMessage("SRV_TraceDirButton")+"</h4>" );
            out.println( "<INPUT type=submit name=form value='"+ traceDirMessage + "'>" );
            out.println( "<h4>"+langUtil.getTextMessage("SRV_ThreadButton")+"</h4>" );
            out.println( "<INPUT type=submit name=form value='"+ netParamMessage+ "'>" );
        }
        else if (form.equals(stopMessage))
        {

            printAsContentHeader(langUtil.getTextMessage("SRV_NotStarted"), out);
            String logButton = getLogging(request);
            String traceButton =  getTrace(request);
            if (logButton !=  null && logButton.equals(logOnMessage))
                logStatus = true;
            if (logButton !=  null && logButton.equals(logOffMessage))
                logStatus = false;
            if (traceButton !=  null && traceButton.equals(traceOnMessage))
                traceStatus = true;
            if (traceButton !=  null && traceButton.equals(traceOffMessage))
                traceStatus = false;
            if (logStatus)
            {
                out.println( "<h4>"+langUtil.getTextMessage("SRV_LogOffButton")+"</h4>" );
                out.println( "<INPUT type=submit name=logform value='"+logOffMessage + "'>" );
            }
            else
            {
                out.println( "<h4>"+langUtil.getTextMessage("SRV_LogOnButton")+"</h4>" );
                out.println( "<INPUT type=submit name=logform value='"+logOnMessage + "'>" );
            }
            if (traceStatus)
            {
                out.println( "<h4>"+langUtil.getTextMessage("SRV_TraceOffButton")+"</h4>" );
                out.println( "<INPUT type=submit name=traceform value='"+traceOffMessage + "'>" );
            }
            else
            {
                out.println( "<h4>"+langUtil.getTextMessage("SRV_TraceOnButton")+"</h4>" );
                out.println( "<INPUT type=submit name=traceform value='"+traceOnMessage + "'>" );
            }
            out.println( "<h4>"+langUtil.getTextMessage("SRV_StartButton")+"</h4>" );
            out.println( "<INPUT type=submit name=form value='"+startMessage+ "'>" );
        }
        else if (form.equals(traceSessionMessage))
        {
            if (doAction != null)
            {
                if (doAction.equals(traceOnOffMessage))
                {
                    String sessionid = request.getParameter("sessionid");
                    Integer session;
                    try {
                        session = Integer.valueOf(sessionid);
                    } catch (NumberFormatException nfe) {
                        printErrorForm(langUtil,
                            langUtil.getTextMessage("SRV_InvalidVal",
                            sessionid, langUtil.getTextMessage("SRV_SessionID")),
                                       returnMessage, out);
                        return;
                    }
                    Properties p;
                    try {
                        p = server.getCurrentProperties();
                    } catch (Exception e) {
                        printErrorForm(langUtil, e, returnMessage, out);
                        return;
                    }
                    // if it's on, turn it off, if its off, turn it on
                    boolean val;
                    if (p.getProperty(Property.DRDA_PROP_TRACE+sessionid) != null)
                        val = false;
                    else
                        val = true;
                    if (traceSession(langUtil, val, session.intValue(),
                            returnMessage, out))
                    {
                        out.println( "<h4>" + langUtil.getTextMessage(
                                val ? "SRV_StatusTraceNoOn"
                                    : "SRV_StatusTraceNoOff",
                                session.toString()) + "</h4>");
                    }
                    else
                        return;
                        
                }
            }
            printAsContentHeader(langUtil.getTextMessage("SRV_TraceSessButton"), out);
            out.println( "<h4>" + getHtmlLabelledMessageInstance(langUtil,
                "SRV_SessionID", "sessionId") + "</h4>");
            out.println( "<INPUT type=text name=sessionid size=10 maxlength=10 " +
                "id='sessionId' value=''>");
            out.println( "<h4> </h4>");
            out.println( "<INPUT type=submit name=doaction value='"+traceOnOffMessage+ "'>" );
            out.println( "<INPUT type=submit name=form value='"+returnMessage+ "'>" );
        }
        else if (form.equals(traceDirMessage))
        {
            boolean set = false;
            String traceDirectory = null;
            printAsContentHeader(traceDirMessage, out);
            if (doAction != null)
            {
                if (doAction.equals(traceDirMessage))
                {
                    traceDirectory = getParam(request, "tracedirectory");
                    if (traceDirectory(langUtil, traceDirectory,
                                       returnMessage, out) )
                        set = true;
                    else
                        return;
                    
                }
            }
            if (set)
            {
                out.println("<h2>" + langUtil.getTextMessage("SRV_TraceDirDone",
                        escapeHTML(traceDirectory)) + "</h2>");
                out.println( "<INPUT type=submit name=form value='"+returnMessage+"'>" );
            }
            else
            {
                out.println( "<h4>" + getHtmlLabelledMessageInstance(langUtil,
                    "SRV_TraceDir", "tracedir") + "</h4>");
                out.println( "<INPUT type=text name=tracedirectory size=60 maxlength=256 " +
                    "id='tracedir' value='"+escapeHTML(tracingDirectory)+"'>");
                out.println( "<h4> </h4>");
                out.println( "<INPUT type=submit name=doaction value='"+traceDirMessage+ "'>" );
                out.println( "<INPUT type=submit name=form value='"+returnMessage+ "'>" );
            }
        }
        else if (form.equals(netParamMessage))
        {
            int maxThreads;
            int timeSlice;
            String maxName = langUtil.getTextMessage("SRV_NewMaxThreads");
            String sliceName = langUtil.getTextMessage("SRV_NewTimeSlice");
            try {
                Properties p = server.getCurrentProperties();
                String val = p.getProperty(Property.DRDA_PROP_MAXTHREADS);
                maxThreads= Integer.parseInt(val);
                val = p.getProperty(Property.DRDA_PROP_TIMESLICE);
                timeSlice= Integer.parseInt(val);
            } catch (Exception e) {
                printErrorForm(langUtil, e, returnMessage, out);
                return;
            }
            if (doAction != null && doAction.equals(netParamMessage))
            {
                int newMaxThreads = getIntParameter(request, "newmaxthreads", 
                    "SRV_NewMaxThreads", langUtil, returnMessage, out);
                int newTimeSlice = (newMaxThreads == INVALID) ? NOT_GIVEN :
                    getIntParameter(request, "newtimeslice", "SRV_NewTimeSlice", langUtil, 
                        returnMessage, out);
                if ((newMaxThreads == INVALID) || (newTimeSlice == INVALID))
                    return;
                else if (!(newMaxThreads == NOT_GIVEN && newTimeSlice == NOT_GIVEN))
                {
                    if (newMaxThreads != NOT_GIVEN)
                        maxThreads = newMaxThreads;
                    if (newTimeSlice != NOT_GIVEN)
                        timeSlice = newTimeSlice;
                    if (!setNetParam(langUtil, maxThreads, timeSlice,
                            returnMessage, out))
                        return;
                }
            }
            
            out.println(formHeader);
            printAsContentHeader(netParamMessage, out);
            out.println( "<h4>"+langUtil.getTextMessage("SRV_MaxThreads", Integer.toString(maxThreads) +"</h4>"));
            out.println( "<h4>"+langUtil.getTextMessage("SRV_TimeSlice", Integer.toString(timeSlice) +"</h4>"));
            out.println( "<h4> </h4>");
            out.println( "<h4> <label for='newmaxthreads'>"+maxName+"</label> </h4>");
            out.println( "<INPUT type=text name=newmaxthreads size=10 maxlength=10 " +
                "id='newmaxthreads' value=''>" );
            out.println( "<h4> <label for='newslice'>"+sliceName+"</label> </h4>");
            out.println( "<INPUT type=text name=newtimeslice size=10 maxlength=10 " +
                "id='newslice' value=''>" );
            out.println( "<h4> </h4>");
            out.println( "<INPUT type=submit name=doaction value='"+netParamMessage+ "'>" );
            out.println( "<INPUT type=submit name=form value='"+returnMessage+ "'>" );
        }
        else
        {
            System.out.println("Internal Error: Unknown form, "+ form);
            out.println("Internal Error: Unknown form");
        }

        out.println( "</body>" );
        out.println( "</html>" ); 
    }

    /**
        Get the form of NetServlet. Provides a buttons and form to control the
        Network server

    */
    public void doPost (HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException
    {
        // simply call the doGet()
        doGet(request, response);
    }

    private String getForm(HttpServletRequest request)  throws java.io.IOException{
        return getParam(request, "form");
    }
    private String getDoAction(HttpServletRequest request) throws java.io.IOException {
        return getParam(request, "doaction");
    }
    private String getLogging(HttpServletRequest request) throws java.io.IOException {
        return getParam(request, "logform");
    }
    private String getTrace(HttpServletRequest request) throws java.io.IOException {
        return getParam(request, "traceform");
    }

    /**
     *  get UTF8 parameter value and decode international characters
     *  @param request   HttpServletRequest
     *  @param paramName  Parameter name
     *  @return decoded String
     */
    private String getParam(HttpServletRequest request, String paramName) throws
    java.io.IOException { 
                
        String value = request.getParameter(paramName);
        if (value != null) {
            return new String(value.getBytes("ISO-8859-1"),"UTF8");
        } else {
            return null;
        }
    }

    /**
     *  Start the network server and attempt to connect to it before
     *  returning
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param returnMessage localized continue message for continue button on error form
     * @param out Form PrintWriter
     * @exception ServletException throws an exception if error in starting the 
     *      Network Server during initialization
     */
    private void runServer
        ( LocalizedResource localUtil, String returnMessage, PrintWriter out )
        throws ServletException
    {
        final Runnable service = new Runnable() {
            public void run() {
                try {
                    //Echo server output to console
                    NetworkServerControl runserver = new
                        NetworkServerControl(InetAddress.getByName(host),
                                             portNumber);
                    runserver.start(null);
                }
                catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        };
        Thread servThread = null;
        try {
            servThread = AccessController.doPrivileged(
                                new PrivilegedExceptionAction<Thread>() {
                                    public Thread run() throws Exception
                                    {
                                        return new Thread(service);
                                    }
                                }
                            );
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        servThread.start();

        // try to connect to server
        try {
            boolean connectWorked = false;
            int t = 0;
            do
            {
                t++;
                try {
                        Thread.sleep(100);
                } catch (InterruptedException ie) {
                    throw new ServletException(localUtil.getTextMessage("SRV_Interupt"));
                }
                try {
                    if (isServerStarted(server,1))
                        connectWorked = true;
                } catch (Exception e) {} //ignore error we'll just try again
                
            }while (!connectWorked && t < MAX_CONNECT_TRYS);
            if (t >= MAX_CONNECT_TRYS)
                throw new Exception(localUtil.getTextMessage("SRV_MaxTrys",
                     Integer.toString(MAX_CONNECT_TRYS)));
            // turn logging on if required
            if (logStatus)
                server.logConnections(true);
            // turn tracing on
            if (traceStatus)
                server.trace(true);
        }catch (Exception e) {
            if (out != null)
                printErrorForm(localUtil, e, returnMessage, out);
            else
                throw new ServletException(e.getMessage());
        }
    }
    /**
     *  Display an error form
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param e     Exception to be displayed
     * @param returnMessage localized continue message for continue button on error form
     * @param out Form PrintWriter
     */
    private void printErrorForm
        (
         LocalizedResource localUtil,
         Exception e,
         String returnMessage,
         PrintWriter out
         )
    {
        printAsContentHeader(localUtil.getTextMessage("SRV_NetworkServerError"), out);
        out.println( "<h4>" +
                localUtil.getTextMessage(
                    "SRV_Message", escapeHTML(e.getMessage())) + "</h4>" );
        out.println( "<INPUT type=submit name=form value='"+returnMessage+"'>" );
        out.println( "</body>" );
        out.println( "</html>" );
    }
    /**
     *  Display an error form
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param msg   String to be displayed
     * @param out Form PrintWriter
     * @param returnMessage localized continue message for continue button on error form
     */
    private void printErrorForm
        (
         LocalizedResource localUtil,
         String msg,
         String returnMessage,
         PrintWriter out
         )
    {
        printAsContentHeader(localUtil.getTextMessage("SRV_NetworkServerError"), out);
        out.println(
                "<h4>" +
                localUtil.getTextMessage("SRV_Message", escapeHTML(msg)) +
                "</h4>" );
        out.println( "<INPUT type=submit name=form value='"+returnMessage+"'>" );
        out.println( "</body>" );
        out.println( "</html>" ); 
    }
    /**
     *  Display the current Network server status
     *
     * @param localUtil     LocalizedResource to use for localizing messages
     * @param returnMessage localized continue message for continue button on error form
     * @param out Form PrintWriter
     */
    private void displayCurrentStatus
        (
         LocalizedResource localUtil,
         String returnMessage,
         PrintWriter out
         )
    {
        try {

            printAsContentHeader(localUtil.getTextMessage("SRV_Started"), out);
            Properties p = server.getCurrentProperties();
            String val = p.getProperty(Property.DRDA_PROP_LOGCONNECTIONS);
            if (val.equals("true"))
                logStatus = true;
            else
                logStatus = false;
            if (logStatus)
                out.println( "<h4>"+localUtil.getTextMessage("SRV_StatusLogOn")+"</h4>");
            else
                out.println( "<h4>"+localUtil.getTextMessage("SRV_StatusLogOff")+"</h4>");
            val = p.getProperty(Property.DRDA_PROP_TRACEALL);
            if (val.equals("true"))
                traceStatus = true;
            else
                traceStatus = false;
            if (traceStatus)
                out.println( "<h4>"+localUtil.getTextMessage("SRV_StatusTraceOn")+"</h4>");
            else
                out.println( "<h4>"+localUtil.getTextMessage("SRV_StatusTraceOff")+"</h4>");
            val = p.getProperty(Property.DRDA_PROP_PORTNUMBER);
            out.println( "<h4>"+localUtil.getTextMessage("SRV_PortNumber", val)+"</h4>");
            
        }
        catch (Exception e) {
            printErrorForm(localUtil, e, returnMessage, out);
        }
    }
    /**
     *  Get the currrent server status by using test connection
     *
     * @return true if server is up and reachable; false; otherwise
     */
    private boolean getServerStatus()
    {
        try {
            
            if (isServerStarted(server,1))
                return true;
        } catch (Exception e) {}
        return false;
    }
    /**
     *  Shutdown the network server
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param returnMessage localized continue message for continue button on error form
     * @param out Form PrintWriter
     * @return true if succeeded; false; otherwise
     */
    private boolean shutdownServer
        (
         LocalizedResource localUtil,
         String returnMessage,
         PrintWriter out
         )
    {
        boolean retval = false;
        try {
            server.shutdown();
            retval = true;
        } catch (Exception e) 
        {
            printErrorForm(localUtil, e, returnMessage, out);
        }
        return retval;
    }
    /**
     *  Turn logging of connections on
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param returnMessage localized continue message for continue button on error form
     * @param out Form PrintWriter
     * @return true if succeeded; false; otherwise
     */
    private boolean logging
        (
         LocalizedResource localUtil,
         boolean val,
         String returnMessage,
         PrintWriter out
         )
    {
        boolean retval = false;
        try {
            server.logConnections(val);
            retval = true;
        } catch (Exception e) 
        {
            printErrorForm(localUtil, e, returnMessage, out);
        }
        return retval;
    }
    /**
     *  Change tracing for all sessions
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param val   if true, turn tracing on, if false turn it off
     * @param returnMessage localized continue message for continue button on error form
     * @param out Form PrintWriter
     * @return true if succeeded; false; otherwise
     */
    private boolean traceAll
        (
         LocalizedResource localUtil,
         boolean val,
         String returnMessage,
         PrintWriter out
         )
    {
        boolean retval = false;
        try {
            server.trace(val);
            retval = true;
        } catch (Exception e) 
        {
            printErrorForm(localUtil, e, returnMessage, out);
        }
        return retval;
    }
    /**
     *  Change tracing for a given session
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param val   if true, turn tracing on, if false turn it off
     * @param session   session to trace
     * @param returnMessage localized continue message for continue button on error form
     * @param out Form PrintWriter
     * @return true if succeeded; false; otherwise
     */
    private boolean traceSession
        (
         LocalizedResource localUtil,
         boolean val,
         int session,
         String returnMessage,
         PrintWriter out
         )
    {
        boolean retval = false;
        try {
            server.trace(session, val);
            retval = true;
        } catch (Exception e) 
        {
            printErrorForm(localUtil, e, returnMessage, out);
        }
        return retval;
    }

    /**
     * Set trace directory
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param traceDirectory    directory for trace files
     * @param returnMessage     localized continue message for continue button on error form
     * @param out Form PrintWriter
     * @return true if succeeded; false; otherwise
     */
    private boolean traceDirectory
        (
         LocalizedResource localUtil,
         String traceDirectory,
         String returnMessage,
         PrintWriter out
         )
    {
        boolean retval = false;

        if ((traceDirectory == null) || traceDirectory.equals("")) {
            printErrorForm(localUtil,
                localUtil.getTextMessage("SRV_MissingParam",
                                         localUtil.getTextMessage("SRV_TraceDir")), returnMessage, out);

            return retval;
        }

        try {
            this.tracingDirectory = traceDirectory;
            server.setTraceDirectory(traceDirectory);
            retval = true;
        } catch (Exception e) 
        {
            printErrorForm(localUtil, e, returnMessage, out);
        }
        return retval;
    }

    /**
     * Set Network server parameters
     *
     * @param localUtil LocalizedResource to use to translate messages
     * @param max               maximum number of threads
     * @param slice             time slice for each connection
     * @param returnMessage     localized continue message for continue button on error form
     * @param out Form PrintWriter
     * @return true if succeeded; false; otherwise
     */
    private boolean setNetParam
        (
         LocalizedResource localUtil,
         int max,
         int slice,
         String returnMessage,
         PrintWriter out
         )
    {
        boolean retval = false;

        try {
            server.setMaxThreads(max);
            server.setTimeSlice(slice);
            retval = true;
        } catch (Exception e) 
        {
            printErrorForm(localUtil, e, returnMessage, out);
        }
        return retval;
    }


    /** 
     * Set defaults for logging and tracing (both off)
     */
    private void setDefaults()
    {
        logStatus = false;
        traceStatus = false;
    }
    /**
     * Get an integer parameter
     *
     * @param request           HttpServetRequest for  forms
     * @param name              parameter name
     * @param fieldKey          Key for the name of the field we're reading.
     * @param localUtil             LocalizedResource to use in localizing messages
     * @param returnMessage     localized continue message for continue button on error form
     * @param out Form PrintWriter
     */
    private int getIntParameter
        (
         HttpServletRequest request,
         String name,
         String fieldKey,
         LocalizedResource localUtil,
         String returnMessage,
         PrintWriter out
         )
    {
        String val = request.getParameter(name);
        int retval;
        if (val == null || val.equals(""))
            return NOT_GIVEN;
        try {
            retval = Integer.parseInt(val);
        } catch (Exception e) {
            printErrorForm(localUtil,localUtil.getTextMessage("SRV_InvalidVal",
                val, localUtil.getTextMessage(fieldKey)), returnMessage, out);
            return INVALID;
        }
        if (retval < 0) {
        // negative integers not allowed for the parameters we're getting.
            printErrorForm(localUtil, localUtil.getTextMessage("SRV_InvalidVal",
                 val, localUtil.getTextMessage(fieldKey)), returnMessage, out);
            return INVALID;
        }
        return retval;
    }
    /**
     * Print Derby Network Server banner
     */
    private void printBanner(LocalizedResource localUtil, PrintWriter out)
    {
        out.println("<!DOCTYPE html>");
        out.println("<html>");        
        out.println("<head>");
        out.println("<title>"+localUtil.getTextMessage("SRV_Banner")+"</title>");
        out.println("</head>");
        out.println("<body>");     
        out.println("<a href=\"#navskip\">[ " +
        localUtil.getTextMessage("SRV_SkipToContent") + " ]</a>");
        out.println("  -  <a href=\"" + SERVLET_ADDRESS + "\">[ " +
        localUtil.getTextMessage("SRV_BackToMain") + " ]</a>");
        out.println("<hr>");
        out.println("<h1>"+localUtil.getTextMessage("SRV_Banner")+"</h1>");
        out.println("<hr>");
    }

    /**
     * Determine the locale file needed for this browsers preferences
     * Defaults to the settings for derby.locale and derby.codeset if set
     *      English otherwise if browsers preferences can't be found
     *
     * @param request           HttpServetRequest for forms
     * @param locale                Name of locale (return arg)
     * @return the appUI which fits the browsers preferences
     */
    private LocalizedResource getCurrentAppUI(HttpServletRequest request, String[] locale )
    {
        LocalizedResource localUtil;
        String acceptLanguage = request.getHeader("Accept-Language");
        localUtil = new LocalizedResource(SERVLET_PROP_MESSAGES);
        // if no language specified use one set by derby.locale, derby.codeset
        locale[ 0 ] = null;
        if (acceptLanguage == null)
        {
            return localUtil;
        }
        // Use a tokenizer ot separate acceptable languages
        StringTokenizer tokenizer = new StringTokenizer(acceptLanguage, ",");
        while (tokenizer.hasMoreTokens())
        {
            //Get the next acceptable language
            String lang = tokenizer.nextToken();
            lang = getLocStringFromLanguage(lang);
            int langindex = translationAvailable(lang);
            // have we found one
            if (langindex != -1)
            {
                localUtil.init(null, lang, SERVLET_PROP_MESSAGES);
                // locale will be passed to server, server routines will get set appropriately
                locale[ 0 ] = lang;
                return localUtil;
            }
        }
        // nothing worked use defaults
        return localUtil;
        
    }
    /**
     * Get locale string from language which may have qvalue set
     * 
     * @param lang  language string to parse
     *
     * @return stripped language string to use in matching
     */
    private String getLocStringFromLanguage(String lang)
    {
        int semi;
        // Cut off any q-value that might come after a semi-colon
        if ((semi = lang.indexOf(';')) != -1)
        {
            lang = lang.substring(0, semi);
        }
        // trim any whitespace and fix the code, as some browsers might send a bad format
        lang = fixLanguageCode(lang.trim());
        return lang;
    }
    /**
     * Check if the required translation is available
     *
     * @param lang  language we are looking for
     * 
     * @return index into language array if found, -1 otherwise;
     */
    private int translationAvailable(String lang)
    {
        // assert lang == fixLanguageCode(lang)
        // we don't need to use toUpperCase() anymore, as the lang is already fixed
        for (int i = 0; i < knownLang.length; i++)
            if (knownLang[i].equals(lang))
                return i;
        return -1;
    }
    
    /**
     * Fix the language code, as some browsers send then in a bad format (for instance, 
     * Firefox sends en-us instead of en_US).
     *
     * @param lang  language to be fixed
     * 
     * @return fixed version of the language, with _ separating parts and country in upper case
     */
    private String fixLanguageCode( String lang ) {
        int index = lang.indexOf('-');
        if ( index != -1 ) {        
            return fixLanguageCode( lang, index );
        }
        index = lang.indexOf('_');
        if ( index != -1 ) {        
            return fixLanguageCode( lang, index );
        }
        return lang;
    }

    private String fixLanguageCode(String lang, int index) {
        return lang.substring(0,index) + "_" + lang.substring(index+1).toUpperCase(Locale.ENGLISH);
    }

    /**
     * get an HTML labelled message from the resource bundle file, according to
     * the given key.
     */
    private String getHtmlLabelledMessageInstance(LocalizedResource localUtil,
                                                  String key, String id) {

        if (id == null)
            id = "";

        return ("<label for='" + id + "'>" + localUtil.getTextMessage(key) +
            "</label>");

    }

    /**
     * Print the received string as a header.
     * @param str The string to be printed as a header.
     * @param out Form PrintWriter
     */
    private void printAsContentHeader(String str, PrintWriter out) {
        out.println("<a name=\"navskip\"></a><h2>" + str + "</h2>");
    }

    /**
     * If the received string has one or more single quotes
     * in it, replace each one with the HTML escape-code
     * for a single quote (apostrophe) so that the string 
     * can be properly displayed on a submit button.
     * @param str The string in which we want to escape
     *  single quotes.
     */
    private String escapeSingleQuotes(String str) {

        if ((str == null) || (str.indexOf("'") < 0))
            return str;

        char [] cA = str.toCharArray();

        // Worst (and extremely unlikely) case is every 
        // character is a single quote, which means the
        // escaped string would need to be 5 times as long.
        char [] result = new char[5*cA.length];

        int j = 0;
        for (int i = 0; i < cA.length; i++) {

            if (cA[i] == '\'') {
                result[j++] = '&';
                result[j++] = '#';
                result[j++] = '3';
                result[j++] = '9';
                result[j++] = ';';
            }
            else
                result[j++] = cA[i];

        }

        return new String(result, 0, j);

    }

    /**
     * Escapes potentially dangerous characters in data written to the browser.
     * <p>
     * <em>NOTE</em>: This is a poor mans implementation - it doesn't protect
     * against all kinds of attacks, and it cannot be used in all contexts.
     *
     * @param str the string to escape
     * @return A sanitized string.
     */
    private String escapeHTML(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        // Replaced characters: ; \ / < > " ' = &
        char[] cA = str.toCharArray();
        char[] result = new char[5 * cA.length];
        int j = 0;
        for (int i=0; i < cA.length; i++) {
            boolean escaped = true; // controls addition of ;
            char c = cA[i];
            switch (c) {
                case ';':
                    result[j++] = '&';
                    result[j++] = '#';
                    result[j++] = '5';
                    result[j++] = '9';
                    break;
                case '\\':
                    result[j++] = '&';
                    result[j++] = '#';
                    result[j++] = '9';
                    result[j++] = '2';
                    break;
                case '/':
                    result[j++] = '&';
                    result[j++] = '#';
                    result[j++] = '4';
                    result[j++] = '7';
                    break;
                case '<':
                    result[j++] = '&';
                    result[j++] = 'l';
                    result[j++] = 't';
                    break;
                case '>':
                    result[j++] = '&';
                    result[j++] = 'g';
                    result[j++] = 't';
                    break;
                case '"':
                    result[j++] = '&';
                    result[j++] = '#';
                    result[j++] = '3';
                    result[j++] = '4';
                    break;
                case '\'':
                    result[j++] = '&';
                    result[j++] = '#';
                    result[j++] = '3';
                    result[j++] = '9';
                    break;
                case '=':
                    result[j++] = '&';
                    result[j++] = '#';
                    result[j++] = '6';
                    result[j++] = '1';
                    break;
                case '&':
                    result[j++] = '&';
                    result[j++] = 'a';
                    result[j++] = 'm';
                    result[j++] = 'p';
                    break;
                default:
                    result[j++] = c;
                    escaped = false;
            }
            if (escaped) {
                result[j++] = ';';
            }
        }
        return String.copyValueOf(result, 0, j);
    }

    private static boolean isServerStarted(NetworkServerControl server, int ntries)
    {
        for (int i = 1; i <= ntries; i ++)
        {
            try {
                Thread.sleep(500);
                server.ping();
                return true;
            }
            catch (Exception e) {
                if (i == ntries)
                    return false;
            }
        }
        return false;
    }
    
}
