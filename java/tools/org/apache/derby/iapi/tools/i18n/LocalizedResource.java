/*

   Derby - Class org.apache.derby.iapi.tools.i18n.LocalizedResource

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
package org.apache.derby.iapi.tools.i18n;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.StringTokenizer;


public final class LocalizedResource  implements java.security.PrivilegedAction<String> {

	private ResourceBundle res;
	private Locale locale;
	private String encode;
	private final static String MESSAGE_FILE = "org.apache.derby.loc.toolsmessages";
    public final static String ENV_CODESET = "derby.ui.codeset";
    public final static String ENV_LOCALE = "derby.ui.locale";
	private String messageFileName;
	private String resourceKey;
	private LocalizedOutput out;
	private LocalizedInput in;
	private boolean enableLocalized;
	private static LocalizedResource local;
	private int dateSize;
	private int timeSize;
	private int timestampSize;
	private DateFormat formatDate;
	private DateFormat formatTime;
	private DateFormat formatTimestamp;
	private NumberFormat formatNumber;
    private DecimalFormat formatDecimal;

    private LocalizedResource(){

		init();
	}
    public LocalizedResource(String msgF){

        init(null, null, msgF, true);
	}

    /**
     * This overload version does <b>not</b> check the environment for values of
     * encoding via {@link #ENV_CODESET} and locale via {@link #ENV_LOCALE}.
     * The others do if value of either is null. DERBY-6680.
     * @param encStr encoding
     * @param locStr locale
     * @param msgF message file
     */
    public LocalizedResource(String encStr, String locStr, String msgF){

        init(encStr,locStr,msgF, false);
    }

    public static LocalizedResource getInstance(){

		if (local == null){

			local = new  LocalizedResource();
		}
		return local;
	}
    // Resets the 'local' field to null. This is not needed for normal
    // operations, however, when executing sql files in our junit tests, we use
    // the same jvm and thus the locale will get loaded only once, resulting
    // in trouble when testing the localization for ij.
    public static void resetLocalizedResourceCache()
    {
        local=null;
    }
	public void init(){
		init(null,null,null);
	}


    public void init (String encStr, String locStr, String msgF) {
        init(encStr, locStr, msgF, true);
    }

    private void init (String encStr, String locStr, String msgF, boolean readEnv){

		if (encStr != null){

			encode = encStr;
		}
		//then get encoding string from environment
        if (encode == null && readEnv) {

			String eEncode = getEnvProperty(ENV_CODESET);
			if ( eEncode != null ){

				encode = eEncode;
			}
		}
		
		// If null at this point then the default encoding
		// will be always used.

		//get locale string from the caller first
		locale = getNewLocale(locStr);

		//if null, get locale again from the environment variable
        if (locale==null && readEnv) {

			String s = getEnvProperty(ENV_LOCALE);
			locale = getNewLocale(s);
		}
		//get the default locale if forced
		if (locale==null){

			locale = Locale.getDefault();
		}
		if (msgF != null) {

			messageFileName = msgF;
		}
		else {

			messageFileName = MESSAGE_FILE;
		}

		//create default in/out
		out = getNewOutput(System.out);
		in = getNewInput(System.in);

		//for faster code: get the format objs
		if (enableLocalized && locale != null){

			formatDecimal = (DecimalFormat)DecimalFormat.getInstance(locale);
			formatNumber = NumberFormat.getInstance(locale);
			formatDate = DateFormat.getDateInstance(DateFormat.LONG,locale);
			formatTime = DateFormat.getTimeInstance(DateFormat.LONG,locale);
			formatTimestamp = DateFormat.getDateTimeInstance(DateFormat.LONG,
													DateFormat.LONG, locale);
		}
		else {

			formatDecimal = (DecimalFormat)DecimalFormat.getInstance();
			formatNumber = NumberFormat.getInstance();
			formatDate = DateFormat.getDateInstance(DateFormat.LONG);
			formatTime = DateFormat.getTimeInstance(DateFormat.LONG);
			formatTimestamp = DateFormat.getDateTimeInstance(DateFormat.LONG,
													DateFormat.LONG);
		}
		//initialize display sizes for columns
		initMaxSizes2();
	}
	//get the message file resource according to the locale
	//fall back to English message file if locale message file is not found
	private void setResource(){
		if (res != null){

			return;
		}
		if ( locale == null || locale.toString().equals("none") ){

			res = ResourceBundle.getBundle(messageFileName);
		}
		else
		try {

			res = ResourceBundle.getBundle(messageFileName,locale);
		}
		catch(java.util.MissingResourceException e){

			res = ResourceBundle.getBundle(messageFileName,Locale.ENGLISH);
		}
	}
	private void initMaxSizes2(){
		dateSize = 0;
		timeSize = 0;
		timestampSize = 0;

		int len;

		// check the date & timestamp max length
		// 3900/01/28 !! original devloper thought they were getting 2000/01/28
		Date d = new Date(60907276800000L);
		Timestamp t = new Timestamp(d.getTime());

		for(int month  = 0 ;  month <=11 ; month++, d.setTime(d.getTime() + (30L * 24L * 60L * 60L * 1000L))) {
			len=getDateAsString(d).length();

			if(len > dateSize ) {
				dateSize=len;
			}

			t.setTime(d.getTime() + ((((21L * 60L) + 59L) * 60L) + 59L));
			len=getTimestampAsString(t).length();

			if(len > timestampSize) {
				timestampSize=len;
			}
		}

		// set the time max length
		// minimum of 18 because the old buggy code always used 18
		len = 18;
		for (int hour = 0 ; hour < 24; hour++) {

			long secs = (hour * 3600L) + (59 * 60L) + 59L;

			long ms = secs * 1000L;

			Date td = new Date(ms);

			String fd = formatTime.format(td);

			if (fd.length() > len)
				len = fd.length();
		}
		timeSize=len;

	}

	public LocalizedInput getNewInput(InputStream i) {
		try {

			if (encode != null)
			    return new LocalizedInput(i,encode);
		}
		catch (UnsupportedEncodingException e){
			
		}
		return new LocalizedInput(i);
	}

	public LocalizedInput getNewEncodedInput(InputStream i, String encoding) {

		try {
	          return new LocalizedInput(i,encoding);
		}
		catch (UnsupportedEncodingException e){
			
		}
		return new LocalizedInput(i);
        }

	public LocalizedOutput getNewOutput(OutputStream o){
		try {

			if (encode != null)
			    return new LocalizedOutput(o,encode);
		}
		catch(UnsupportedEncodingException e){
		}
		return new LocalizedOutput(o);
	}
	/**
	 * Get a new LocalizedOutput with the given encoding.
	 * @throws UnsupportedEncodingException
	 */
	public LocalizedOutput getNewEncodedOutput(OutputStream o,
			String encoding) throws UnsupportedEncodingException{
	    out = new LocalizedOutput(o, encoding);

	    return out;
	}
	private Locale getNewLocale(String locStr){
			String l="", r="", v="";
			StringTokenizer st;

			if (locStr==null) {
				return null;
			}
			st=new StringTokenizer(locStr, "_");
			try {
				l=st.nextToken();
				if(st.hasMoreTokens()==true)
					r=st.nextToken();
				if(st.hasMoreTokens()==true)
					v=st.nextToken();
				return new Locale(l,r,v);
			} catch (Exception e) {
				return null;
			}
	}
    public String getTextMessage(String key, Object... objectArr) {

		if (res == null){

			setResource();
		}

		if(key=="IJ_UnabToEsta") {
			try{

			return "No connection available";
			}
		catch(Exception e){
		String tmpFormat = key;

		for (int i=0; objectArr != null && i<objectArr.length; i++)
			tmpFormat = tmpFormat + ", <{" + (i) + "}>";
			return MessageFormat.format(tmpFormat, objectArr);
		}

		}

		else{
		
			try{

				return MessageFormat.format(res.getString(key), objectArr);
			} catch (Exception e) {
					String tmpFormat = key;

					for (int i=0; objectArr != null && i<objectArr.length; i++)
						tmpFormat = tmpFormat + ", <{" + (i) + "}>";
					return MessageFormat.format(tmpFormat, objectArr);
			}
		}

	}
	public String getLocalizedString(ResultSet rs,
										ResultSetMetaData rsm,
										int columnNumber) throws SQLException{
			if (!enableLocalized){

				return rs.getString(columnNumber);
			}
			int type = rsm.getColumnType(columnNumber);
			if ( type == Types.DATE ) {

				return getDateAsString(rs.getDate(columnNumber));
			}
			else if ( type == Types.INTEGER ||	type == Types.SMALLINT ||
					type == Types.BIGINT ||	type == Types.TINYINT ) {

				return getNumberAsString(rs.getLong(columnNumber));
			}
			else if (type == Types.REAL || 	type == Types.FLOAT ||
					type == Types.DOUBLE ) {

				return getNumberAsString(rs.getDouble(columnNumber));
			}
            else if (type == Types.NUMERIC || type == Types.DECIMAL) {

                return getNumberAsString(rs.getBigDecimal(columnNumber));
			}
			else if (type == Types.TIME ) {

				return getTimeAsString(rs.getTime(columnNumber));
			}
			else if (type == Types.TIMESTAMP ) {
		
	return getTimestampAsString(rs.getTimestamp(columnNumber));
			}
			return rs.getString(columnNumber);
		}

	public String getDateAsString(Date d){

		if (!enableLocalized){

			return d.toString();
		}

		return formatDate.format(d);
	}
	public String getTimeAsString(Date t){

		if (!enableLocalized){

			return t.toString();
		}
		return formatTime.format(t,	new StringBuffer(),
									  new java.text.FieldPosition(0)).toString();
	}
	public String getNumberAsString(int o){

		if (enableLocalized){

			return formatNumber.format(o);
		}
		else {

			return String.valueOf(o);
		}
	}
	public String getNumberAsString(long o){
		if (enableLocalized){

			return formatNumber.format(o);
		}
		else{

			return String.valueOf(o);
		}
	}
	public String getNumberAsString(Object o){
		if (enableLocalized){

			return formatNumber.format(o, new StringBuffer(),
										new FieldPosition(0)).toString();
		}
		else {

			return o.toString();
		}
	}
	public String getNumberAsString(double o){
		if (!enableLocalized) {

			return String.valueOf(o);
		}
		return formatDecimal.format(o);
	}
	public String getTimestampAsString(Timestamp t){

		if (!enableLocalized){

			return t.toString();
		}
		return formatTimestamp.format
			(t, new StringBuffer(),
			 new java.text.FieldPosition(0)).toString();
	}
	public int getColumnDisplaySize(ResultSetMetaData rsm,
										int columnNumber) throws SQLException{
		  if (!enableLocalized){

				return rsm.getColumnDisplaySize(columnNumber);
		  }
		  int type = rsm.getColumnType(columnNumber);
		  if (type == Types.DATE)
					return dateSize;
		  if (type == Types.TIME)
					return timeSize;
		  if (type == Types.TIMESTAMP)
					return timestampSize;
		  return rsm.getColumnDisplaySize(columnNumber);
	}
	public String getStringFromDate(String dateStr)
		throws ParseException{
			if (!enableLocalized){

				return dateStr;
			}
			Date d = formatDate.parse(dateStr);
			return new java.sql.Date(d.getTime()).toString();
	}
	public String getStringFromTime(String timeStr)
		throws ParseException{

			if (!enableLocalized){
				return timeStr;
			}
			Date t = formatTime.parse(timeStr);
			return new java.sql.Time(t.getTime()).toString();
	}
	public String getStringFromValue(String val)
		throws ParseException{

			if (!enableLocalized){
				return val;
			}
			return formatNumber.parse(val).toString();
	}
	public String getStringFromTimestamp(String timestampStr)
		throws ParseException{
			if (!enableLocalized){

				return timestampStr;
			}
			Date ts = formatTimestamp.parse(timestampStr);
			return new java.sql.Timestamp(ts.getTime()).toString();
	}
	public Locale getLocale(){
;
			return locale;
	}

	private final synchronized String getEnvProperty(String key) {
		String s;

		 try
		  {
				resourceKey =  key;
				s = java.security.AccessController.doPrivileged(this);
		}
		catch (SecurityException se) {

			s = null;
		}
		//System.out.println("{"+resourceKey+"="+s+"}");
		return s;
	}
	public final String run() {

		String s = System.getProperty(resourceKey);
		return s;
	}
	public static boolean enableLocalization(boolean mode) {

		getInstance().enableLocalized = mode;
		//re-initialized locale
		getInstance().init();
		return mode;
	}
	public boolean isLocalized(){

		return getInstance().enableLocalized;
	}
    public static String getMessage(String key, Object... args) {

        return getInstance().getTextMessage(key, args);
    }
	public static LocalizedOutput OutputWriter(){

		return getInstance().out;
	}
	public static LocalizedInput InputReader(){

		return getInstance().in;
	}
	public static String getNumber(long o){

		return getInstance().getNumberAsString(o);
	}
	public static String getNumber(int o){

		return getInstance().getNumberAsString(o);
	}
	public String toString(){

		String s = "toString(){\n" +
			"locale=" + (locale==null?"null":locale.toString()) + "\n" +
			"encode=" + encode + "\n" +
			"messageFile=" + messageFileName + "\n" +
			"resourceKey=" + resourceKey + "\n" +
			"enableLocalized=" + enableLocalized + " \n" +
			"dateSize=" + dateSize + "\n" +
			"timeSize=" + timeSize + "\n" +
			"timestampSize="+timestampSize+ "\n}";
			return s;
	}
}
