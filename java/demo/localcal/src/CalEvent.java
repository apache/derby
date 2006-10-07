/*
 
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

import java.util.Date;
import java.text.SimpleDateFormat;
import org.json.*;

/**
 * Encapsulates an event
 */
public class CalEvent implements java.io.Serializable {
    private String id;
    private String date;
    private String title;
    private String editURL;
    private String versionId;
    
    private SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE");
    
    private SimpleDateFormat fullFormat = new SimpleDateFormat("yyyy-MM-dd");
    
    /** Creates a new instance of DerbyCalEvent 
     *  
     *  @param id
     *      The id for this event.  This may be a temporary id until we're
     *      assigned an "official" one by Google Calendar (whenever this event
     *      is posted to Google Calendar).
     * 
     *  @param date
     *      The day for this event, in the format <yyyy>-<mm>-<dd>
     * 
     *  @param title
     *      The title for the event
     *
     *  @param editURL
     *      The edit URL provided by Google Calendar (can be null)
     *
     *  @param versionId
     *      The version identifier provided by Google Calendar (can be null)
     */
    public CalEvent(String id, String date, String title,
            String editURL, String versionId) {
        this.id         = id;
        this.date       = date;
        this.title      = title;
        this.editURL    = editURL;
        this.versionId  = versionId;
    }

    public void setId(String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }

    public String getDate() {
        return date;
    }
    
    /** Get the day string for the date */
    public String getDay() throws Exception {
        return dayFormat.format(fullFormat.parse(getDate()));
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setDate(String date) {
        this.date = date;
    }
    
    /** 
     * Get the JSONObject for this event
     */
    public JSONObject getJSONObject() throws Exception {
        JSONObject jobj = new JSONObject();
        jobj.put("eventid", getId());
        jobj.put("day", getDay());
        jobj.put("title", getTitle());
        jobj.put("date", getDate());
        jobj.put("editURL", getEditURL());
        jobj.put("versionId", getVersionId());
        
        return jobj;
    }

    public String getEditURL() {
        return editURL;
    }

    public String getVersionId() {
        return versionId;
    }
        
    public String toString() {
        return "id: " + getId() +", date: " + getDate() + ", title: " +
                getTitle();
    }

        
}
