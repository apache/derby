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

var derbycal = {};

// INITIALIZATION VARIABLES.  Set these as you see fit

// The offset from GMT for this calendar.  Change this if you want to change
// time zones for the calendar.  Right now set to CST (Austin).
derbycal.gmtOffset = "-08:00";

derbycal.onlineColor = "#66FF66";
derbycal.offlineColor = "#ccFFFF";

// This needs to be in a valid date format for initialization a Javascript date
// It should generally be on a Sunday, but it can be any day and we'll start
// the calendar from the next Sunday after your given date.
derbycal.startDate = "October 12, 2006";

// === END INIT VARIABLES ==

/** 
* The array of days.  The days are hardcoded, the dates will be calculated
* as part of the derbycal.init() function
*/
derbycal.days = [ 
    ["Sunday",      ""],
    ["Monday",      ""],
    ["Tuesday",     ""],
    ["Wednesday",   ""],
    ["Thursday",    ""],
    ["Friday",      ""],
    ["Saturday",    ""]
];

// Sorry, English-chauvinist code...
derbycal.months = [ 
    "January", "Febuary", "March", "April", "May", "June",
    "July", "August", "September", "October", "November",
    "December"
];

derbycal.offline   = false;


/**
* Do any initialization work.  This should be called when the
* page is loaded
*/
derbycal.init = function() {
  var today = new Date();
  var dateForm = document.dateForm;
  dateForm.startDate.value = derbycal.months[today.getMonth()] + " " +
     today.getDate() + ", " + today.getFullYear();
  derbycal.updateDays();
}

derbycal.updateDays = function() { 
  derbycal.calcDays(document.dateForm.startDate.value);
  derbycal.startDate = derbycal.days[0][1];
  derbycal.endDate   = derbycal.days[6][1];  
}

derbycal.changeStartDate = function() {
  derbycal.updateDays();
  derbycal.refreshCalendar();
}

/**
 * Given a starting day, set the dates for the derbycal.days
 * array, starting with the next Sunday from the given start day.  
 * This array is then used to set up the calendar.
 */
derbycal.calcDays = function(startDay) {
    var date = new Date(startDay);

    // adjust to the next Sunday, unless today is Sunday
    var day = date.getDay();
    var daysToSunday = 0;
    if ( day != 0 ) {
        daysToSunday = 7 - day;
    }
        
    date.setDate(date.getDate() + daysToSunday);

    // Update the date on the page to reflect Sunday's date
    var dateForm = document.dateForm;
    dateForm.startDate.value = derbycal.months[date.getMonth()] + 
        " " + date.getDate() + ", " + date.getFullYear(); 

    // Set the array of days to have the right dates based on
    // the given start date
    for ( day = 0 ; day < 7; day++ ) {
        derbycal.days[day][1] = date.getFullYear() + "-" +
                       (date.getMonth() + 1) + "-" +
                       date.getDate();
        date.setDate(date.getDate() + 1);
    }
} 

/** 
 * Log in to Google Calendar
 */
derbycal.login = function() {
    var statusText = document.getElementById("loggingIn");
    statusText.style.visibility = "visible";

    var loginForm = document.loginForm;
    derbycal.user = loginForm.user.value + "@gmail.com";
    derbycal.password = loginForm.password.value;

    try {
        // Get the events list from  Google Calendar
        calapplet.login(derbycal.user, derbycal.gmtOffset, 
            derbycal.user, derbycal.password,
            derbycal.startDate, derbycal.endDate, false);


    } catch ( e ) {
        statusText.style.visibility = "hidden";
        alert("Error logging in to calendar : " + 
            derbycal.getExceptionMessage(e));
        return;
    }

    var loginDiv = document.getElementById("login-div");
    loginDiv.style.display = "none";

    var mainDiv = document.getElementById("main-div");
    mainDiv.style.visibility = "visible";
    
    derbycal.refreshCalendar();

    derbycal.setPageOffline(false);
}

// Start working offline without logging in
derbycal.startOffline = function () {
    derbycal.goOffline();
    var mainDiv = document.getElementById("main-div");
    mainDiv.style.visibility = "visible";
    derbycal.refreshCalendar();

    document.getElementById("workOfflineButton").style.visibility = "hidden";
    document.getElementById("onlineButton").disabled = true;
}

derbycal.goOffline = function() {
    calapplet.goOffline();
    derbycal.setPageOffline(true);
}

derbycal.setPageOffline = function(offline) {
    derbycal.offline = offline;
    document.offlineForm.offlineButton.disabled = offline;
    document.offlineForm.onlineButton.disabled  = !offline;

    var bodyElement = document.getElementById("body");
    if ( offline ) {
        bodyElement.style.background = derbycal.offlineColor;    
    } else {
        bodyElement.style.background = derbycal.onlineColor;    
    }
}

derbycal.goOnline = function() {
    var status = document.getElementById("goingOnline");
    // This isn't working right... status.style.visibility = "visible";
    try {
        calapplet.goOnline();
    } catch ( e ) {
        alert("Error going online: " + derbycal.getExceptionMessage(e));
    }
    status.style.visibility = "hidden";    
    derbycal.setPageOffline(false);

    var conflicts = calapplet.getConflicts();
    if ( conflicts != null ) {
        alert(conflicts);
    }

    derbycal.checkOffline();
    derbycal.refreshCalendar();
}

// Check to see if we're offline, and if so fix the page to indicate this
// Should do this after each invocation to derbycal
derbycal.checkOffline = function() {
    if ( derbycal.offline ) {
        return;
    }

    if ( ! calapplet.isOnline() ) {
        derbycal.setPageOffline(true);
        alert("Unable to communicate with Google Calendar; " +
            "going offline.");
    }
}


// Get the events from the Google Calendar
derbycal.refreshCalendar = function() {

    // Get the list of events from Google Calendar
    var eventStr = calapplet.refresh();

    derbycal.loadEvents(eventStr);
    derbycal.checkOffline();
}

//
// Load events onto the page from a JSON array containing
// the events
//
derbycal.loadEvents = function(eventStr) {
    var events = eval('(' + eventStr + ')');

    // First, clear the calendar on the web page
    derbycal.clearCalendarOnPage();

    var i = 0;
    while ( events != null  &&  i < events.length ) {
        var event = events[i++];
        derbycal.addEventToPage(event.eventid, event.day, event.date, event.title);
    }
}
derbycal.clearCalendarOnPage = function() {
    var i = 0;
    var days = "";
    while ( i < derbycal.days.length ) {
        var day = derbycal.days[i++][0];
  
        var dayrow = document.getElementById(day);
  
        var j = dayrow.childNodes.length - 1;
        while ( j > 1 ) {
            dayrow.removeChild(dayrow.childNodes[j--]);
        }
    }
}
  

derbycal.postNewEvent = function() {
    // Get the form data
    var day     = document.newEventForm.eventDay.value;
    var title   = document.newEventForm.eventTitle.value;

    // Generate a unique id for it
    var id = derbycal.getUniqueId();

    var date = derbycal.getDateForDay(day);

    // Tell our applet to save this
    try {

        var eventJSON = calapplet.addEvent(id, date, title);
        var event = eval( "(" + eventJSON + ")" );
        id = event.eventid;

        // Update the HTML
        derbycal.addEventToPage(id, day, date, title);
    } catch ( e ) {
        alert("Unable to add event: " + derbycal.getExceptionMessage(e));
    }
    derbycal.checkOffline();
}

derbycal.getDateForDay = function(day) {
    var i = 0;
    while ( i < derbycal.days.length ) {
        if ( derbycal.days[i][0] == day )
            return derbycal.days[i][1];
        else
            i++;
    }

    throw "The day specified, " + day + " is not one I recognize";
}

derbycal.addEventToPage = function(id, day, date, title) {
    // Ignore the event if the date is out of range
    if ( ! derbycal.dateInRange(date) ) {
        return;
    }
    
    // Get the table for the specified day
    var table = document.getElementById(day);

    if ( table == null ) {
        alert("Can't find the HTML element for day " + day);
        return;
    }

    var tr = document.createElement("tr");
    // tr.class        = "event";
    tr.className    = "event";  // for IE
    tr.id           = id;

    tr.innerHTML =  
        '<td><input type="image" src="images/delete.gif" ' +
        'onclick="derbycal.deleteEvent(this); return false;"</td>' +
        '<td>&nbsp;&nbsp;&nbsp;</td>' +
        '<td><input type="text" size="70" ' +
        'class="input-box"' + 
        'onchange="derbycal.updateEvent(this); return false;" ' + 
        'value="' + title + '"></input></td>';

    table.appendChild(tr);
}

derbycal.dateInRange = function(date) {
    for ( i = 0 ; i < 6 ; i++ ) {
        if ( derbycal.days[i][1] == date ) {
            return true;
        }
    }
  
    return false;
}

/**
 * Update the event
 * 
 *  element - the DOM element for the input box for this event.
 *      This is used to get to the enclosing row, which has the id for this
 *      event.
 */
derbycal.updateEvent = function(element) {
    var id = element.parentNode.id;

    // tell our applet to update this
    try {
        calapplet.updateEvent(id, element.value);
    } catch (e) {
        alert("Unable to update event: " + derbycal.getExceptionMessage(e));

        // Simple Big Hammer approach to fixing things.  Can get more
        // refined if you want to by remembering the old value and resetting
        // it...
        derbycal.refreshCalendar();
    }
    derbycal.checkOffline();
}

/**
 * Delete the event
 *
 *  element - the DOM element for the Delete button.  This is used
 *      to get the parent and delete the whole row
 */
derbycal.deleteEvent = function(element) {
    var row = element.parentNode;
    var id = row.id;

    // now tell our applet to delete this
    try {
        calapplet.deleteEvent(id);
    } catch (e) {
        alert("Unable to delete event from Google Calendar: " + 
            derbycal.getExceptionMessage(e));
        return;
    }
            
    row.parentNode.removeChild(row);
    derbycal.checkOffline();
}

/**
 * Get a temporary unique id.  This isn't perfect, but it will do for our purposes.
 * If you have a better algorithm, please feel free to use it!
 *
 * This one was scraped from the web at
 *
 * http://www.sitepoint.com/forums/showthread.php?t=318819
 */
derbycal.getUniqueId = function() {
	return Math.random() * Math.pow(10, 17) + Math.random() * 
            Math.pow(10, 17) + Math.random() * Math.pow(10, 17) + 
            Math.random() * Math.pow(10, 17);
}

derbycal.getExceptionMessage = function(e) {
    // Java exceptions (at least in Firefox) are stacked underneath
    // a PrivilegedActionException and then an InvocationTargetException,
    // so we have to dig deep to get the real message.  
    //
    // I suspect other browsers do this differently so your mileage may
    // vary, and I can see this turning into a very "rich" function
    //
    if ( e.getException ) {
        var exc = e.getException().getCause().getCause();
        return exc.getClass().getName() + ": " + exc.getMessage() + ".  Please " +
            "see the Java Console for more information";
    } else if ( e.getMessage ) {
        return e.getMessage + ". Please see the Java Console for more information.";
    } else {
        return e;
    }
}
