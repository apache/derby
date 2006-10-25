--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
connect 'jdbc:derby:detest;create=true;' as MÅnchen;

drop table deta;
create table detab ( dc1 decimal(5,3), dc3 date, dc2 char(200) );
insert into detab values(1.123,date('2000-01-25'),'Zuviele Programme gefunden. Versuchen Sie es erneut.');
insert into detab values(2.123,date('2000-02-24'),'Nur Teile der Entwicklungsdatenbank gefunden.');
insert into detab values(3.123,date('2000-03-23'),'Mehr als eine Instanz der Tabelle %s in Entwicklungsdatenbank gefunden.');
insert into detab values(4.123,date('2000-04-22'),'Mehr als einen Runner-Datensatz f?r dieses Programm gefunden.');
insert into detab values(5.123,date('2000-05-21'),'Es kann nur eine BEFORE- oder AFTER INPUT/CONSTRUCT-Klausel in einer ');
insert into detab values(6.123,date('2000-06-20'),'INPUT-/CONSTRUCT-Anweisung erscheinen.');
insert into detab values(7.123,date('2000-07-19'),'Die Funktion %s kann nur in einer INPUT- oder CONSTRUCT-Anweisung ');
insert into detab values(8.123,date('2000-08-18'),'Fglpc, der Pcode-Kompiler, ist nicht in Ihrem Pfad vorhanden.');
insert into detab values(9.123,date('2000-09-17'),'Kann Runner %s nicht finden.');
insert into detab values(10.123,date('2000-10-16'),'I/O-Fehler beim Laufen von fglc: %s.');
insert into detab values(11.123,date('2000-11-15'),'I/O-Fehler beim Ausf?hren von fglc.');
insert into detab values(12.123,date('2000-12-14'),'Kann Datei %s nicht îffnen, um den Wert einer TEXT-Variable zu lesen.');
insert into detab values(13.123,date('2000-01-13'),'Der angegebene WORDWRAP RIGHT MARGIN-Wert liegt au·erhalb des Bereichs.'); 
insert into detab values(14.123,date('2000-02-12'),'als oder gleich dem Wert des rechten Reportrands sein.');
insert into detab values(15.123,date('2000-03-11'),'4GL unterst?tzt nicht die Ausgabe einer Blob-Variable.');
insert into detab values(16.123,date('2000-04-10'),'Die HELP- und ATTRIBUTE-Klauseln kînnen jeweils nur einmal definiert werden.');
insert into detab values(17.123,date('2000-05-09'),'Ein Feld des INTERVAL-Kennzeichners liegt au·erhalb des Bereichs.'); 
insert into detab values(18.123,date('2000-06-08'),'Der Bereich geht von YEAR TO MONTH und von DAY TO FRACTION.');
insert into detab values(19.123,date('2000-07-07'),'Das Anlegen von Indizes ist hier nicht erlaubt.');
insert into detab values(20.123,date('2000-08-06'),'Hier wird die Eingabe des Spaltennamen erwartet.');
insert into detab values(21.123,date('2000-09-05'),'Hier wird die Eingabe des Tabellennamen erwartet.');
insert into detab values(22.123,date('2000-10-04'),'Der eigentliche Spaltenname kann hier nicht angegeben werden.'); 
insert into detab values(23.123,date('2000-11-03'),'Die maximale Grî·e f?r Varchar mu· zwischen 1 und 255 liegen.');
insert into detab values(24.123,date('2000-12-02'),'Kann keine temporÑre Datei %s anlegen, um eine Blob-Variable aufzunehmen.');
insert into detab values(25.123,date('2000-11-01'),'Symbol %s mu· ein SQL-Datenbank-Elementname sein - entweder ein ');
insert into detab values(26.123,date('2000-10-28'),'Datenbankname, ein Tabellenname oder ein Spaltenname.');
insert into detab values(27.123,date('2000-09-29'),'DATETIME-Einheiten kînnen nur YEAR, MONTH, DAY, HOUR, MINUTE,'); 
insert into detab values(28.123,date('2000-08-30'),'Eingeklammerte Genauigkeit von FRACTION mu· zwischen 1 und 5 liegen.');
insert into detab values(29.123,date('2000-07-31'),'F?r andere Zeiteinheiten kann keine Genauigkeit definiert werden.');
insert into detab values(30.123,date('2000-06-25'),'Das Startfeld von DATETIME oder INTERVAL-Kennzeichnern mu· in der ');
insert into detab values(31.123,date('2000-05-25'),'Zeitliste vor dem Endfeld stehen.');
insert into detab values(32.123,date('2000-04-25'),'Entladen in Datei %s mi·lungen.');
insert into detab values(33.123,date('2000-03-25'),'Laden aus Datei %s mi·lungen.');
insert into detab values(34.123,date('2000-02-25'),'Das Programm kann eine DISPLAY ARRAY-Anweisung an dieser Stelle nicht'); 
insert into detab values(35.123,date('2000-01-25'),'beenden, weil es sich nicht in einer DISPLAY ARRAY-Anweisung befindet.');
insert into detab values(36.123,date('2000-02-25'),'Das Programm kann eine INPUT-Anweisung an dieser Stelle nicht beenden,'); 
insert into detab values(37.123,date('2000-04-25'),'weil es sich nicht in einer INPUT-Anweisung befindet.');
insert into detab values(38.123,date('2000-05-25'),'Konnte Datei %s nicht îffnen.');
insert into detab values(39.123,date('2000-07-25'),'Name des Eigent?mers %s hat die LÑnge von 8 Zeichen ?berschritten.');
insert into detab values(40.123,date('2000-09-25'),'Fehler bei der Speicherzuordnung.');
insert into detab values(41.123,date('2000-05-25'),'Unter dem angegebenen Namen wurde kein lauffÑhiges 4GL-Programm gefunden.');
LOCALIZEDDISPLAY ON;
show connections;
select * from detab;
