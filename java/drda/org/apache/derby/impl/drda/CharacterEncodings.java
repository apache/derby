/*

   Derby - Class org.apache.derby.impl.drda.CharacterEncodings

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.drda;

public final class CharacterEncodings
{

  // This is a static class, so hide the default constructor.
  private CharacterEncodings () {}

  //private static java.util.Hashtable javaEncodingToCCSIDTable__ = new java.util.Hashtable();
  private static java.util.Hashtable ccsidToJavaEncodingTable__ = new java.util.Hashtable();

  static {
    populate_ccsidToJavaEncodingTable();
    //populate_javaEncodingToCCSIDTable();
  }

  /*
  static void populate_javaEncodingToCCSIDTable ()
  {
    javaEncodingToCCSIDTable__.put ("Cp037", new Integer (37));
    javaEncodingToCCSIDTable__.put ("Cp273", new Integer (273));
    javaEncodingToCCSIDTable__.put ("Cp277", new Integer (277));
    javaEncodingToCCSIDTable__.put ("Cp278", new Integer (278));
    javaEncodingToCCSIDTable__.put ("Cp280", new Integer (280));
    javaEncodingToCCSIDTable__.put ("Cp284", new Integer (284));
    javaEncodingToCCSIDTable__.put ("Cp285", new Integer (285));
    javaEncodingToCCSIDTable__.put ("Cp297", new Integer (297));
    javaEncodingToCCSIDTable__.put ("Cp420", new Integer (420));
    javaEncodingToCCSIDTable__.put ("Cp424", new Integer (424));
    javaEncodingToCCSIDTable__.put ("Cp747", new Integer (437));
    javaEncodingToCCSIDTable__.put ("Cp500", new Integer (500));
    javaEncodingToCCSIDTable__.put ("Cp737", new Integer (737));
    javaEncodingToCCSIDTable__.put ("Cp775", new Integer (775));
    javaEncodingToCCSIDTable__.put ("ISO8859_7", new Integer (813));
    javaEncodingToCCSIDTable__.put ("ISO8859_1", new Integer (819));
    javaEncodingToCCSIDTable__.put ("Cp838", new Integer (838));
    javaEncodingToCCSIDTable__.put ("Cp850", new Integer (850));
    javaEncodingToCCSIDTable__.put ("Cp852", new Integer (852));
    javaEncodingToCCSIDTable__.put ("Cp855", new Integer (855));
    javaEncodingToCCSIDTable__.put ("Cp856", new Integer (856));
    javaEncodingToCCSIDTable__.put ("Cp857", new Integer (857));
    javaEncodingToCCSIDTable__.put ("Cp858", new Integer (858));
    javaEncodingToCCSIDTable__.put ("Cp860", new Integer (860));
    javaEncodingToCCSIDTable__.put ("Cp861", new Integer (861));
    javaEncodingToCCSIDTable__.put ("Cp862", new Integer (862));
    javaEncodingToCCSIDTable__.put ("Cp863", new Integer (863));
    javaEncodingToCCSIDTable__.put ("Cp864", new Integer (864));
    javaEncodingToCCSIDTable__.put ("Cp865", new Integer (865));
    javaEncodingToCCSIDTable__.put ("Cp866", new Integer (866));
    javaEncodingToCCSIDTable__.put ("Cp868", new Integer (868));
    javaEncodingToCCSIDTable__.put ("Cp869", new Integer (869));
    javaEncodingToCCSIDTable__.put ("Cp870", new Integer (870));
    javaEncodingToCCSIDTable__.put ("Cp871", new Integer (871));
    javaEncodingToCCSIDTable__.put ("Cp874", new Integer (874));
    javaEncodingToCCSIDTable__.put ("Cp875", new Integer (875));
    javaEncodingToCCSIDTable__.put ("KOI8_R", new Integer (878));
    javaEncodingToCCSIDTable__.put ("ISO8859_2", new Integer (912));
    javaEncodingToCCSIDTable__.put ("ISO8859_3", new Integer (913));
    javaEncodingToCCSIDTable__.put ("ISO8859_4", new Integer (914));
    javaEncodingToCCSIDTable__.put ("ISO8859_5", new Integer (915));
    javaEncodingToCCSIDTable__.put ("ISO8859_8", new Integer (916));
    javaEncodingToCCSIDTable__.put ("Cp918", new Integer (918));
    javaEncodingToCCSIDTable__.put ("ISO8859_9", new Integer (920));
    javaEncodingToCCSIDTable__.put ("ISO8859_15_FDIS", new Integer (923));
    javaEncodingToCCSIDTable__.put ("Cp921", new Integer (921));
    javaEncodingToCCSIDTable__.put ("Cp922", new Integer (922));
    javaEncodingToCCSIDTable__.put ("Cp930", new Integer (930));
    javaEncodingToCCSIDTable__.put ("Cp933", new Integer (933));
    javaEncodingToCCSIDTable__.put ("Cp935", new Integer (935));
    javaEncodingToCCSIDTable__.put ("Cp937", new Integer (937));
    javaEncodingToCCSIDTable__.put ("Cp939", new Integer (939));
    javaEncodingToCCSIDTable__.put ("Cp948", new Integer (948));
    javaEncodingToCCSIDTable__.put ("Cp950", new Integer (950));
    javaEncodingToCCSIDTable__.put ("Cp964", new Integer (964));
    javaEncodingToCCSIDTable__.put ("Cp970", new Integer (970));
    javaEncodingToCCSIDTable__.put ("Cp1006", new Integer (1006));
    javaEncodingToCCSIDTable__.put ("Cp1025", new Integer (1025));
    javaEncodingToCCSIDTable__.put ("Cp1026", new Integer (1026));
    javaEncodingToCCSIDTable__.put ("Cp1046", new Integer (1046));
    javaEncodingToCCSIDTable__.put ("ISO8859_6", new Integer (1089));
    javaEncodingToCCSIDTable__.put ("Cp1097", new Integer (1097));
    javaEncodingToCCSIDTable__.put ("Cp1098", new Integer (1098));
    javaEncodingToCCSIDTable__.put ("Cp1112", new Integer (1112));
    javaEncodingToCCSIDTable__.put ("Cp1122", new Integer (1122));
    javaEncodingToCCSIDTable__.put ("Cp1123", new Integer (1123));
    javaEncodingToCCSIDTable__.put ("Cp1124", new Integer (1124));
    javaEncodingToCCSIDTable__.put ("Cp1140", new Integer (1140));
    javaEncodingToCCSIDTable__.put ("Cp1141", new Integer (1141));
    javaEncodingToCCSIDTable__.put ("Cp1142", new Integer (1142));
    javaEncodingToCCSIDTable__.put ("Cp1143", new Integer (1143));
    javaEncodingToCCSIDTable__.put ("Cp1144", new Integer (1144));
    javaEncodingToCCSIDTable__.put ("Cp1145", new Integer (1145));
    javaEncodingToCCSIDTable__.put ("Cp1146", new Integer (1146));
    javaEncodingToCCSIDTable__.put ("Cp1147", new Integer (1147));
    javaEncodingToCCSIDTable__.put ("Cp1148", new Integer (1148));
    javaEncodingToCCSIDTable__.put ("Cp1149", new Integer (1149));
    javaEncodingToCCSIDTable__.put ("UTF8", new Integer (1208));
    javaEncodingToCCSIDTable__.put ("Cp1250", new Integer (1250));
    javaEncodingToCCSIDTable__.put ("Cp1251", new Integer (1251));
    javaEncodingToCCSIDTable__.put ("Cp1252", new Integer (1252));
    javaEncodingToCCSIDTable__.put ("Cp1253", new Integer (1253));
    javaEncodingToCCSIDTable__.put ("Cp1254", new Integer (1254));
    javaEncodingToCCSIDTable__.put ("Cp1255", new Integer (1255));
    javaEncodingToCCSIDTable__.put ("Cp1256", new Integer (1256));
    javaEncodingToCCSIDTable__.put ("Cp1257", new Integer (1257));
    javaEncodingToCCSIDTable__.put ("Cp1258", new Integer (1258));
    javaEncodingToCCSIDTable__.put ("MacGreek", new Integer (1280));
    javaEncodingToCCSIDTable__.put ("MacTurkish", new Integer (1281));
    javaEncodingToCCSIDTable__.put ("MacCyrillic", new Integer (1283));
    javaEncodingToCCSIDTable__.put ("MacCroatian", new Integer (1284));
    javaEncodingToCCSIDTable__.put ("MacRomania", new Integer (1285));
    javaEncodingToCCSIDTable__.put ("MacIceland", new Integer (1286));
    javaEncodingToCCSIDTable__.put ("Cp1381", new Integer (1381));
    javaEncodingToCCSIDTable__.put ("Cp1383", new Integer (1383));
    javaEncodingToCCSIDTable__.put ("Cp33722", new Integer (33722));

    javaEncodingToCCSIDTable__.put ("Cp290", new Integer (8482));
    javaEncodingToCCSIDTable__.put ("Cp300", new Integer (16684));
    javaEncodingToCCSIDTable__.put ("Cp930", new Integer (1390));
    javaEncodingToCCSIDTable__.put ("Cp833", new Integer (13121));
    javaEncodingToCCSIDTable__.put ("Cp834", new Integer (4930));
    javaEncodingToCCSIDTable__.put ("Cp836", new Integer (13124));
    javaEncodingToCCSIDTable__.put ("Cp837", new Integer (4933));
    javaEncodingToCCSIDTable__.put ("Cp943", new Integer (941));
    javaEncodingToCCSIDTable__.put ("Cp1027", new Integer (5123));
    javaEncodingToCCSIDTable__.put ("Cp1043", new Integer (904));
    javaEncodingToCCSIDTable__.put ("Cp1114", new Integer (5210));
    javaEncodingToCCSIDTable__.put ("ASCII", new Integer (367));
    javaEncodingToCCSIDTable__.put ("MS932", new Integer (932));
    javaEncodingToCCSIDTable__.put ("UnicodeBigUnmarked", new Integer (1200));
    javaEncodingToCCSIDTable__.put ("Cp943", new Integer (943));
    javaEncodingToCCSIDTable__.put ("Cp1362", new Integer (1114));
    javaEncodingToCCSIDTable__.put ("Cp301", new Integer (301));
    javaEncodingToCCSIDTable__.put ("Cp1041", new Integer (1041));
    javaEncodingToCCSIDTable__.put ("Cp1351", new Integer (1351));
    javaEncodingToCCSIDTable__.put ("Cp1088", new Integer (1088));
    javaEncodingToCCSIDTable__.put ("Cp951", new Integer (951));
    javaEncodingToCCSIDTable__.put ("Cp971", new Integer (971));
    javaEncodingToCCSIDTable__.put ("Cp1362", new Integer (1362));
    javaEncodingToCCSIDTable__.put ("Cp1363", new Integer (1363));
    javaEncodingToCCSIDTable__.put ("Cp1115", new Integer (1115));
    javaEncodingToCCSIDTable__.put ("Cp1380", new Integer (1380));
    javaEncodingToCCSIDTable__.put ("Cp1385", new Integer (1385));
    javaEncodingToCCSIDTable__.put ("Cp947", new Integer (947));
    javaEncodingToCCSIDTable__.put ("Cp942", new Integer (942));
    javaEncodingToCCSIDTable__.put ("Cp897", new Integer (897));
    javaEncodingToCCSIDTable__.put ("Cp949", new Integer (949));
    javaEncodingToCCSIDTable__.put ("Cp1370", new Integer (1370));
    javaEncodingToCCSIDTable__.put ("Cp927", new Integer (927));
    javaEncodingToCCSIDTable__.put ("Cp1382", new Integer (1382));
    javaEncodingToCCSIDTable__.put ("Cp1386", new Integer (1386));
    javaEncodingToCCSIDTable__.put ("Cp835", new Integer (835));
    javaEncodingToCCSIDTable__.put ("Cp1051", new Integer (1051));
  }
  */

  static void populate_ccsidToJavaEncodingTable ()
  {
    ccsidToJavaEncodingTable__.put (new Integer (5346), "Cp1250");
    ccsidToJavaEncodingTable__.put (new Integer (5347), "Cp1251");
    ccsidToJavaEncodingTable__.put (new Integer (5348), "Cp1252");
    ccsidToJavaEncodingTable__.put (new Integer (5349), "Cp1253");
    ccsidToJavaEncodingTable__.put (new Integer (5350), "Cp1254");
    ccsidToJavaEncodingTable__.put (new Integer (5351), "Cp1255");
    ccsidToJavaEncodingTable__.put (new Integer (4909), "Cp813");
    ccsidToJavaEncodingTable__.put (new Integer (858), "Cp850"); //we can't map 858 to Cp850 because 858 has Euro characters that Cp858 doesn't support
    ccsidToJavaEncodingTable__.put (new Integer (872), "Cp855");
    ccsidToJavaEncodingTable__.put (new Integer (867), "Cp862");
    ccsidToJavaEncodingTable__.put (new Integer (17248), "Cp864");
    ccsidToJavaEncodingTable__.put (new Integer (808), "Cp866");
    ccsidToJavaEncodingTable__.put (new Integer (1162), "Cp847");
    ccsidToJavaEncodingTable__.put (new Integer (9044), "Cp852");
    ccsidToJavaEncodingTable__.put (new Integer (9048), "Cp856");
    ccsidToJavaEncodingTable__.put (new Integer (9049), "Cp857");
    ccsidToJavaEncodingTable__.put (new Integer (9061), "Cp869");
    ccsidToJavaEncodingTable__.put (new Integer (901), "Cp921");
    ccsidToJavaEncodingTable__.put (new Integer (902), "Cp922");
    ccsidToJavaEncodingTable__.put (new Integer (21427), "Cp947");
    ccsidToJavaEncodingTable__.put (new Integer (1370), "Cp950"); //we can't map 1370 to Cp1370 becasue 1370 has Euro character that Cp1370 doesn't support
    ccsidToJavaEncodingTable__.put (new Integer (5104), "Cp1008");
    ccsidToJavaEncodingTable__.put (new Integer (9238), "Cp1046");
    ccsidToJavaEncodingTable__.put (new Integer (848), "Cp1125");
    ccsidToJavaEncodingTable__.put (new Integer (1163), "Cp1129");
    ccsidToJavaEncodingTable__.put (new Integer (849), "Cp1131");
    ccsidToJavaEncodingTable__.put (new Integer (5352), "Cp1256");
    ccsidToJavaEncodingTable__.put (new Integer (5353), "Cp1257");
    ccsidToJavaEncodingTable__.put (new Integer (5354), "Cp1258");

    ccsidToJavaEncodingTable__.put (new Integer (37), "Cp037");
    ccsidToJavaEncodingTable__.put (new Integer (273), "Cp273");
    ccsidToJavaEncodingTable__.put (new Integer (277), "Cp277");
    ccsidToJavaEncodingTable__.put (new Integer (278), "Cp278");
    ccsidToJavaEncodingTable__.put (new Integer (280), "Cp280");
    ccsidToJavaEncodingTable__.put (new Integer (284), "Cp284");
    ccsidToJavaEncodingTable__.put (new Integer (285), "Cp285");
    ccsidToJavaEncodingTable__.put (new Integer (297), "Cp297");
    ccsidToJavaEncodingTable__.put (new Integer (420), "Cp420");
    ccsidToJavaEncodingTable__.put (new Integer (424), "Cp424");
    ccsidToJavaEncodingTable__.put (new Integer (437), "Cp437");
    ccsidToJavaEncodingTable__.put (new Integer (500), "Cp500");
    ccsidToJavaEncodingTable__.put (new Integer (737), "Cp737");
    ccsidToJavaEncodingTable__.put (new Integer (775), "Cp775");
    ccsidToJavaEncodingTable__.put (new Integer (838), "Cp838");
    ccsidToJavaEncodingTable__.put (new Integer (850), "Cp850");
    ccsidToJavaEncodingTable__.put (new Integer (852), "Cp852");
    ccsidToJavaEncodingTable__.put (new Integer (855), "Cp855");
    ccsidToJavaEncodingTable__.put (new Integer (856), "Cp856");
    ccsidToJavaEncodingTable__.put (new Integer (857), "Cp857");
    //ccsidToJavaEncodingTable__.put (new Integer (858), "Cp858");
    ccsidToJavaEncodingTable__.put (new Integer (860), "Cp860");
    ccsidToJavaEncodingTable__.put (new Integer (861), "Cp861");
    ccsidToJavaEncodingTable__.put (new Integer (862), "Cp862");
    ccsidToJavaEncodingTable__.put (new Integer (863), "Cp863");
    ccsidToJavaEncodingTable__.put (new Integer (864), "Cp864");
    ccsidToJavaEncodingTable__.put (new Integer (865), "Cp865");
    ccsidToJavaEncodingTable__.put (new Integer (866), "Cp866");
    ccsidToJavaEncodingTable__.put (new Integer (868), "Cp868");
    ccsidToJavaEncodingTable__.put (new Integer (869), "Cp869");
    ccsidToJavaEncodingTable__.put (new Integer (870), "Cp870");
    ccsidToJavaEncodingTable__.put (new Integer (871), "Cp871");
    ccsidToJavaEncodingTable__.put (new Integer (874), "Cp874");
    ccsidToJavaEncodingTable__.put (new Integer (875), "Cp875");
    ccsidToJavaEncodingTable__.put (new Integer (918), "Cp918");
    ccsidToJavaEncodingTable__.put (new Integer (921), "Cp921");
    ccsidToJavaEncodingTable__.put (new Integer (922), "Cp922");
    ccsidToJavaEncodingTable__.put (new Integer (930), "Cp930");
    ccsidToJavaEncodingTable__.put (new Integer (933), "Cp933");
    ccsidToJavaEncodingTable__.put (new Integer (935), "Cp935");
    ccsidToJavaEncodingTable__.put (new Integer (937), "Cp937");
    ccsidToJavaEncodingTable__.put (new Integer (939), "Cp939");
    ccsidToJavaEncodingTable__.put (new Integer (948), "Cp948");
    ccsidToJavaEncodingTable__.put (new Integer (950), "Cp950");
    ccsidToJavaEncodingTable__.put (new Integer (964), "Cp964");
    ccsidToJavaEncodingTable__.put (new Integer (970), "Cp970");
    ccsidToJavaEncodingTable__.put (new Integer (1006), "Cp1006");
    ccsidToJavaEncodingTable__.put (new Integer (1025), "Cp1025");
    ccsidToJavaEncodingTable__.put (new Integer (1026), "Cp1026");
    ccsidToJavaEncodingTable__.put (new Integer (1046), "Cp1046");
    ccsidToJavaEncodingTable__.put (new Integer (1097), "Cp1097");
    ccsidToJavaEncodingTable__.put (new Integer (1098), "Cp1098");
    ccsidToJavaEncodingTable__.put (new Integer (1112), "Cp1112");
    ccsidToJavaEncodingTable__.put (new Integer (1122), "Cp1122");
    ccsidToJavaEncodingTable__.put (new Integer (1123), "Cp1123");
    ccsidToJavaEncodingTable__.put (new Integer (1124), "Cp1124");
    ccsidToJavaEncodingTable__.put (new Integer (1140), "Cp1140");
    ccsidToJavaEncodingTable__.put (new Integer (1141), "Cp1141");
    ccsidToJavaEncodingTable__.put (new Integer (1142), "Cp1142");
    ccsidToJavaEncodingTable__.put (new Integer (1143), "Cp1143");
    ccsidToJavaEncodingTable__.put (new Integer (1144), "Cp1144");
    ccsidToJavaEncodingTable__.put (new Integer (1145), "Cp1145");
    ccsidToJavaEncodingTable__.put (new Integer (1146), "Cp1146");
    ccsidToJavaEncodingTable__.put (new Integer (1147), "Cp1147");
    ccsidToJavaEncodingTable__.put (new Integer (1148), "Cp1148");
    ccsidToJavaEncodingTable__.put (new Integer (1149), "Cp1149");
    ccsidToJavaEncodingTable__.put (new Integer (1250), "Cp1250");
    ccsidToJavaEncodingTable__.put (new Integer (1251), "Cp1251");
    ccsidToJavaEncodingTable__.put (new Integer (1252), "Cp1252");
    ccsidToJavaEncodingTable__.put (new Integer (1253), "Cp1253");
    ccsidToJavaEncodingTable__.put (new Integer (1254), "Cp1254");
    ccsidToJavaEncodingTable__.put (new Integer (1255), "Cp1255");
    ccsidToJavaEncodingTable__.put (new Integer (1256), "Cp1256");
    ccsidToJavaEncodingTable__.put (new Integer (1257), "Cp1257");
    ccsidToJavaEncodingTable__.put (new Integer (1258), "Cp1258");
    ccsidToJavaEncodingTable__.put (new Integer (1381), "Cp1381");
    ccsidToJavaEncodingTable__.put (new Integer (1383), "Cp1383");
    ccsidToJavaEncodingTable__.put (new Integer (33722), "Cp33722");
    ccsidToJavaEncodingTable__.put (new Integer (943), "Cp943");
    ccsidToJavaEncodingTable__.put (new Integer (1043), "Cp1043");

    ccsidToJavaEncodingTable__.put (new Integer (813), "ISO8859_7");
    ccsidToJavaEncodingTable__.put (new Integer (819), "ISO8859_1");
    ccsidToJavaEncodingTable__.put (new Integer (878), "KOI8_R");
    ccsidToJavaEncodingTable__.put (new Integer (912), "ISO8859_2");
    ccsidToJavaEncodingTable__.put (new Integer (913), "ISO8859_3");
    ccsidToJavaEncodingTable__.put (new Integer (914), "ISO8859_4");
    ccsidToJavaEncodingTable__.put (new Integer (915), "ISO8859_5");
    ccsidToJavaEncodingTable__.put (new Integer (916), "ISO8859_8");
    ccsidToJavaEncodingTable__.put (new Integer (920), "ISO8859_9");
    ccsidToJavaEncodingTable__.put (new Integer (923), "ISO8859_15_FDIS");
    ccsidToJavaEncodingTable__.put (new Integer (1089), "ISO8859_6");
    ccsidToJavaEncodingTable__.put (new Integer (1208), "UTF8");
    ccsidToJavaEncodingTable__.put (new Integer (1280), "MacGreek");
    ccsidToJavaEncodingTable__.put (new Integer (1281), "MacTurkish");
    ccsidToJavaEncodingTable__.put (new Integer (1283), "MacCyrillic");
    ccsidToJavaEncodingTable__.put (new Integer (1284), "MacCroatian");
    ccsidToJavaEncodingTable__.put (new Integer (1285), "MacRomania");
    ccsidToJavaEncodingTable__.put (new Integer (1286), "MacIceland");
    ccsidToJavaEncodingTable__.put (new Integer (8482), "Cp290");
    ccsidToJavaEncodingTable__.put (new Integer (16684), "Cp300");
    ccsidToJavaEncodingTable__.put (new Integer (1390), "Cp930");
    ccsidToJavaEncodingTable__.put (new Integer (13121), "Cp833");
    ccsidToJavaEncodingTable__.put (new Integer (4930), "Cp834");
    ccsidToJavaEncodingTable__.put (new Integer (13124), "Cp836");
    ccsidToJavaEncodingTable__.put (new Integer (4933), "Cp837");
    ccsidToJavaEncodingTable__.put (new Integer (941), "Cp943");
    ccsidToJavaEncodingTable__.put (new Integer (5123), "Cp1027");
    ccsidToJavaEncodingTable__.put (new Integer (904), "Cp1043");
    ccsidToJavaEncodingTable__.put (new Integer (5210), "Cp1114");
    ccsidToJavaEncodingTable__.put (new Integer (367), "ASCII");
    ccsidToJavaEncodingTable__.put (new Integer (932), "MS932");
    ccsidToJavaEncodingTable__.put (new Integer (1200), "UnicodeBigUnmarked");
    ccsidToJavaEncodingTable__.put (new Integer (5026), "Cp930");
    ccsidToJavaEncodingTable__.put (new Integer (1399), "Cp939");
    ccsidToJavaEncodingTable__.put (new Integer (4396), "Cp300");
    ccsidToJavaEncodingTable__.put (new Integer (1388), "Cp935");
    ccsidToJavaEncodingTable__.put (new Integer (1364), "Cp933");
    ccsidToJavaEncodingTable__.put (new Integer (5035), "Cp939");
    ccsidToJavaEncodingTable__.put (new Integer (28709), "Cp37");
    ccsidToJavaEncodingTable__.put (new Integer (1114), "Cp1362");
    ccsidToJavaEncodingTable__.put (new Integer (954), "Cp33722");

    //----the following codepages may  only be supported by IBMSDk 1.3.1
    ccsidToJavaEncodingTable__.put (new Integer (301), "Cp301");
    ccsidToJavaEncodingTable__.put (new Integer (1041), "Cp1041");
    ccsidToJavaEncodingTable__.put (new Integer (1351), "Cp1351");
    ccsidToJavaEncodingTable__.put (new Integer (1088), "Cp1088");
    ccsidToJavaEncodingTable__.put (new Integer (951), "Cp951");
    ccsidToJavaEncodingTable__.put (new Integer (971), "Cp971");
    ccsidToJavaEncodingTable__.put (new Integer (1362), "Cp1362");
    ccsidToJavaEncodingTable__.put (new Integer (1363), "Cp1363");
    ccsidToJavaEncodingTable__.put (new Integer (1115), "Cp1115");
    ccsidToJavaEncodingTable__.put (new Integer (1380), "Cp1380");
    ccsidToJavaEncodingTable__.put (new Integer (1386), "Cp1386");
    ccsidToJavaEncodingTable__.put (new Integer (1385), "Cp1385");
    ccsidToJavaEncodingTable__.put (new Integer (947), "Cp947");
    ccsidToJavaEncodingTable__.put (new Integer (942), "Cp942");
    ccsidToJavaEncodingTable__.put (new Integer (897), "Cp897");
    ccsidToJavaEncodingTable__.put (new Integer (949), "Cp949");
    ccsidToJavaEncodingTable__.put (new Integer (927), "Cp927");
    ccsidToJavaEncodingTable__.put (new Integer (1382), "Cp1382");
    ccsidToJavaEncodingTable__.put (new Integer (290), "Cp290");
    ccsidToJavaEncodingTable__.put (new Integer (300), "Cp300");
    ccsidToJavaEncodingTable__.put (new Integer (1027), "Cp1027");
    ccsidToJavaEncodingTable__.put (new Integer (16686), "Cp16686");
    ccsidToJavaEncodingTable__.put (new Integer (833), "Cp833");
    ccsidToJavaEncodingTable__.put (new Integer (834), "Cp834");
    ccsidToJavaEncodingTable__.put (new Integer (836), "Cp836");
    ccsidToJavaEncodingTable__.put (new Integer (837), "Cp837");
    ccsidToJavaEncodingTable__.put (new Integer (835), "Cp835");
    ccsidToJavaEncodingTable__.put (new Integer (895), "Cp33722");
    ccsidToJavaEncodingTable__.put (new Integer (1051), "Cp1051");
    ccsidToJavaEncodingTable__.put (new Integer (13488), "UnicodeBigUnmarked");

  }

  /*
  public static int getCCSID (String javaEncoding) throws java.io.UnsupportedEncodingException
  {
    int ccsid = ((Integer) javaEncodingToCCSIDTable__.get (javaEncoding)).intValue();
    if (ccsid == 0)
      throw new java.io.UnsupportedEncodingException ("unsupported java encoding");
    else
      return ccsid;
  }
  */

  public static String getJavaEncoding (int ccsid) throws java.io.UnsupportedEncodingException
  {
    String javaEncoding = (String) ccsidToJavaEncodingTable__.get (new Integer (ccsid));
    if (javaEncoding == null)
      throw new java.io.UnsupportedEncodingException ("unsupported ccsid");
    else
      return javaEncoding;
  }
}
