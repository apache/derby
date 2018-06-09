/*

   Derby - Class org.apache.derby.impl.drda.CharacterEncodings

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

package org.apache.derby.impl.drda;

import java.util.HashMap;

final class CharacterEncodings
{

  // This is a static class, so hide the default constructor.
  private CharacterEncodings () {}

  private static final HashMap<Integer, String> ccsidToJavaEncodingTable__ =
          new HashMap<Integer, String>();

  static {
    populate_ccsidToJavaEncodingTable();
  }

  static void populate_ccsidToJavaEncodingTable ()
  {
    ccsidToJavaEncodingTable__.put(5346, "Cp1250");
    ccsidToJavaEncodingTable__.put(5347, "Cp1251");
    ccsidToJavaEncodingTable__.put(5348, "Cp1252");
    ccsidToJavaEncodingTable__.put(5349, "Cp1253");
    ccsidToJavaEncodingTable__.put(5350, "Cp1254");
    ccsidToJavaEncodingTable__.put(5351, "Cp1255");
    ccsidToJavaEncodingTable__.put(4909, "Cp813");
    // We can't map 858 to Cp850 because 858 has Euro characters that
    // Cp858 doesn't support.
    ccsidToJavaEncodingTable__.put(858, "Cp850");
    ccsidToJavaEncodingTable__.put(872, "Cp855");
    ccsidToJavaEncodingTable__.put(867, "Cp862");
    ccsidToJavaEncodingTable__.put(17248, "Cp864");
    ccsidToJavaEncodingTable__.put(808, "Cp866");
    ccsidToJavaEncodingTable__.put(1162, "Cp847");
    ccsidToJavaEncodingTable__.put(9044, "Cp852");
    ccsidToJavaEncodingTable__.put(9048, "Cp856");
    ccsidToJavaEncodingTable__.put(9049, "Cp857");
    ccsidToJavaEncodingTable__.put(9061, "Cp869");
    ccsidToJavaEncodingTable__.put(901, "Cp921");
    ccsidToJavaEncodingTable__.put(902, "Cp922");
    ccsidToJavaEncodingTable__.put(21427, "Cp947");
    // We can't map 1370 to Cp1370 because 1370 has Euro character that
    // Cp1370 doesn't support.
    ccsidToJavaEncodingTable__.put(1370, "Cp950");
    ccsidToJavaEncodingTable__.put(5104, "Cp1008");
    ccsidToJavaEncodingTable__.put(9238, "Cp1046");
    ccsidToJavaEncodingTable__.put(848, "Cp1125");
    ccsidToJavaEncodingTable__.put(1163, "Cp1129");
    ccsidToJavaEncodingTable__.put(849, "Cp1131");
    ccsidToJavaEncodingTable__.put(5352, "Cp1256");
    ccsidToJavaEncodingTable__.put(5353, "Cp1257");
    ccsidToJavaEncodingTable__.put(5354, "Cp1258");

    ccsidToJavaEncodingTable__.put(37, "Cp037");
    ccsidToJavaEncodingTable__.put(273, "Cp273");
    ccsidToJavaEncodingTable__.put(277, "Cp277");
    ccsidToJavaEncodingTable__.put(278, "Cp278");
    ccsidToJavaEncodingTable__.put(280, "Cp280");
    ccsidToJavaEncodingTable__.put(284, "Cp284");
    ccsidToJavaEncodingTable__.put(285, "Cp285");
    ccsidToJavaEncodingTable__.put(297, "Cp297");
    ccsidToJavaEncodingTable__.put(420, "Cp420");
    ccsidToJavaEncodingTable__.put(424, "Cp424");
    ccsidToJavaEncodingTable__.put(437, "Cp437");
    ccsidToJavaEncodingTable__.put(500, "Cp500");
    ccsidToJavaEncodingTable__.put(737, "Cp737");
    ccsidToJavaEncodingTable__.put(775, "Cp775");
    ccsidToJavaEncodingTable__.put(838, "Cp838");
    ccsidToJavaEncodingTable__.put(850, "Cp850");
    ccsidToJavaEncodingTable__.put(852, "Cp852");
    ccsidToJavaEncodingTable__.put(855, "Cp855");
    ccsidToJavaEncodingTable__.put(856, "Cp856");
    ccsidToJavaEncodingTable__.put(857, "Cp857");
    //ccsidToJavaEncodingTable__.put(858, "Cp858");
    ccsidToJavaEncodingTable__.put(860, "Cp860");
    ccsidToJavaEncodingTable__.put(861, "Cp861");
    ccsidToJavaEncodingTable__.put(862, "Cp862");
    ccsidToJavaEncodingTable__.put(863, "Cp863");
    ccsidToJavaEncodingTable__.put(864, "Cp864");
    ccsidToJavaEncodingTable__.put(865, "Cp865");
    ccsidToJavaEncodingTable__.put(866, "Cp866");
    ccsidToJavaEncodingTable__.put(868, "Cp868");
    ccsidToJavaEncodingTable__.put(869, "Cp869");
    ccsidToJavaEncodingTable__.put(870, "Cp870");
    ccsidToJavaEncodingTable__.put(871, "Cp871");
    ccsidToJavaEncodingTable__.put(874, "Cp874");
    ccsidToJavaEncodingTable__.put(875, "Cp875");
    ccsidToJavaEncodingTable__.put(918, "Cp918");
    ccsidToJavaEncodingTable__.put(921, "Cp921");
    ccsidToJavaEncodingTable__.put(922, "Cp922");
    ccsidToJavaEncodingTable__.put(930, "Cp930");
    ccsidToJavaEncodingTable__.put(933, "Cp933");
    ccsidToJavaEncodingTable__.put(935, "Cp935");
    ccsidToJavaEncodingTable__.put(937, "Cp937");
    ccsidToJavaEncodingTable__.put(939, "Cp939");
    ccsidToJavaEncodingTable__.put(948, "Cp948");
    ccsidToJavaEncodingTable__.put(950, "Cp950");
    ccsidToJavaEncodingTable__.put(964, "Cp964");
    ccsidToJavaEncodingTable__.put(970, "Cp970");
    ccsidToJavaEncodingTable__.put(1006, "Cp1006");
    ccsidToJavaEncodingTable__.put(1025, "Cp1025");
    ccsidToJavaEncodingTable__.put(1026, "Cp1026");
    ccsidToJavaEncodingTable__.put(1046, "Cp1046");
    ccsidToJavaEncodingTable__.put(1097, "Cp1097");
    ccsidToJavaEncodingTable__.put(1098, "Cp1098");
    ccsidToJavaEncodingTable__.put(1112, "Cp1112");
    ccsidToJavaEncodingTable__.put(1122, "Cp1122");
    ccsidToJavaEncodingTable__.put(1123, "Cp1123");
    ccsidToJavaEncodingTable__.put(1124, "Cp1124");
    ccsidToJavaEncodingTable__.put(1140, "Cp1140");
    ccsidToJavaEncodingTable__.put(1141, "Cp1141");
    ccsidToJavaEncodingTable__.put(1142, "Cp1142");
    ccsidToJavaEncodingTable__.put(1143, "Cp1143");
    ccsidToJavaEncodingTable__.put(1144, "Cp1144");
    ccsidToJavaEncodingTable__.put(1145, "Cp1145");
    ccsidToJavaEncodingTable__.put(1146, "Cp1146");
    ccsidToJavaEncodingTable__.put(1147, "Cp1147");
    ccsidToJavaEncodingTable__.put(1148, "Cp1148");
    ccsidToJavaEncodingTable__.put(1149, "Cp1149");
    ccsidToJavaEncodingTable__.put(1250, "Cp1250");
    ccsidToJavaEncodingTable__.put(1251, "Cp1251");
    ccsidToJavaEncodingTable__.put(1252, "Cp1252");
    ccsidToJavaEncodingTable__.put(1253, "Cp1253");
    ccsidToJavaEncodingTable__.put(1254, "Cp1254");
    ccsidToJavaEncodingTable__.put(1255, "Cp1255");
    ccsidToJavaEncodingTable__.put(1256, "Cp1256");
    ccsidToJavaEncodingTable__.put(1257, "Cp1257");
    ccsidToJavaEncodingTable__.put(1258, "Cp1258");
    ccsidToJavaEncodingTable__.put(1381, "Cp1381");
    ccsidToJavaEncodingTable__.put(1383, "Cp1383");
    ccsidToJavaEncodingTable__.put(33722, "Cp33722");
    ccsidToJavaEncodingTable__.put(943, "Cp943");
    ccsidToJavaEncodingTable__.put(1043, "Cp1043");

    ccsidToJavaEncodingTable__.put(813, "ISO8859_7");
    ccsidToJavaEncodingTable__.put(819, "ISO8859_1");
    ccsidToJavaEncodingTable__.put(878, "KOI8_R");
    ccsidToJavaEncodingTable__.put(912, "ISO8859_2");
    ccsidToJavaEncodingTable__.put(913, "ISO8859_3");
    ccsidToJavaEncodingTable__.put(914, "ISO8859_4");
    ccsidToJavaEncodingTable__.put(915, "ISO8859_5");
    ccsidToJavaEncodingTable__.put(916, "ISO8859_8");
    ccsidToJavaEncodingTable__.put(920, "ISO8859_9");
    ccsidToJavaEncodingTable__.put(923, "ISO8859_15_FDIS");
    ccsidToJavaEncodingTable__.put(1089, "ISO8859_6");
    ccsidToJavaEncodingTable__.put(1208, "UTF8");
    ccsidToJavaEncodingTable__.put(1280, "MacGreek");
    ccsidToJavaEncodingTable__.put(1281, "MacTurkish");
    ccsidToJavaEncodingTable__.put(1283, "MacCyrillic");
    ccsidToJavaEncodingTable__.put(1284, "MacCroatian");
    ccsidToJavaEncodingTable__.put(1285, "MacRomania");
    ccsidToJavaEncodingTable__.put(1286, "MacIceland");
    ccsidToJavaEncodingTable__.put(8482, "Cp290");
    ccsidToJavaEncodingTable__.put(16684, "Cp300");
    ccsidToJavaEncodingTable__.put(1390, "Cp930");
    ccsidToJavaEncodingTable__.put(13121, "Cp833");
    ccsidToJavaEncodingTable__.put(4930, "Cp834");
    ccsidToJavaEncodingTable__.put(13124, "Cp836");
    ccsidToJavaEncodingTable__.put(4933, "Cp837");
    ccsidToJavaEncodingTable__.put(941, "Cp943");
    ccsidToJavaEncodingTable__.put(5123, "Cp1027");
    ccsidToJavaEncodingTable__.put(904, "Cp1043");
    ccsidToJavaEncodingTable__.put(5210, "Cp1114");
    ccsidToJavaEncodingTable__.put(367, "ASCII");
    ccsidToJavaEncodingTable__.put(932, "MS932");
    ccsidToJavaEncodingTable__.put(1200, "UnicodeBigUnmarked");
    ccsidToJavaEncodingTable__.put(5026, "Cp930");
    ccsidToJavaEncodingTable__.put(1399, "Cp939");
    ccsidToJavaEncodingTable__.put(4396, "Cp300");
    ccsidToJavaEncodingTable__.put(1388, "Cp935");
    ccsidToJavaEncodingTable__.put(1364, "Cp933");
    ccsidToJavaEncodingTable__.put(5035, "Cp939");
    ccsidToJavaEncodingTable__.put(28709, "Cp37");
    ccsidToJavaEncodingTable__.put(1114, "Cp1362");
    ccsidToJavaEncodingTable__.put(954, "Cp33722");

    //----the following codepages may  only be supported by IBMSDk 1.3.1
    ccsidToJavaEncodingTable__.put(301, "Cp301");
    ccsidToJavaEncodingTable__.put(1041, "Cp1041");
    ccsidToJavaEncodingTable__.put(1351, "Cp1351");
    ccsidToJavaEncodingTable__.put(1088, "Cp1088");
    ccsidToJavaEncodingTable__.put(951, "Cp951");
    ccsidToJavaEncodingTable__.put(971, "Cp971");
    ccsidToJavaEncodingTable__.put(1362, "Cp1362");
    ccsidToJavaEncodingTable__.put(1363, "Cp1363");
    ccsidToJavaEncodingTable__.put(1115, "Cp1115");
    ccsidToJavaEncodingTable__.put(1380, "Cp1380");
    ccsidToJavaEncodingTable__.put(1386, "Cp1386");
    ccsidToJavaEncodingTable__.put(1385, "Cp1385");
    ccsidToJavaEncodingTable__.put(947, "Cp947");
    ccsidToJavaEncodingTable__.put(942, "Cp942");
    ccsidToJavaEncodingTable__.put(897, "Cp897");
    ccsidToJavaEncodingTable__.put(949, "Cp949");
    ccsidToJavaEncodingTable__.put(927, "Cp927");
    ccsidToJavaEncodingTable__.put(1382, "Cp1382");
    ccsidToJavaEncodingTable__.put(290, "Cp290");
    ccsidToJavaEncodingTable__.put(300, "Cp300");
    ccsidToJavaEncodingTable__.put(1027, "Cp1027");
    ccsidToJavaEncodingTable__.put(16686, "Cp16686");
    ccsidToJavaEncodingTable__.put(833, "Cp833");
    ccsidToJavaEncodingTable__.put(834, "Cp834");
    ccsidToJavaEncodingTable__.put(836, "Cp836");
    ccsidToJavaEncodingTable__.put(837, "Cp837");
    ccsidToJavaEncodingTable__.put(835, "Cp835");
    ccsidToJavaEncodingTable__.put(895, "Cp33722");
    ccsidToJavaEncodingTable__.put(1051, "Cp1051");
    ccsidToJavaEncodingTable__.put(13488, "UnicodeBigUnmarked");

  }

  public static String getJavaEncoding (int ccsid) throws java.io.UnsupportedEncodingException
  {
    String javaEncoding = ccsidToJavaEncodingTable__.get(ccsid);
    if (javaEncoding == null) {
      throw new java.io.UnsupportedEncodingException ("unsupported ccsid");
    } else {
      return javaEncoding;
    }
  }
}
