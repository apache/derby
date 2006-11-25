# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set ylabel "${SUITE} run duration in % of baseline."

set xdata time
set timefmt "%Y-%m-%d"
set xrange [*:*]
set yrange [0:*]
set format x "-%m-%d"
# set timefmt "%Y-%m-%d"
set data style linespoints
set xtics rotate
set grid xtics
set key top left
set xtics 604800
# set style histogram clustered

set term png color
set out "plot_all.png"

# plot "data.dat" using 1:3 title "Duration over revision"

# CYGWIN_NT-5.2_i686-unknown
# Linux-2.4.20-31.9_i686-i686
# Linux-2.4.21-27.ELsmp_i686-athlon
# SunOS-5.10_i86pc-i386
# SunOS-5.10_sun4u-sparc
# SunOS-5.9_sun4u-sparc
plot "CYGWIN_NT-5.2_i686-unknown/${SUITE}.data" using 1:3 title "CYGWIN_NT-5.2_i686-unknown", "Linux-2.4.20-31.9_i686-i686/${SUITE}.data" using 1:3 title "Linux-2.4.20-31.9_i686-i686", "Linux-2.4.21-27.ELsmp_i686-athlon/${SUITE}.data" using 1:3 title "Linux-2.4.21-27.ELsmp_i686-athlon", "SunOS-5.10_i86pc-i386/${SUITE}.data" using 1:3 title "SunOS-5.10_i86pc-i386", "SunOS-5.10_sun4u-sparc/${SUITE}.data" using 1:3 title "SunOS-5.10_sun4u-sparc", "SunOS-5.9_sun4u-sparc/${SUITE}.data" using 1:3 title "SunOS-5.9_sun4u-sparc"

