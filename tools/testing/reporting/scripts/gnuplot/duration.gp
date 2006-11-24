set ylabel "Test run duration in % of baseline."

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
set out "plot.png"

plot "data.dat" using 1:3 title "Duration over revision"
# plot "data.dat" using 1:3:xticlabels(2) title "Duration over revision"
