#!/bin/bash
#################################################################
#
# usage: ./script.sh <file to be parsed> <new CSV file>
#
################################################################

output="$1"
new_output="$2"
awk -F, '/IP/ && /tcp/' $output | sed  's/ > / / ; s/: tcp/ tcp/ ; s/ /\t\t/g' | \
sed -n  's/\([0-9]\{1,3\}\.\)\{3\}[0-9]\{1,3\}/ & \t\t/gp' | \
awk 'BEGIN {OFS=FS="\t\t"} {gsub(/\./,"",$4);gsub(/\./,"",$6)}1' | \
awk -vOS=',' -vcdate=$(date '+%Y-%m-%d') ' {print cdate, $0}' >> $new_output

sed -i -r 's/\+s/-/' $new_output
sed -i 's/\t\t/,/g' $new_output

##########################################################
#
# this script to be implemented on TCP traffic only!
#
