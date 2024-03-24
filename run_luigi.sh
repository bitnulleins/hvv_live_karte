#!/bin/bash

while getopts ":t:" opt; do
  case $opt in
    t) seconds="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac

  case $OPTARG in
    -*) echo "Option $opt needs a valid argument (timerange in seconds)"
    exit 1
    ;;
  esac
done

if ! [[ "$seconds" =~ ^[0-9]+$ && $seconds -ge 30 ]] ; then
  printf "%s is not a positive number or not greater than 30 seconds (minimum)\n" "$seconds"
else     
  printf "  __                                            \n"
  printf " /\ \                  __                __     \n"
  printf " \ \ \        __  __  /\_\       __     /\_\    \n"
  printf "  \ \ \  __  /\ \/\ \ \/\ \    /'_  \   \/\ \   \n"
  printf "   \ \ \L\ \ \ \ \_\ \ \ \ \  /\ \L\ \   \ \ \  \n"
  printf "    \ \____/  \ \____/  \ \_\ \ \____ \   \ \_\ \n"
  printf "     \/___/    \/___/    \/_/  \/___L\ \   \/_/ \n"
  printf "                                /\____/         \n"
  printf "                                \_/__/          \n"
  
  printf "[INFO] Run LUIGI ETL Pipelines for %s seconds\n" "$seconds"

  i=1
  while true
  do
    printf "[INFO] ------------------------ Run #%s: ------------------------ \n" "$i"
    python -m luigi --module src.etl.propagate propagate
    i=`expr $i + 1`
    sleep $seconds
  done
fi