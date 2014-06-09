#!/bin/bash

### Modified by Peichun Cheng (2009-Mar)

## Usage
function usage {
  printf "Usage:\n"
  printf "  raw2txt.sh [option] rawfiles.txt\n\n"
  printf "  \"rawfiles.txt\"\n\t  A text file listing (one per line)"
  printf "\n\t  rawfiles to convert to machine readable ASCII files\n"
  printf "  \"-r\"\n\t  (Optional) Remove raw file(s) after conversion\n"
  exit 1
}

## bgpdump program (Configure to /path/to/bgpdump)
bgpdump_prog="/usr/local/bin/bgpdump"
bgpdump_options="-m"
bgpdump_cmd="$bgpdump_prog $bgpdump_options"

## check if user supplied correct number of arguments
if [ $# -lt 1 ]; then
 echo "Incorrect number of input arguments"
 usage
fi

## check if 'bgpdump' program exists and is executable
if [ ! -f $bgpdump_prog ]; then
  echo "Unable to find 'bgpdump' program; Please configure raw2txt.sh"
  exit 1
elif [ ! -x $bgpdump_prog ]; then
  echo "'bgpdump' exists but is not executable; Please change permissions on 'bgpdump'"
  exit 1
fi

## don't remove raw files (default)
remove=0


## parse command line agrs
if [ $# -gt 1 ]; then
  if [ $1 -eq "-r" ]; then
    remove=1
    shift
  elif [ $2 -eq "-r" ]; then
    remove=1
  else
    echo "Unrecognized parameters present on command line"
    usage
  fi
fi

rawfilelist=$1

for infile in `cat $rawfilelist`; do

  ## figure out if we going to use 'gzip' or 'bzip2' for uncompressing rawfile
  type=`echo $infile | awk -F'.' '{print $NF}'`


  if [ "$type" == "gz" ]; then
    uncompress="gzip"
  elif [ $type == "bz2" ]; then
    uncompress="bzip2"
  else
    echo "Unrecognized file type: $type from rawfile: $infile"
  fi

  outfile=`echo $infile | sed 's/\.gz$//'`
  
  #echo "$uncompress -dc ${infile} | $bgpdump_cmd | gzip -9 > ${outfile}.txt.gz"
  #$uncompress -dc ${infile} | $bgpdump_cmd | gzip -9 > ${outfile}.txt.gz

  cmd="$bgpdump_cmd ${infile} | gzip -9 > ${outfile}.txt.gz"
  echo $cmd

  eval $cmd
  
  if [ $? -ne 0 ]; then
    printf "\nCommand Failed: $uncompress -dc ${infile} | $bgpdump_cmd"
    printf " | gzip -9 > ${outfile}.txt.gz\n"
    exit 1
  fi
  
  if [ $remove == 1 ]; then
    echo "rm -f $infile"
    rm -f ${infile}
  fi

done

echo "Success: converting rawfiles to machine readable ASCII files completed"
