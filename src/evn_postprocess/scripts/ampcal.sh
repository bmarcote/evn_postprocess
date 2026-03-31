#!/bin/bash
###### ampcal.sh
###### Jay Blanchard 2017 - bash script to do ampcal db stuff for an experiment
###### run in the directory for the experiment. Can easily be wrapped for a list of exps etc.
###### needs to pull from ccs to handle jb1/2 so needs ssh keys to be ok.
## last modified: 2022-12-08
## version update notes:
## v2 (Gabor)
## - added an option to not use the BPASS list in the inp file, but rather the source given in argument $1. Only works for single sources now.
## - also changed that if nothing is specified, used sources from both the bpass and phaseref list in the inp file.
## - added a help file. call it with -h.

################################################################################
# Help                                                                         #
################################################################################
Help()
{
# Display Help
    echo
    echo "Get the ampcal information for a source to insert it into the Grafana database."
    echo 
    echo "Usage: run in the OUT directory of the experiment."
    echo "If no argument is specified, use BPASS and PHASEREF source list from pipeline inp file."
    echo "If source name is given with existing ampcal file, insert that into the database."
    echo "If your vex file uses non standard naming (not exp.vix) please scp and rename it yourself."
    echo
    echo "options:"
    echo "-h    Print this Help."
    echo "-e evlbi_name"
    echo "      evlbi_name is the name of the first experiment in the eVLBI session"
    echo "\$1    Insert only specified source into database."
    echo "none  Insert all bpass and phaseref sources into database."
    echo
}
# Get the options
while getopts ":h" option; do
    case $option in
        h) # display Help
            Help
            exit;;
    esac
done                              
################################################################################                                                            
                              

function readAmpcal {
 #$1 is the first argument -> the source for which to get the ampcal
local source=$1
#echo "readAmpcal thinks source is ${source}"

#check if ampcal exists for the source (exp_name will have been defined by the time this is run... scope in bash scripts eh :D)
if [[ -f "${exp_name}_${source}_CALIB_AMP2.ampcal" ]]; then
    local amp_file="${exp_name}_${source}_CALIB_AMP2.ampcal"

#else maybe we have a spectral line exp
elif [[ -f "${exp_name}_1_${source}_CALIB_AMP2.ampcal" ]]; then
    local amp_file=${exp_name}_1_${source}_CALIB_AMP2.ampcal

#else maybe Benito has named his _0 just to be difficult
elif [[ -f "${exp_name}_0_${source}_CALIB_AMP2.ampcal" ]]; then
    local amp_file=${exp_name}_0_${source}_CALIB_AMP2.ampcal


else 
    echo "There is no ampcal file for calibrator ${source}"
fi


#if file was found!

if [[ -f "${amp_file}" ]]; then
#copy the file first then sed for JB
#I think we no longer care about keeping things separate, we should worry about that when plotting!
    cp -puv ${amp_file} $OUT/ampcal/new/
#fix JB
    sed -i "s/(JB)/(${jodrell})/g" "$OUT/ampcal/new/${amp_file}"

    #need to do ampcaldb as well as update database
    #need to do this in that directory because ampcaldb.pl is stupid. note: THIS BROKE EVERYTHING
    cd "$OUT/ampcal/new"
    ampcaldb.pl "${amp_file}"
    cat "ampcal.dat" >> "ampcal.all.dat"
    #insert into database:
    ampcal-db.py
    #have to return to where we were...
    cd "-" 1>/dev/null
fi
}



###### START MAIN

#exp name from PWD
exp_name=${PWD##*/}

#getopts because eVLBI is _special_
usage() { echo "Usage: ampcal.sh [-e evlbi_name]"; echo "evlbi_name is the name of the first experiment in the eVLBI session";echo "If your vex file uses non standard naming (not exp.vix) please scp and rename it yourself."; exit 1; }
while getopts ":e:" opt; do
    case $opt in
        e)
            vexName=${OPTARG,,}
            echo "eVLBI mode, using vex $vexName.vix"
            ;;
        *)
            usage
            ;;
     esac
done



#we need to check we're not in $IN
if [[ "${PWD}" == "${IN}/${exp_name}" ]]; then 
    echo "You are in ${PWD}! Please run from ${OUT}/${exp_name}"
    exit 1
fi

#special handling for JB:
#find out if we are using jb1 or 2
#need to get vix from ccs (if it doesn't exist):
if [[ ! -f "${exp_name}.vix" ]]; then

    #if eVLBI we need to copy the evlbi named vix file (we also rename it here so the rest of the code doesn't need changing)
    if [[ -n "${vexName}" ]]; then
	scp jops@ccs:/ccs/expr/"${vexName^^}"/"${vexName}".vix "${exp_name}".vix
    else
	scp jops@ccs:/ccs/expr/"${exp_name^^}"/"${exp_name}".vix .
    fi
fi

#check if we found it
if [[ ! -f "${exp_name}.vix" ]]; then
    echo "Could not find vix. Are you in the experiment directory?"
    exit 1
fi

##grep the file for "JODRELL2" if so we are jb2
if [[ -z `grep JODRELL2 ${exp_name}.vix` ]]; then
    jodrell="J1"
else
    jodrell="J2"
fi

#echo "We are ${jodrell}"

#we need to get the list of bandpass and phase ref cals
#we must cover the case of multi phase centre or spectral line
#first see if $IN/expname.inp.txt exists

if [[ -f "${IN}/${exp_name}/${exp_name}.inp.txt" ]]; then
    bpass_str=`grep -h bpass ${IN}/${exp_name}/${exp_name}.inp.txt`
    phaseref_str=`grep -h phaseref ${IN}/${exp_name}/${exp_name}.inp.txt`
#else check for _1.inp.txt
elif [[ -f  "${IN}/${exp_name}/${exp_name}_1.inp.txt" ]];  then
    bpass_str=`grep -h bpass ${IN}/${exp_name}/${exp_name}_1.inp.txt`
    phaseref_str=`grep -h phaseref ${IN}/${exp_name}/${exp_name}_1.inp.txt`

elif [[ -f  "${IN}/${exp_name}/${exp_name}_0.inp.txt" ]];  then
    bpass_str=`grep -h bpass ${IN}/${exp_name}/${exp_name}_0.inp.txt`
    phaseref_str=`grep -h phaseref ${IN}/${exp_name}/${exp_name}_0.inp.txt`
else
    echo "No input file found in ${IN}/${exp_name}! Have you run the pipeline?"
    exit 1
fi


if [[ -z bpass_str ]] || [[ -z phaseref_str ]]; then
    echo "No bpass or phaseref sources found."
    exit -1
elif [[ -z "$1" ]]; then
    IFS=', ' read -r -a bpass <<< ${bpass_str##*=}
    IFS=', ' read -r -a phaseref <<< ${phaseref_str##*=}
#loop through the calibrator list and get the ampcal for each
    for cal in "${bpass[@]}"
    do
        echo "Doing calibrator: ${cal}"
        readAmpcal "${cal}"
    done
    for cal in "${phaseref[@]}"
    do
        echo "Doing calibrator: ${cal}"
        readAmpcal "${cal}"
    done
#if a single source name is given at $1, run through that instead    
else
    echo ""
    echo "Doing calibrator: $1"
    readAmpcal "$1"
fi

