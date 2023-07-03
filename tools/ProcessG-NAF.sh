# This script processes some ABS files and some of the cut down G-NAF files
# that have been prepared by the PrepareG-NAF.sh script
# PrepareG-NAF.sh must be run before this script
# This script takes a single argument, being the folder containing the ABS data

if [ $# -ne 1 ]
then
	echo 'ABS directory is a manadatory argument'
	exit 0
fi
ABSdir="$1"

# Create the street_numbers.psv, locality_postcode.psv and buildings.psv files
if test -f "extraFlats.psv" && test -f "extraLevels.psv" && test -f "extraTrims.psv" && test -f "extraStates.psv" && test -f "extraStreetTypes.psv" && test -f "extraStreetSuffixes.psv" && test -f "extraPostcodeSA1LGA.psv" && test -f "extraLocality.psv" ; then
    echo "extraFlats.psv extraLevels.psv extraTrims.psv extraStates.psv extraStreetTypes.psv extraStreetSuffixes.psv extraPostcodeSA1LGA.psv and extraLocality.psv already created"
elif test ! -f "state.psv" ; then
	echo "required file state.psv is missing. Run PrepareG-NAF.sh"
elif test ! -f "locality.psv" ; then
	echo "required file locality.psv is missing. Run PrepareG-NAF.sh"
elif test ! -f "postcode_SA1LGA.psv" ; then
	echo "required file postcode_SA1LGA.psv is missing. Run PrepareG-NAF.sh"
else
	echo "creating extraFlats.psv, extraLevels.psv, extraTrims.psv, extraStreetTypes.psv, extraPostcodeSA1LGA.psv and extraLocality.psv"
	python3 getExtras.py -A ${ABSdir}
fi

