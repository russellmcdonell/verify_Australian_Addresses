# This script prepares some G-NAF files.
# In some cases is creates a file suitable for verifyAddress.py
# In other cases the data needs to be joined and/or modified using Python
# The script ProcessG-NAF.sh does that processing
# This script takes a single argument, being the folder containing the G-NAF data
# that is, the folder containing the 'Authority Code' and 'Standard' folders

if [ $# -ne 2 ]
then
	echo 'G-NAF directory and ABS directory are manadatory argument'
	exit 0
fi
GNAFdir="$1"
ABSdir="$2"


# Create the address_trim.psv file (merging the flat types and level types, and adding in any local definitions)
if test -f "address_flat.psv" && test -f "address_level.psv"
then
	echo "address_flat.psv, address_level.psv already created"
else
	echo "creating address_flat.psv, address_level.psv"
	python3 getAddressTrim.py -G "${GNAFdir}"
fi

# Create the street_type.psv file (adding in any local definitions)
if test -f "street_type.psv"
then
	echo "street_type.psv already created"
else
	echo "creating street_type.psv"
	python3 csvExtract.py -c STREET_TYPE "${GNAFdir}/Authority Code/Authority_Code_STREET_TYPE_AUT_psv.psv" > street_type.psv
fi

# Create the street_suffix.psv file (adding in any local definitions)
if test -f "street_suffix.psv"
then
	echo "street_suffix.psv already created"
else
	echo "creating street_suffix.psv"
	python3 csvExtract.py -c STREET_SUFFIX "${GNAFdir}/Authority Code/Authority_Code_STREET_SUFFIX_AUT_psv.psv" > street_suffix.psv
fi

# Create the street_details.psv file
if test -f "street_details.psv"
then
	echo "street_details.psv already created"
else
	echo "creating street_details.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_STREET_LOCALITY_psv.psv" | python3 csvExtract.py -c STREET_LOCALITY1 > street_details.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_STREET_LOCALITY_psv.psv" | python3 csvExtract.py -s -c STREET_LOCALITY1 >> street_details.psv
	done
fi

# Create the street_details_alias.psv file
if test -f "street_details_alias.psv"
then
	echo "street_details_alias.psv already created"
else
	echo "creating street_details_alias.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_STREET_LOCALITY_ALIAS_psv.psv" | python3 csvExtract.py -c STREET_LOCALITY2 > street_details_alias.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_STREET_LOCALITY_ALIAS_psv.psv" | python3 csvExtract.py -s -c STREET_LOCALITY2 >> street_details_alias.psv
	done
fi

# Create the street_SA1LGA.psv file
if test -f "street_SA1LGA.psv"
then
	echo "street_SA1LGA.psv already created"
else
	echo "creating street_SA1LGA.psv"
	python3 getStreetSA1LGA.py -G "${GNAFdir}"
fi

# Create the locality.psv file (merging the locality and locality aliases)
if test -f "locality.psv"
then
	echo "locality.psv already created"
else
	echo "creating locality.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_LOCALITY_psv.psv" | python3 csvExtract.py -c LOCALITY1 > locality.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_LOCALITY_psv.psv" | python3 csvExtract.py -s -c LOCALITY1 >> locality.psv
	done
	for i in ACT NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_LOCALITY_ALIAS_psv.psv" | python3 csvExtract.py -s -c LOCALITY2 >> locality.psv
	done
fi

# Create the neighbours.psv file
if test -f "neighbours.psv"
then
	echo "neighbours.psv already created"
else
	echo "creating neighbours.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_LOCALITY_NEIGHBOUR_psv.psv" | python3 csvExtract.py -c NEIGHBOUR > neighbours.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_LOCALITY_NEIGHBOUR_psv.psv" | python3 csvExtract.py -s -c NEIGHBOUR >> neighbours.psv
	done
fi

# Create the locality_SA1LGA.psv file
if test -f "locality_SA1LGA.psv"
then
	echo "locality_SA1LGA.psv already created"
else
	echo "creating locality_SA1LGA.psv"
	python3 getLocalitySA1LGA.py -G "${GNAFdir}"
fi

# Create the postcode_SA1LGA.psv file
if test -f "postcode_SA1LGA.psv"
then
	echo "postcode_SA1LGA.psv already created"
else
	python3 getPostcodeSA1LGA.py -A "${ABSdir}"
fi

# Create the file sa1.psv file
if test -f "sa1.csv"
then
	echo "sa1.csv already created"
else
	echo "creating sa1.csv"
	python3 csvExtract.py -c SA1 "${ABSdir}/MB/MB_2016_ACT.csv" > sa1.csv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvExtract.py -s -c SA1 "${ABSdir}/MB/MB_2016_${i}.csv" >> sa1.csv
	done
fi

# Create the file lga.psv file
if test -f "lga.csv"
then
	echo "lga.csv already created"
else
	echo "creating lga.csv"
	python3 csvExtract.py -c LGA "${ABSdir}/LGA/LGA_2020_ACT.csv" > lga.csv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvExtract.py -s -c LGA "${ABSdir}/LGA/LGA_2020_${i}.csv" >> lga.csv
	done
fi

# Create the state.psv file
if test -f "state.psv"
then
	echo "state.psv already created"
else
	echo "creating state.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_STATE_psv.psv" | python3 csvExtract.py -c STATE > state.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_STATE_psv.psv" | python3 csvExtract.py -s -c STATE >> state.psv
	done
fi

# Create the file addressMB.psv file
if test -f "addressMB.psv"
then
	echo "addressMB.psv already created"
else
	echo "creating addressMB.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_ADDRESS_MESH_BLOCK_2016_psv.psv" | python3 csvExtract.py -c ADDRESS_MB > addressMB.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_ADDRESS_MESH_BLOCK_2016_psv.psv" | python3 csvExtract.py -s -c ADDRESS_MB >> addressMB.psv
	done
fi

# Create the file MB.psv file
if test -f "MB.psv"
then
	echo "MB.psv already created"
else
	echo "creating MB.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_MB_2016_psv.psv" | python3 csvExtract.py -c MB > MB.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_MB_2016_psv.psv" | python3 csvExtract.py -s -c MB >> MB.psv
	done
fi

# Create the file address_detail.psv file
if test -f "address_detail.psv"
then
	echo "address_detail.psv already created"
else
	echo "creating address_detail.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_ADDRESS_DETAIL_psv.psv" | python3 csvExtract.py -c ADDRESS_DETAILS > address_detail.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_ADDRESS_DETAIL_psv.psv" | python3 csvExtract.py -s -c ADDRESS_DETAILS >> address_detail.psv
	done
fi

# Create the address_default_geocode.psv file
if test -f "address_default_geocode.psv"
then
	echo "address_default_geocode.psv already created"
else
	echo "creating address_default_geocode.psv"
	python3 csvFind.py -c notRetired "${GNAFdir}/Standard/ACT_ADDRESS_DEFAULT_GEOCODE_psv.psv" | python3 csvExtract.py -c ADDRESS_DEFAULT_GEOCODE > address_default_geocode.psv
	for i in NSW NT OT QLD SA TAS VIC WA
	do
		python3 csvFind.py -c notRetired "${GNAFdir}/Standard/${i}_ADDRESS_DEFAULT_GEOCODE_psv.psv" | python3 csvExtract.py -s -c ADDRESS_DEFAULT_GEOCODE >> address_default_geocode.psv
	done
fi


# Create the community_SA1LGA.psv file
if test -f "community_SA1LGA.psv"
then
	echo "community_SA1LGA.psv already created"
else
	echo "creating community_SA1LGA.psv"
	python3 getCommunity.py -A "${ABSdir}"
fi
