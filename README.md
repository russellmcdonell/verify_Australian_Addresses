# verify Australian Addresses
**A Python script to check free text/partially structured addresses to see if they are possible or plausable or actual Australian addresses**

# Required additional files
This project depends upon some large data sets that you will need to download. They are not part of this repository because
* They are too big and github won't allow it
* You should get them from their original source

G-NAF - Australia's Geocoded National Address File - from [Data.gov.au](https://data.gov.au/home) - just search for G-NAF. Select the GDA2020 version.

Some data from the [Australian Bureau of Statistics - Statistical Geograpy](https://www.abs.gov.au/statistics/statistical-geography) - select the Australian Statistical Geograpy Standards (ASGS), then ABS geograpy products.
 * You will need MESH BLOCK (MB) data and Statitical Area Level 1 (SA1) data. From ABS geography products select 'Main Structures and Greater Capital City Statistical Areas' then Downloads. You will need to download the nine Mesh Blocks '.csv Format' files [one for each state/territory and one for Other Territories], plus the Statistical Area Level 1 (SG1) '.csv Format' file. However, you will also need to download the Statistical Area Level 1 'ESRI Shapefile Format' ZIP file.
 * You will need the LGA data. From ABS geography products select 'Non ABS Structures' then Downloads. You will need to download the nine Local Government Aras '.csv Format' files [one for each state/territory and one for Other Territories].  However, you will also need to download the Local Government Areas 'ESRI Shapefile Format' ZIP file.
 
 The shapefiles are used to "approximately" geocode [assign an SA1 and LGA code] to addresses that have no house number; just a street, or no street; just a suburb. There are scripts in the tools folder for processing these shape files, but the processing it time consuming [several days/hours].

 You will also need a list of postcodes with longitude/latitude if you want to "approximate" SA1/LGA to addresses that only have a postcode. See Postcodes/README.md for sources of such lists. Again, there's a script to assign SA1 and LGA from the polygons. You may need to edit it, depending upon where you source the list from. Again, processing is time consuming.

 ## Tools
 The tools directory contains scripts to pre-process the data files into smaller, more compact data files. There's also scripts for loading G-NAF into a database. However, **verifyAddress.py** pre-loads all the data before verifying any addresses and can pre-load from either a full G-NAF database or the compact file of the full G-NAF PSV files.

 ## Service Delivery Addresses
 G-NAF is not Australia Post, so it doesn't know about things like registered mail bags and 'care of the postoffice' (CARE P.O. etc). **verifyAddress.p** handles these types of addresses. It uses a user editable file (serviceDelivery.psv) which holds text strings and cardinality. A cardinality of '0' means the service delivery text is never followed by a number. A cardinality of '*' means the service delivery text may be followed by a number. A cardinality of '1' means that the service delivery text is always followed by a number.

 ## Postcodes and Localities
 G-NAF uses standards for street type, localities etc. People don't always stick to the standards, so you will find non-standard things in addresses, such as different abbreviations for street type, other names for suburbs. To cater for this, **verifyAddress.py** reads, some extra files which users can configure.
   * extraFlats.psv and extraLevels.psv - for other names for flats/units etc [things that may be followed by a number that isn't the street number]
   * extraTrims.psv - other text that may exist before the house number, such as unregistered building names.
   * extraStreetTypes.psv and extraStreetSuffixes.psv - for non-standard street types/street suffixes and/or their non-standard abbreviations.
   * extraPostcodeSA1LGA.psv - other names for suburbs/localities associated with a postcode.
   * extraLocalities.psv - additional postcodes for G-NAF localities

   






<br><br>
  ** G-NAF © [Geoscape Australia](https://geoscape.com.au/legal/data-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under the [Open Geo-coded National Address File (G-NAF) End User Licence Agreement](https://data.gov.au/dataset/ds-dga-19432f89-dc3a-4ef3-b943-5326ef1dbecc/distribution/dist-dga-09f74802-08b1-4214-a6ea-3591b2753d30/details?q=gnaf).

  
<br/><br/>
 ** Incorporates or developed using G-NAF © [Geoscape Australia](https://geoscape.com.au/legal/data-copyright-and-disclaimer/) licensed by the Commonwealth of Australia under the [Open Geo-coded National Address File (G-NAF) End User Licence Agreement](https://data.gov.au/dataset/ds-dga-19432f89-dc3a-4ef3-b943-5326ef1dbecc/distribution/dist-dga-09f74802-08b1-4214-a6ea-3591b2753d30/details?q=gnaf).