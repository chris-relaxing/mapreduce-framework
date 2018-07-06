#-------------------------------------------------------------------------------
# Name:         PlacesValidations.py

# Purpose:      This is a part of the Python MapReduce framework I developed to run validations and data mining
#               on Places xml data on PlacesLab HDFS. This script needs to reside on PlacesLab next to the Mapper and Reducer.
#               Takes in an input file called 'product_vals.xml', which should also be uploaded to PlacesLab.
#
# Author:       Chris Nielsen (chris.relaxing@gmail.com)
#-------------------------------------------------------------------------------

import os
import sys
import lxml.etree as etree
import re
from math import pi , acos , sin , cos

xml_file = 'product_vals.xml'

# Note: All validations below have been written to work with the Places xml (EWP) schema.
#
#

# Media_0002 ------------------------------------------------------
def Media_0002(Place, map_input_file):
    """
    # For every Place Location, see if the CountryCode matches the name of the xml file. For example,
    # CountryCode="CHL" doesn't match file name "COL.xml". Also gather the PlaceId and primary=TRUE or FALSE.
    # Output:           Media_0002|17066jcc-4c61f028e5fd4539bfc2ba9b0702d96c|false|COL.xml|CHL	1
    # Report header:    Media_0002|PlaceId|Primary|File Name|CountryCode   Count
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/CountryCode
    """
    LocationList = Place.findall(ns+"Location")
    try:
        PlaceId = Place.find(ns+"PlaceId").text
    except:
        PlaceId = 'None'
    if map_input_file != "unknown":
        file_piece = map_input_file[-7:]
        xfn = file_piece[:3]
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        CountryCode = Location.find(ns+"CountryCode").text
        try:
            Location_primary_value = LocationAttributes['primary']  # LocationAttributes['primary']
        except:
            Location_primary_value = 'None'
        if xfn != CountryCode:
            emit_string = 'Media_0002|'+CountryCode+'|'+PlaceId+'|primary='+Location_primary_value+'|'+file_piece
            return_emits.append(emit_string)
    return return_emits


# BASIC_0001 ------------------------------------------------------
def Basic_0001(PlaceList, map_input_file):
    """
    # Check the xmlns for the PlaceList (first element in the document only)
    # xpath: PlaceList/@xmlns
    """
    correct = '<PlaceList xmlns="http://places.maps.domain.com/pds">'
    if map_input_file != "unknown":
        file_piece = map_input_file[-7:]
    if PlaceList != correct:
        emit_string = 'Basic_0001|'+file_piece+'|PlaceList xmlns is incorrect.|'+PlaceList
        return emit_string


# BASIC_0002 ------------------------------------------------------
def Basic_0002(Place, map_input_file):
    """
    # Check the xmlns for each Place
    # xpath: PlaceList/Place/@xmlns
    # 1. xmlns is not correct (a) - http://places.maps.domain.com/pd
    # 2. xmlns is null (b)  - xmlns=""
    # 3. No xmlns defined (c) - <Place>
    # 4. Extra attribution found (d) - {'something': 'else'}
    """
    if map_input_file != "unknown":
        file_piece = map_input_file[-7:]
    correct_xmlns = "http://places.maps.domain.com/pds"
    pns = Place.nsmap
    if len(pns) == 1:                           # Normal case
        current_xmlns = pns.values()[0]
    elif len(pns) < 1:                          # Case of Null or missing xmlns key/value pair
        emit_string = 'Basic_0002c|'+file_piece+'|This Place does not have an xmlns defined.|<Place>'
        return emit_string
    if len(pns) == 1 and Place.attrib:          # Check for extra attributes
        emit_string = 'Basic_0002d|'+file_piece+'|This Place contains extra attributes|'+str(Place.attrib)
        return emit_string
    if current_xmlns != correct_xmlns:
        if current_xmlns == "":                 # Case of Null or missing xmlns value
            emit_string = 'Basic_0002b|'+file_piece+'|Place xmlns is Null.|xmlns=""'
        else:                                   # Case of incorrect xmlns
            emit_string = 'Basic_0002a|'+file_piece+'|Place xmlns is incorrect.|'+current_xmlns
        return emit_string


# BASIC_0003 ------------------------------------------------------
def Basic_0003(Place):
    """
    # Validates that there are no null values, no zero values, and that there are only values between -90 and 90 for the 'Latitude' element.
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Latitude
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    GeoPositionList = Place.findall(ns+"GeoPosition")
    return_emits = []
    for GeoPosition in GeoPositionList:
        Latitude = GeoPosition.find(ns+"Latitude")
        try:
            LAT = str(Latitude.text)
            if not LAT or LAT == "None":
                emit_string = 'Basic_0003a|'+CountryCode+'|'+PlaceId+'|Latitude is 0 or Null|'+LAT
                return_emits.append(emit_string)
            else:
                if "E" in LAT:
                    emit_string = 'Basic_0003c|'+CountryCode+'|'+PlaceId+'|Latitude Invalid format|'+LAT
                    return_emits.append(emit_string)
                if float(LAT) > 90 or float(LAT) < -90:
                    emit_string = 'Basic_0003b|'+CountryCode+'|'+PlaceId+'|Latitude not between -90 and 90|'+LAT
                    return_emits.append(emit_string)
                if float(LAT) == 0:
                    emit_string = 'Basic_0003a|'+CountryCode+'|'+PlaceId+'|Latitude is 0 or Null|'+LAT
                    return_emits.append(emit_string)
        except:
            emit_string = 'Basic_0003d|'+CountryCode+'|'+PlaceId+'|Latitude element is missing|<Latitude>'
            return_emits.append(emit_string)

    return return_emits


# BASIC_0004 ------------------------------------------------------
def Basic_0004(Place):
    """
    # Validates that there are no null values, no zero values, and that there are only values between -90 and 90 for the 'Longitude' element.
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Longitude
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Longitude
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    GeoPositionList = Place.findall(ns+"GeoPosition")
    return_emits = []
    for GeoPosition in GeoPositionList:
        Longitude = GeoPosition.find(ns+"Longitude")
        try:
            Long = str(Longitude.text)
            if not Long or Long == "None":
                emit_string = 'Basic_0004a|'+CountryCode+'|'+PlaceId+'|Longitude is 0 or Null|'+Long
                return_emits.append(emit_string)
            else:
                if "E" in Long:
                    emit_string = 'Basic_0004c|'+CountryCode+'|'+PlaceId+'|Longitude Invalid format|'+Long
                    return_emits.append(emit_string)
                if float(Long) > 180 or float(Long) < -180:
                    emit_string = 'Basic_0004b|'+CountryCode+'|'+PlaceId+'|Longitude not between -180 and 180|'+Long
                    return_emits.append(emit_string)
                if float(Long) == 0:
                    emit_string = 'Basic_0004a|'+CountryCode+'|'+PlaceId+'|Longitude is 0 or Null|'+Long
                    return_emits.append(emit_string)
        except:
            emit_string = 'Basic_0004d|'+CountryCode+'|'+PlaceId+'|Longitude element is missing|<Longitude>'
            return_emits.append(emit_string)
    return return_emits


# BASIC_0005 ------------------------------------------------------
def Basic_0005(Place):
    """
    # Validates that there are no instances where the same values are published for both the 'Latitude' and 'Longitude' elements for a particular Place record.
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude and
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Longitude
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    GeoPositionList = Place.findall(ns+"GeoPosition")
    return_emits = []
    for GeoPosition in GeoPositionList:
        try:
            Longitude = GeoPosition.find(ns+"Longitude")
            Latitude = GeoPosition.find(ns+"Latitude")
            Long = str(Longitude.text)
            LAT = str(Latitude.text)
            if float(Long) == float(LAT):
                emit_string = 'Basic_0005|'+CountryCode+'|'+PlaceId+'|Longitude and Latitude values are identical.|'+LAT+' '+Long
                return_emits.append(emit_string)
            elif "E" in Long and Long == LAT:
                emit_string = 'Basic_0005|'+CountryCode+'|'+PlaceId+'|Longitude and Latitude values are identical.|'+LAT+' '+Long
                return_emits.append(emit_string)
        except:
            pass
    return return_emits


# BASIC_0006a ------------------------------------------------------
def Basic_0006a(Place):
    """
    # Validates that there are no instances where the same 'Latitude' and 'Longitude' values are published for both the 'Display' and 'Routing'.
    # This validation should exclude Locations where no 'Side' attribute is included or the 'Side' attribute has a value of 'neither'.
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude and
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Longitude
    # 0006a is less noisy than 0006b because this one gives results in counts per CountryCode.
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    # This check is only relevant if there is both a type="DISPLAY" AND type="ROUTING" in the same GeoPositionList element
    # According to Spec, "Side" should be either L, R, or N.. not the full words left, right, neither
    # For each Location in LocationList, check the side (assumes there is only 1 Side per Location)
    for Location in LocationList:
        try:
            Side = Location.find(ns+"Side").text
        except:
            Side = "None"
        GeoPositionList = Location.findall(ns+"GeoPosition")

        if len(GeoPositionList) == 2:       # Only consider when there is more than one GeoPosition
            for GeoPosition in GeoPositionList:
                try:
                    GeoPosition_type = GeoPosition.attrib
                    if GeoPosition_type["type"] == 'DISPLAY':
                        display_LAT = GeoPosition.find(ns+"Latitude")
                        display_LONG = GeoPosition.find(ns+"Longitude")
                    if GeoPosition_type["type"] == 'ROUTING':
                        routing_LAT = GeoPosition.find(ns+"Latitude")
                        routing_LONG = GeoPosition.find(ns+"Longitude")
                    if Side == "left" or Side == "right":           # Exluding Side="None" and Side="neither"
                        if display_LAT.text == routing_LAT.text and display_LONG.text == routing_LONG.text:
                            emit_string = 'Basic_0006a|'+CountryCode+'|DISPLAY and ROUTING Lat/Longs are identical'
                            return_emits.append(emit_string)
                except:
                    pass
    return return_emits


# BASIC_0006b ------------------------------------------------------
def Basic_0006b(Place):
    """
    # Validates that there are no instances where the same 'Latitude' and 'Longitude' values are published for both the 'Display' and 'Routing'.
    # This validation should exclude Locations where no 'Side' attribute is included or the 'Side' attribute has a value of 'neither'.
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude and
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Longitude
    # This validation is noisy. It prodicted over 200K results, so it should only be used when we want to see the PlaceIds and Side info for each.
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    # This check is only relevant if there is both a type="DISPLAY" AND type="ROUTING" in the same GeoPositionList element
    # According to Spec, "Side" should be either L, R, or N.. not the full words left, right, neither
    # For each Location in LocationList, check the side (assumes there is only 1 Side per Location)
    for Location in LocationList:
        try:
            Side = Location.find(ns+"Side").text
        except:
            Side = "None"
        GeoPositionList = Location.findall(ns+"GeoPosition")

        if len(GeoPositionList) == 2:       # Only consider when there is more than one GeoPosition
            for GeoPosition in GeoPositionList:
                try:
                    GeoPosition_type = GeoPosition.attrib
                    if GeoPosition_type["type"] == 'DISPLAY':
                        display_LAT = GeoPosition.find(ns+"Latitude")
                        display_LONG = GeoPosition.find(ns+"Longitude")
                    if GeoPosition_type["type"] == 'ROUTING':
                        routing_LAT = GeoPosition.find(ns+"Latitude")
                        routing_LONG = GeoPosition.find(ns+"Longitude")
                    if Side == "left" or Side == "right":           # Exluding Side="None" and Side="neither"
                        if display_LAT.text == routing_LAT.text and display_LONG.text == routing_LONG.text:
                            emit_string = 'Basic_0006b|'+CountryCode+'|'+PlaceId+'|DISPLAY and ROUTING Lat/Longs are identical|Side='+Side
                            return_emits.append(emit_string)
                            # print PlaceId, "\t", Side, "\t", len(LocationList)
                            # print display_LAT.text
                            # print display_LONG.text
                            # print routing_LAT.text
                            # print routing_LONG.text
                except:
                    pass
    return return_emits

# Basic_0008a ------------------------------------------------------
def Basic_0008a(Place):
    """
    # Check that the location has attributes
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if not LocationAttributes:
            emit_string = 'Basic_0008a|'+PlaceId+'|This location does not have attributes.|'+CountryCode
            return_emits.append(emit_string)
    return return_emits


# Basic_0008b ------------------------------------------------------
def Basic_0008b(Place):
    """
    # Check that the location attributes contains a 'primary' key
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'primary' not in LocationAttributes.keys():
            emit_string = 'Basic_0008b|'+PlaceId+'|This location is missing the "primary" key.|'+CountryCode
            return_emits.append(emit_string)
    return return_emits

# Basic_0008c ------------------------------------------------------
def Basic_0008c(Place):
    """
    # Check that the location attributes contains a 'primary' value
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'primary' in LocationAttributes.keys():
            if not LocationAttributes['primary']:
                emit_string = 'Basic_0008c|'+PlaceId+'|The "primary" key is missing a value.|'+CountryCode
                return_emits.append(emit_string)
    return return_emits


# Basic_0008d ------------------------------------------------------
def Basic_0008d(Place):
    """
    # Check that the location attributes contains a 'type' key
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'type' not in LocationAttributes.keys():
            emit_string = 'Basic_0008d|'+PlaceId+'|This location is missing the "type" key.|'+CountryCode
            return_emits.append(emit_string)
    return return_emits


# Basic_0008e ------------------------------------------------------
def Basic_0008e(Place):
    """
    # Check that the location attributes contains a 'type' value
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'type' in LocationAttributes.keys():
            if not LocationAttributes['type']:
                emit_string = 'Basic_0008e|'+PlaceId+'|The "type" key is missing a value.|'+CountryCode
                return_emits.append(emit_string)
    return return_emits

# Basic_0008f ------------------------------------------------------
def Basic_0008f(Place):
    """
    # Check for the combination of primary=true and type!=MAIN
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'primary' in LocationAttributes.keys() and 'type' in LocationAttributes.keys():
            if LocationAttributes['primary'] == 'true' and LocationAttributes['type'] != 'MAIN':
                emit_string = 'Basic_0008f|'+PlaceId+'|primary="true" and type != "MAIN".|'+CountryCode
                return_emits.append(emit_string)
    return return_emits

# Basic_0008g ------------------------------------------------------
def Basic_0008g(Place):
    """
    # Check that there is always one and only one primary == true per Place
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    primaryValueList = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'primary' in LocationAttributes.keys():
            primaryValueList.append(LocationAttributes['primary'])
    truecount = primaryValueList.count('true')
    if truecount > 1:
        emit_string = 'Basic_0008g|'+PlaceId+'|This Place contains more than one primary="true".|'+CountryCode
        return emit_string


# Basic_0008h ------------------------------------------------------
def Basic_0008h(Place):
    """
    # Check for the case of less than one primary == true
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    LocationList = Place.findall(ns+"Location")
    primaryValueList = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'primary' in LocationAttributes.keys():
            primaryValueList.append(LocationAttributes['primary'])
    truecount = primaryValueList.count('true')
    if truecount < 1:
        emit_string = 'Basic_0008h|'+PlaceId+'|There should always be at least one primary="true" per Place.|'+CountryCode
        return emit_string


# BASIC_0015 ------------------------------------------------------
def Basic_0015(Place):
    """
    # Validates that only the value 'false' is present within the 'isDeleted' element of the 'Identity' element and that there are no null values present.
    # xpath: PlaceList/Place/Identity[@isDeleted]
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    try:
        Identity = Place.find(ns+"Identity")
        Identity_attrib = Identity.attrib
        if 'isDeleted' not in Identity_attrib.keys():
            emit_string = 'Basic_0015|'+CountryCode+'|'+str(PlaceId)+'|Identity isDeleted attribute missing|'
            return emit_string
        if Identity_attrib['isDeleted'] == "" or not Identity_attrib['isDeleted']:
            emit_string = 'Basic_0015|'+CountryCode+'|'+str(PlaceId)+'|Identity isDeleted value is Null or missing.|isDeleted=""'
            return emit_string
        elif Identity_attrib['isDeleted'] == "true":
            emit_string = 'Basic_0015|'+CountryCode+'|'+str(PlaceId)+'|Identity isDeleted has invalid value "true"|isDeleted="true"'
            return emit_string
        elif Identity_attrib['isDeleted'] != "false":
            emit_string = 'Basic_0015|'+CountryCode+'|'+str(PlaceId)+'|Identity isDeleted has invalid value|isDeleted="'+Identity_attrib['isDeleted']+'"'
            return emit_string
    except:
        # Note: If the xmlns is broken or missing (Basic_0002), then this validation won't work.
        pass


# Basic_0017a ------------------------------------------------------
def Basic_0017a(Place):
    """
    # PlaceId checks: Emit all PlaceIds in order to check for global duplicates
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    emit_string = 'Basic_0017a|'+str(PlaceId)
    return emit_string


# Basic_0017b ------------------------------------------------------
def Basic_0017b(Place):
    """
    # PlaceId checks: Check for PlaceIds shorter than 41 chars
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    if len(str(PlaceId)) < 41:
        emit_string = 'Basic_0017b|'+str(PlaceId)+'|PlaceId is shorter than 41 chars.|'+CountryCode
        return emit_string


# Basic_0017c ------------------------------------------------------
def Basic_0017c(Place):
    """
    # PlaceId checks: Check for PlaceIds longer than 64 chars
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    if len(str(PlaceId)) > 64:
        emit_string = 'Basic_0017c|'+str(PlaceId)+'|PlaceId is longer than 64 chars.|'+CountryCode
        return emit_string


# Basic_0017d ------------------------------------------------------
def Basic_0017d(Place):
    """
    # PlaceId checks: Check for case of missing PlaceId
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    if PlaceId == "None":
        emit_string = 'Basic_0017d|No PlaceId found.|'+CountryCode
        return emit_string


# BASIC_0020 ------------------------------------------------------
def Basic_0020(Place):
    """
    # Validates that there are no excess spaces (2 or more) within the 'BaseText' element within the 'Text' element
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext = btext.rstrip("\n")
            if "  " in btext:       # Just looks for two consecutive blank spaces
                print_btext = btext.replace("  ", "__")
                print_btext = print_btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                emit_string = 'Basic_0020|'+CountryCode+'|'+PlaceId+'|Multiple consecutive spaces found in BaseText|'+print_btext
                return_emits.append(emit_string)
    except:
        pass
    return return_emits


# BASIC_0022 ------------------------------------------------------
def Basic_0022(Place):
    """
    # Validates for the presence of consecutive (2 or more) punctuations, such as: -/\()!"+,'&.
    # Also checks to make sure that these are not the only characters in the BaseText.
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText
    # Check for these punctuations = /\()!"+,'&.
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    consecutive_punctuation = re.compile('(([-/\\\\()!"+,&\'.])\\2+)')      # checks for consecutive punctuations
    punctuation = re.compile('^[-\/\\\(\)\!\"\+\,\&\'\.]*$')                # checks to see if the string contains only these punctuations
    BaseTextList = Place.findall(ns+"BaseText")
    for BaseText in BaseTextList:
        btext = BaseText.text
        btext = btext.encode('UTF-8')
        consec = consecutive_punctuation.search(btext)
        match = punctuation.search(btext)
        btext = btext.rstrip("\n")
        if consec or match:
            btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
            if consec:
                result = consec.group(1)
                emit_string = 'Basic_0022|'+CountryCode+'|'+PlaceId+'|Multiple consecutive punctuation found in BaseText|'+btext+'|'+result
            else:
                result = match.group(0)
                emit_string = 'Basic_0022a|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in BaseText|'+btext+'|'+result
            return_emits.append(emit_string)
    return return_emits


# BASIC_0029 ------------------------------------------------------
def Basic_0029(Place):
    """
    # Validates that Cyrillic Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the values 'bg', 'be', 'mk', 'ru', 'ro', 'sr', 'uk', 'bs', or 'kk' (See excel spreadsheet entitled 'Global_POI_XML_Cyrillic_TextCharacter_Reference'
    # (Content Server ID=232009) for a listing of Cyrillic characters)
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Cyrillic unicode range: \u0400-\u0482, \u0488-\u04FF
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext = btext.rstrip("\n")
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "bg" and btext_lc != "be" and btext_lc != "mk" and btext_lc != "ru" and btext_lc != "ro" \
                and btext_lc != "sr" and btext_lc != "uk" and btext_lc != "bs" and btext_lc != "kk":
                for char in btext:
                    if re.match(ur'[\u0400-\u0482]+|[\u0488-\u04FF]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0029|'+CountryCode+'|'+PlaceId+'|Invalid: Cyrillic characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0030 ------------------------------------------------------
def Basic_0030(Place):
    """
    # Validates that Greek Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'el' (See excel spreadsheet entitled 'Global_POI_XML_Greek_Text_Character_Reference'
    # (Content Server ID=232010) for a listing of Greek characters)
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Greek unicode range: \u0386-\u03F6
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext = btext.rstrip("\n")
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "el":
                for char in btext:
                    if re.match(ur'[\u0386-\u03F6]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0030|'+CountryCode+'|'+PlaceId+'|Invalid: Greek characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits




# BASIC_0031 ------------------------------------------------------
def Basic_0031(Place):
    """
    # Validates that Hebrew Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'he' (See excel spreadsheet entitled 'Global_POI_XML_Hebrew_TextCharacter_Reference'
    # (Content Server ID=232011) for a listing of Hebrew characters)
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Hebrew unicode range: \u05D0-\u05EA
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "he":
                for char in btext:
                    if re.match(ur'[\u05D0-\u05EA]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0031|'+CountryCode+'|'+PlaceId+'|Invalid: Hebrew characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0032 ------------------------------------------------------
def Basic_0032(Place):
    """
    # Validates that Arabic Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'ar' or 'ur' (See excel spreadsheet entitled 'Global_POI_XML_Arabic_Text_Character_Reference'
    # (Content Server ID=232012) for a listing of Arabic characters)'
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Arabic unicode ranges: \u0621-\u063A|\u0640-\u064A|\u066E-\u066F|\u0671-\u06D3|\u06EE-\u06EF|\u06FA-\u06FF
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "ar" and btext_lc != "ur":
                for char in btext:
                    if re.match(ur'[\u0621-\u063A]+|[\u0640-\u064A]+|[\u066E-\u066F]+|[\u0671-\u06D3]+|[\u06EE-\u06EF]+|[\u06FA-\u06FF]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0032|'+CountryCode+'|'+PlaceId+'|Invalid: Arabic characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0033 ------------------------------------------------------
def Basic_0033(Place):
    """
    # Validates that Thai Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'th' (See excel spreadsheet entitled 'Global_POI_XML_Thai_TextCharacter_Reference'
    # (Content Server ID=232311) for a listing of Thai characters)'
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Thai unicode range: \u0E01-\u0E5B
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "th":
                for char in btext:
                    if re.match(ur'[\u0E01-\u0E5B]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0033|'+CountryCode+'|'+PlaceId+'|Invalid: Thai characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0034 ------------------------------------------------------
def Basic_0034(Place):
    """
    # Validates that CJK Ideograph characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'zh', 'ja',  'ko', or 'zh-Hant'. (See excel spreadsheet entitled 'Global_POI_XML_CJKIdeograph_Text_Char_Ref'
    # (Content Server ID= 232013) for a listing of CJK characters'
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Chinese unicode range: \u4e00-\u9fa5
    # Japanese unicode ranges: \u3041-\u309f, \u30a0-\u30ff
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "zh" and btext_lc != "ja" and btext_lc != "ko" and btext_lc != "zh-Hant":
                for char in btext:
##                    if re.match(ur'[\u4e00-\u9fa5]+|[\u3041-\u309f]+|[\u30a0-\u30ff]+',char):
                    if re.match(ur'[\u4e00-\u9fa5]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0034|'+CountryCode+'|'+PlaceId+'|Invalid: CJK characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0035 ------------------------------------------------------
def Basic_0035(Place):
    """
    # Validates that Korean Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'ko'. (See excel spreadsheet entitled 'Global_POI_XML_Korean_Hangul_Text_Character_Reference'
    # (Content Server ID= 413872) for a list of Korean Unicode characters)'
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Korean unicode ranges: \u1100-\u11FF, \u3130-\u318F, \uA960-\uA97F, \uAC00-\uD7FF
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "ko":
                for char in btext:
                    if re.match(ur'[\u1100-\u11FF]+|[\u3130-\u318F]+|[\uA960-\uA97F]+|[\uAC00-\uD7FF]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0035|'+CountryCode+'|'+PlaceId+'|Invalid: Korean characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0036 ------------------------------------------------------
def Basic_0036(Place):
    """
    # Validates that Hindi Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'ko'. (See excel spreadsheet entitled 'Global_POI_XML_Hindi_TextCharacter_Reference'
    # (Content Server ID=390702) for a list of Hindi Unicode characters)'
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Hindi unicode range: \u0900-\u097F
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "hi":
                for char in btext:
                    if re.match(ur'[\u0900-\u097F]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0036|'+CountryCode+'|'+PlaceId+'|Invalid: Hindi characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits

# BASIC_0037 ------------------------------------------------------
def Basic_0037(Place):
    """
    # Validates that Georgian Unicode characters are only published within the 'BaseText' element when the associated 'Language_Code'
    # attribute within the 'Text' element has the value 'ka'. (See excel spreadsheet entitled 'Global_POI_XML_Georgian_Text_Character_Reference'
    # (Content Server ID= 407821) for a list of Georgian Unicode characters)'
    # xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText/@language_Code
    # Georgian unicode range: \u10A0-\u10FF
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            try:  # Get the languageCode
                btext_lc = btext_attrib['languageCode']
            except:
                btext_lc = 'None'
            if btext_lc != "ka":
                for char in btext:
                    if re.match(ur'[\u10A0-\u10FF]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0037|'+CountryCode+'|'+PlaceId+'|Invalid: Georgian characters found in BaseText|'+btext+'|languageCode="'+btext_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0040 ------------------------------------------------------
def Basic_0040(Place):
    """
    # Validates for the presence of consecutive (2 or more) punctuations, such as: -/\()!"+,'&.
    # Also checks to make sure that these are not the only characters in the BaseText.
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/StreetName/BaseName
    # Check for these punctuations = /\()!"+,'&.
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    consecutive_punctuation = re.compile('(([-/\\\\()!"+,&\'.])\\2+)')      # checks for consecutive punctuations
    punctuation = re.compile('^[-\/\\\(\)\!\"\+\,\&\'\.]*$')                # checks to see if the string contains only these punctuations
    BaseNameList = Place.findall(ns+"BaseName")
    for BaseName in BaseNameList:
        btext = BaseName.text
        btext = btext.encode('UTF-8')
        consec = consecutive_punctuation.search(btext)
        match = punctuation.search(btext)
        btext = btext.rstrip("\n")
        if consec or match:
            btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
            if consec:
                result = consec.group(1)
                emit_string = 'Basic_0040|'+CountryCode+'|'+PlaceId+'|Multiple consecutive punctuation found in StreetName/BaseName|'+btext+'|'+result
            else:
                result = match.group(0)
                emit_string = 'Basic_0040a|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in StreetName/BaseName|'+btext+'|'+result
            return_emits.append(emit_string)
    return return_emits


# BASIC_0041 ------------------------------------------------------
def Basic_0041(Place):
    """
    # Validates for the presence of punctuation within the 'BaseName' element within the 'StreetName' element
    # Also checks to make sure that these are not the only characters in the BaseText.
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/StreetName/BaseName
    # Check for all punctuations found in Appendix F of 'Global Places XML Validation Rule Code Listing.docx'
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        BaseNameList = Place.findall(ns+"BaseName")
        for BaseName in BaseNameList:
            btext = BaseName.text
            for char in btext:
                if re.match(ur'[\u0023-\u0025]+|[\u002A]+|[\u00A1-\u00A7]+|[\u00A9-\u00AE]+|[\u003A-\u003F]+|[\u007B-\u007E]+|[\u0040]+|[\u005B]+|[\u005D-\u005F]+|[\u00B0-\u00B3]+|[\u00B5-\u00B7]+|[\u00B9-\u00BF]+|[\u0060]+|[\u00D7]+',char):
                    btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                    btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                    emit_string = 'Basic_0041|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in StreetName/BaseName|'+btext
                    return_emits.append(emit_string)
                    break
    except:
        pass
    return return_emits


# BASIC_0045 ------------------------------------------------------
def Basic_0045(Place):
    """
    # Validates that Cyrillic Unicode characters are only published within the within the 'BaseName' element when the associated 'Language_Code'
    # attribute within the 'StreetName' element has the values 'bg', 'be', 'mk', 'ru', 'ro', 'sr', 'uk', 'bs', or 'kk' (See excel spreadsheet entitled 'Global_POI_XML_Cyrillic_TextCharacter_Reference'
    # (Content Server ID=232009) for a listing of Cyrillic characters)
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/StreetName/BaseName
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/@languageCode
    # Cyrillic unicode range: \u0400-\u0482, \u0488-\u04FF
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        ParsedList = Place.findall(ns+"Parsed")
        for Parsed in ParsedList:
            Parsed_attrib = Parsed.attrib
            try:    # Get the languageCode
                Parsed_lc = Parsed_attrib['languageCode']
            except:
                Parsed_lc = 'None'
            try:
                BaseName = Parsed.find(ns+"BaseName")
                btext = BaseName.text
                btext = btext.rstrip("\n")
            except:
                BaseName = ""
            if BaseName != "" and Parsed_lc != "bg" and Parsed_lc != "be" and Parsed_lc != "mk" and Parsed_lc != "ru" and Parsed_lc != "ro" \
                and Parsed_lc != "sr" and Parsed_lc != "uk" and Parsed_lc != "bs" and Parsed_lc != "kk" and Parsed_lc != "None":
                for char in btext:
                    if re.match(ur'[\u0400-\u0482]+|[\u0488-\u04FF]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0045|'+CountryCode+'|'+PlaceId+'|Invalid: Cyrillic characters found in StreetName/BaseName|'+btext+'|languageCode="'+Parsed_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0046 ------------------------------------------------------
def Basic_0046(Place):
    """
    # Validates that Greek Unicode characters are only published within the within the 'BaseName' element when the associated 'Language_Code'
    # attribute within the 'StreetName' element has the values 'el' (See excel spreadsheet entitled 'Global_POI_XML_Greek_Text_Character_Reference'
    # (Content Server ID=232010) for a listing of Greek characters)
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/StreetName/BaseName
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/@languageCode
    # Greek unicode range: \u0386-\u03F6
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        ParsedList = Place.findall(ns+"Parsed")
        for Parsed in ParsedList:
            Parsed_attrib = Parsed.attrib
            try:    # Get the languageCode
                Parsed_lc = Parsed_attrib['languageCode']
            except:
                Parsed_lc = 'None'
            try:
                BaseName = Parsed.find(ns+"BaseName")
                btext = BaseName.text
                btext = btext.rstrip("\n")
            except:
                BaseName = ""
            if BaseName != "" and Parsed_lc != "el" and Parsed_lc != "None":
                for char in btext:
                    if re.match(ur'[\u0386-\u03F6]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0046|'+CountryCode+'|'+PlaceId+'|Invalid: Greek characters found in StreetName/BaseName|'+btext+'|languageCode="'+Parsed_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0048 ------------------------------------------------------
def Basic_0048(Place):
    """
    # Validates that Arabic Unicode characters are only published within the within the 'BaseName' element when the associated 'Language_Code'
    # attribute within the 'StreetName' element has the values 'ar' or 'ur' (See excel spreadsheet entitled 'Global_POI_XML_Arabic_Text_Character_Reference'
    # (Content Server ID=232012) for a listing of Arabic characters)
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/StreetName/BaseName
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/@languageCode
    # Arabic unicode ranges: \u0621-\u063A|\u0640-\u064A|\u066E-\u066F|\u0671-\u06D3|\u06EE-\u06EF|\u06FA-\u06FF
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        ParsedList = Place.findall(ns+"Parsed")
        for Parsed in ParsedList:
            Parsed_attrib = Parsed.attrib
            try:    # Get the languageCode
                Parsed_lc = Parsed_attrib['languageCode']
            except:
                Parsed_lc = 'None'
            try:
                BaseName = Parsed.find(ns+"BaseName")
                btext = BaseName.text
                btext = btext.rstrip("\n")
            except:
                BaseName = ""
            if BaseName != "" and Parsed_lc != "ar" and Parsed_lc != "ur" and Parsed_lc != "None":
                for char in btext:
                    if re.match(ur'[\u0621-\u063A]+|[\u0640-\u064A]+|[\u066E-\u066F]+|[\u0671-\u06D3]+|[\u06EE-\u06EF]+|[\u06FA-\u06FF]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0048|'+CountryCode+'|'+PlaceId+'|Invalid: Arabic characters found in StreetName/BaseName|'+btext+'|languageCode="'+Parsed_lc+'"'
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits

# BASIC_0057 ------------------------------------------------------
def Basic_0057(Place):
    """
    # Validates for the presence of punctuation within the 'StreetType' element within the 'StreetName' element
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/StreetName/StreetType
    # Check for all punctuations found in Appendix F of 'Global Places XML Validation Rule Code Listing.docx'
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        ParsedList = Place.findall(ns+"Parsed")
        for Parsed in ParsedList:
            try:
                StreetType = Parsed.find(ns+"StreetType")
                btext = StreetType.text
                btext = btext.rstrip("\n")
            except:
                btext = ""
            for char in btext:
                if re.match(ur'[\u0023-\u0025]+|[\u002A]+|[\u00A1-\u00A7]+|[\u00A9-\u00AE]+|[\u003A-\u003F]+|[\u007B-\u007E]+|[\u0040]+|[\u005B]+|[\u005D-\u005F]+|[\u00B0-\u00B3]+|[\u00B5-\u00B7]+|[\u00B9-\u00BF]+|[\u0060]+|[\u00D7]+',char):
                    btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                    btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                    emit_string = 'Basic_0057|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in StreetName/StreetType|'+btext
                    return_emits.append(emit_string)
                    break
    except:
        pass
    return return_emits


# BASIC_0087 ------------------------------------------------------
def Basic_0087(Place):
    """
    # Validates for the presence of punctuation within the 'Level3' element within the 'AdminLevel' element
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Admin/AdminLevel/Level3
    # Check for all punctuations found in Appendix F of 'Global Places XML Validation Rule Code Listing.docx'
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    try:
        ParsedList = Place.findall(ns+"Parsed")
        for Parsed in ParsedList:
            try:
                Level3 = Parsed.find(ns+"Level3")
                btext = Level3.text
                btext = btext.rstrip("\n")
            except:
                btext = ""
            if btext != "":
                for char in btext:
                    if re.match(ur'[\u0023-\u0025]+|[\u002A]+|[\u00A1-\u00A7]+|[\u00A9-\u00AE]+|[\u003A-\u003F]+|[\u007B-\u007E]+|[\u0040]+|[\u005B]+|[\u005D-\u005F]+|[\u00B0-\u00B3]+|[\u00B5-\u00B7]+|[\u00B9-\u00BF]+|[\u0060]+|[\u00D7]+',char):
                        btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                        btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                        emit_string = 'Basic_0087|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in AdminLevel/Level3|'+btext
                        return_emits.append(emit_string)
                        break
    except:
        pass
    return return_emits


# BASIC_0101 ------------------------------------------------------
def Basic_0101(Place):
    """
    # Validates for the presence of consecutive (2 or more) punctuations within the 'Level3' element within the 'AdminLevel' element
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Admin/AdminLevel/Level4
    # Check for these punctuations = /\()!"+,'&.
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    consecutive_punctuation = re.compile('(([-/\\\\()!"+,&\'.])\\2+)')      # checks for consecutive punctuations
    punctuation = re.compile('^[-\/\\\(\)\!\"\+\,\&\'\.]*$')                # checks to see if the string contains only these punctuations
    ParsedList = Place.findall(ns+"Parsed")
    for Parsed in ParsedList:
        try:
            Level4 = Parsed.find(ns+"Level4")
            btext = Level4.text
            btext = btext.rstrip("\n")
        except:
            btext = ""
        if btext != "":
            btext = btext.encode('UTF-8')
            consec = consecutive_punctuation.search(btext)
            match = punctuation.search(btext)
            if consec or match:
                btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                if consec:
                    result = consec.group(1)
                    emit_string = 'Basic_0101|'+CountryCode+'|'+PlaceId+'|Multiple consecutive punctuation found in AdminLevel/Level4|'+btext+'|'+result
                else:
                    result = match.group(0)
                    emit_string = 'Basic_0101a|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in AdminLevel/Level4|'+btext+'|'+result
                return_emits.append(emit_string)
    return return_emits


# BASIC_0104 ------------------------------------------------------
def Basic_0104(Place):
    """
    # Validates for instances where there are only numeric values present within the 'Level4' element, OR a numeric only value plus one
    # of the below punctuations before or after the integer.
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Admin/AdminLevel/Level4
    # Check for these punctuations = -/\() ,'&.
    pattern = re.compile('^([-/\\()\s,\'&.]*)([0-9]+)([-/\\()\s,\'&.]*)$')
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    pattern = re.compile('^([-/\\()\s,\'&.]*)([0-9]+)([-/\\()\s,\'&.]*)$')
    ParsedList = Place.findall(ns+"Parsed")
    for Parsed in ParsedList:
        try:
            Level4 = Parsed.find(ns+"Level4")
            btext = Level4.text
            btext = btext.rstrip("\n")
        except:
            btext = ""
        if btext != "":
            btext = btext.encode('UTF-8')
            match = pattern.search(btext)
            if match:
                btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                emit_string = 'Basic_0104|'+CountryCode+'|'+PlaceId+'|Invalid numeric value found in AdminLevel/Level4|'+btext
                return_emits.append(emit_string)
    return return_emits


# BASIC_0116 ------------------------------------------------------
def Basic_0116(Place):
    """
    # Validates for the presence of consecutive (2 or more) punctuations, such as: -/\()!"+,'&.
    # Also checks to make sure that these are not the only characters in the Level5.
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Admin/AdminLevel/Level5
    # Also checks to see if punctuations are the only characters (Basic_0116a)
    # Check for these punctuations = -/\()!"+,'&.
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    consecutive_punctuation = re.compile('(([-/\\\\()!"+,&\'.])\\2+)')      # checks for consecutive punctuations
    punctuation = re.compile('^[-\/\\\(\)\!\"\+\,\&\'\.]*$')                # checks to see if the string contains only these punctuations
    ParsedList = Place.findall(ns+"Parsed")
    for Parsed in ParsedList:
        try:
            Level5 = Parsed.find(ns+"Level5")
            btext = Level5.text
            btext = btext.rstrip("\n")
        except:
            btext = ""
        if btext != "":
            btext = btext.encode('UTF-8')
            consec = consecutive_punctuation.search(btext)
            match = punctuation.search(btext)
            if consec or match:
                btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                if consec:
                    result = consec.group(1)
                    emit_string = 'Basic_0116|'+CountryCode+'|'+PlaceId+'|Multiple consecutive punctuation found in AdminLevel/Level5|'+btext+'|'+result
                else:
                    result = match.group(0)
                    emit_string = 'Basic_0116a|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in AdminLevel/Level5|'+btext+'|'+result
                return_emits.append(emit_string)
    return return_emits

# BASIC_0117 ------------------------------------------------------
def Basic_0117(Place):
    """
    # Validates for the presence of punctuation within the 'Level5' element within the 'AdminLevel' element
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Admin/AdminLevel/Level5
    # Check for all punctuations found in Appendix F of 'Global Places XML Validation Rule Code Listing.docx'
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    ParsedList = Place.findall(ns+"Parsed")
    for Parsed in ParsedList:
        try:
            Level5 = Parsed.find(ns+"Level5")
            btext = Level5.text
            btext = btext.rstrip("\n")
        except:
            btext = ""
        if btext != "":
            for char in btext:
                if re.match(ur'[\u0023-\u0025]+|[\u002A]+|[\u00A1-\u00A7]+|[\u00A9-\u00AE]+|[\u003A-\u003F]+|[\u007B-\u007E]+|[\u0040]+|[\u005B]+|[\u005D-\u005F]+|[\u00B0-\u00B3]+|[\u00B5-\u00B7]+|[\u00B9-\u00BF]+|[\u0060]+|[\u00D7]+',char):
                    btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                    btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                    emit_string = 'Basic_0117|'+CountryCode+'|'+PlaceId+'|Invalid punctuation found in AdminLevel/Level5|'+btext
                    return_emits.append(emit_string)
                    break
    return return_emits


# BASIC_0135 ------------------------------------------------------
def Basic_0135(Place):
    """
    # Validates that there are valid e-mail addresses having key components such as "*@*" or "*.*" being published
    # within the 'ContactString' attribute for values with the 'EMAIL' Contact type.
    # xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=EMAIL)
    # xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=EMAIL) /ContactString
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    # Apparently, new TLDs (Top Level Domains, such as .com, .net, .org, etc..) are going to be allowed up to 63 chars long!
    pattern = re.compile('^(?![a-zA-Z0-9._\-@]*[.-]{2,}[a-zA-Z0-9._\-@]*$)([a-zA-Z0-9._-])*([a-zA-Z0-9]@[a-zA-Z0-9])([a-zA-Z0-9.-])*([a-zA-Z0-9]?\.[a-zA-Z]{2,63})$')
    ContactList = Place.findall(ns+"Contact")
    for Contact in ContactList:
        ContactAttributes = Contact.attrib
        if 'type' in ContactAttributes.keys() and ContactAttributes['type'] == "EMAIL":
            email_address = Contact.find(ns+"ContactString").text
            email_address = email_address.encode('UTF-8')
            email_address = email_address.strip()
            if ';' in email_address:
                ea_list = email_address.split(';')
                for email_address in ea_list:
                    match = pattern.search(email_address)
                    if not match:
                        emit_string = 'Basic_0135|'+CountryCode+'|'+PlaceId+'|Invalid email address|'+email_address
                        return_emits.append(emit_string)
            else:
                match = pattern.search(email_address)
                if not match:
                    emit_string = 'Basic_0135|'+CountryCode+'|'+PlaceId+'|Invalid email address|'+email_address
                    return_emits.append(emit_string)
    return return_emits


# BASIC_0144 ------------------------------------------------------
def Basic_0144(Place):
    """
    # Validates that there are only numeric integers published within the 'ContactString' attribute
    # for values with the 'FAX' Contact type as well as ensures that there are no excess spaces (i.e. 2 or more)
    # within the values (Note: The presence of a  hyphen '-' is allowed to be present along with the numeric values)
    # xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=FAX)/ContactString
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    # Allows Fax numbers to contain digits, spaces, and dashes, but not double dash, double space, or back to back dash and space
    pattern = re.compile('^[0-9]+(?:[ -][0-9]+)*$')
    ContactList = Place.findall(ns+"Contact")
    for Contact in ContactList:
        ContactAttributes = Contact.attrib
        if 'type' in ContactAttributes.keys() and ContactAttributes['type'] == "FAX":
            fax_number = Contact.find(ns+"ContactString").text
            fax_number = fax_number.encode('UTF-8')
            fax_number = fax_number.strip()
            match = pattern.search(fax_number)
            if not match:
                emit_string = 'Basic_0144|'+CountryCode+'|'+PlaceId+'|Invalid FAX number|'+fax_number
                return_emits.append(emit_string)
    return return_emits

# BASIC_0145 ------------------------------------------------------
def Basic_0145(Place):
    """
    # Validates that there are no numbers less than 5 digits or exceeding 11 digits within the the 'ContactString'
    # attribute for values with the 'FAX' Contact type for those POI having values other than 'DEU' and 'AUT' within the 'CountryCode' element
    # xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=FAX)/ContactString
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    ContactList = Place.findall(ns+"Contact")
    for Contact in ContactList:
        ContactAttributes = Contact.attrib
        if 'type' in ContactAttributes.keys() and ContactAttributes['type'] == "FAX":
            fax_number = Contact.find(ns+"ContactString").text
            fax_number = fax_number.encode('UTF-8')
            fax_number = fax_number.strip()
            fax_len = len(fax_number)
            if (fax_len < 5 or fax_len > 11) and (CountryCode != 'AUT' and CountryCode != 'DEU'):
                emit_string = 'Basic_0145|'+CountryCode+'|'+PlaceId+'|Length of FAX value should be between 5 and 11 digits.|'+str(fax_len)+'|'+fax_number
                return_emits.append(emit_string)
    return return_emits


# BASIC_0147 ------------------------------------------------------
def Basic_0147(Place):
    """
    # Validates that there are only numeric integers published within the 'ContactString' attribute
    # for values with the 'MOBILE' Contact type as well as ensures that there are no excess spaces (i.e. 2 or more)
    # within the values (Note: The presence of a  hyphen '-' is allowed to be present along with the numeric values)
    # xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=MOBILE)/ContactString
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    # Allows numbers to contain digits, spaces, and dashes, but not double dash, double space, or back to back dash and space
    pattern = re.compile('^[0-9]+(?:[ -][0-9]+)*$')
    ContactList = Place.findall(ns+"Contact")
    for Contact in ContactList:
        ContactAttributes = Contact.attrib
        if 'type' in ContactAttributes.keys() and ContactAttributes['type'] == "MOBILE":
            mobile_number = Contact.find(ns+"ContactString").text
            mobile_number = mobile_number.encode('UTF-8')
            mobile_number = mobile_number.strip()
            match = pattern.search(mobile_number)
            if not match:
                emit_string = 'Basic_0147|'+CountryCode+'|'+PlaceId+'|Invalid MOBILE number|'+mobile_number
                return_emits.append(emit_string)
    return return_emits



# dvn_check  ------------------------------------------------------
def DVN_0001(Place):
    """
    # Count unique DVNs per country code
    # DVN_0001a|LUX|No Map Attributes	1
    # DVN_0001b|LUX|version:WEU 161|sequenceNumber:1501302	1
    """
    CountryCode = Place.find(ns+"CountryCode").text
    Map = Place.findall(ns+"Map")
    return_emits = []
    version_value = ''
    sequenceNumber_value = ''
    for m in Map:
        attrib_hash = m.attrib
        try:
            version_value = attrib_hash['version']
        except:
            version_value = 'None'
        try:
            sequenceNumber_value = attrib_hash['sequenceNumber']
        except:
            sequenceNumber_value = 'None'
        if not m.attrib:
            emit_string = 'DVN_0001a|'+CountryCode+'|No Map Attributes'
            return_emits.append(emit_string)
        else:
            emit_string = 'DVN_0001b|'+CountryCode+'|'+'version:'+version_value+'|'+'sequenceNumber:'+sequenceNumber_value
            return_emits.append(emit_string)
    return return_emits


# GEO_0001 ------------------------------------------------------
def GEO_0001 (Place):
    """
    # For every Place, return PlaceId, CountryCode, Display Lat/Long, Routing Lat/Long
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Latitude
    # GEO_0001|CountryCode|PlaceId|LinkPvid|Primary= True or False|ROUTING or DISPLAY|LAT|LONG
    # Use case: Wet POI queries
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    if CountryCode == "ITA":    # Target results for just this one country
        LocationList = Place.findall(ns+"Location")
        for Location in LocationList:
            LocationAttributes = Location.attrib
            if 'primary' in LocationAttributes.keys():
                primary_TF = LocationAttributes['primary']
            Link = Location.find(ns+"Link")
            LinkAttributes = Link.attrib
            if 'linkPvid' in LinkAttributes.keys():
                LinkPvid = LinkAttributes['linkPvid']
            GeoPositionList = Location.findall(ns+"GeoPosition")
            for GeoPosition in GeoPositionList:
                GeoPositionAttributes = GeoPosition.attrib
                if 'type' in GeoPositionAttributes.keys():
                    Routing_Display = GeoPositionAttributes['type']
                Longitude = GeoPosition.find(ns+"Longitude")
                Latitude = GeoPosition.find(ns+"Latitude")
                try:
                    Long = str(Longitude.text)
                    LAT = str(Latitude.text)
                    # GEO_0001|CountryCode|PlaceId|Primary= True or False|ROUTING or DISPLAY|LAT|LONG
                    emit_string = 'GEO_0001|'+CountryCode+'|'+PlaceId+'|'+LinkPvid+'|'+primary_TF+'|'+Routing_Display+'|'+LAT+'|'+Long
                    return_emits.append(emit_string)
                except:
                    pass
        return return_emits


# GEO_0002 ------------------------------------------------------
def GEO_0002 (Place):
    """
    # For every Place, return CountryCode, PlaceId, Link PVID, State, Address, Display Lat/Long, Routing Lat/Long
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Latitude
    # GEO_0002|CountryCode|PlaceId|LinkPvid||BaseText|1st CategoryName|Address State|Address Text|ROUTING_LAT|ROUTING_LONG
    # Address Text = Unparsed element
    # Address State = Level2 element
    # Address County = Level3 element
    # Address City = Level4 element
    # Address Zip = PostalCode element
    # Address = HouseNumber element
    # Address street = BaseName element
    # Address street type = StreetType element
    # Updated for only NON-core and ROUTING
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    try:
        state = Place.find(ns+"Level2").text                # Gets the last Level2 value in the Place. Assumes all level2 in the Places are the same
    except:
        state = "None"
    CategoryNameList = []
    CategoryNames = Place.findall(ns+"CategoryName")
    for Category in CategoryNames:
        Name = Category.find(ns+"Text").text
        CategoryNameList.append(Name)
    try:
        FirstCategory = CategoryNameList[0]
    except:
        FirstCategory = "None"

    ExternalReferenceList = Place.findall(ns+"ExternalReference")
    ExternalRefSystem = ""
    for ExternalReference in ExternalReferenceList:
        ExternalRefattrib = ExternalReference.attrib
        if 'system' in ExternalRefattrib.keys():
            ExternalRefSystem = ExternalRefattrib['system']

    return_emits = []
##    if CountryCode == "USA" and (state == "CA" or state == "California"):
    if CountryCode == "USA":
        LocationList = Place.findall(ns+"Location")
        for Location in LocationList:
            try:
                StreetName = Location.find(ns+"BaseName").text
            except:
                StreetName = "None"
            try:
                county = Location.find(ns+"Level3").text
            except:
                county = "None"
            try:
                city = Location.find(ns+"Level4").text
            except:
                city = "None"
            try:
                PostalCode = Location.find(ns+"PostalCode").text
            except:
                PostalCode = "None"
            try:
                address = Location.find(ns+"HouseNumber").text
            except:
                address = "None"
            try:
                StreetType = Location.find(ns+"StreetType").text
            except:
                StreetType = "None"
            Link = Location.find(ns+"Link")
            LinkAttributes = Link.attrib
            if 'linkPvid' in LinkAttributes.keys():
                LinkPvid = LinkAttributes['linkPvid']
            GeoPositionList = Location.findall(ns+"GeoPosition")
            for GeoPosition in GeoPositionList:
                GeoPositionAttributes = GeoPosition.attrib
                if 'type' in GeoPositionAttributes.keys():
                    Routing_Display = GeoPositionAttributes['type']
                Longitude = GeoPosition.find(ns+"Longitude")
                Latitude = GeoPosition.find(ns+"Latitude")
                if Routing_Display == "ROUTING" and ExternalRefSystem != "corepoixml":      # ROUTING and NON-core
                    try:
                        ROUTING_LONG = str(Longitude.text)
                        ROUTING_LAT = str(Latitude.text)
                    except:
                        ROUTING_LONG = ""
                        ROUTING_LAT = ""
                    try:
                        emit_string = 'GEO_0002|'+CountryCode+'|'+PlaceId+'|'+FirstCategory+'|'+LinkPvid+'|'+state+'|'+county+'|'+city+'|'+StreetName+'|'+address+'|'+PostalCode+'|'+StreetType+'|'+ROUTING_LAT+'|'+ROUTING_LONG
                        emit_string = emit_string.encode('UTF-8')
                        return_emits.append(emit_string)
                    except:
                        pass
        return return_emits

# GEO_0003 ------------------------------------------------------
def GEO_0003 (Place):
    """
    # Get all CORE POIs that have been geocoded via GC6
    # For every Place, return CountryCode, PlaceId, linkPvid, Name, Category, POI PVID, ROUTING lat, ROUTING long
    # xpath: PlaceList/Place/Content/Base/ExternalReferenceList/ExternalReference[@system="corepoixml"]
    # xpath: PlaceList/Place/Content/Base/ExternalReferenceList/ExternalReference/ExternalReferenceID[type="SUPPLIER_POIID"]
    # GEO_0003|CountryCode|PlaceId|LinkPvid|POIName|Category|POI_PVID|ROUTING_LAT|ROUTING_LONG

    # Core POIs are identified by whether or not they have system="corepoixml" in the ExternalReference element. For example:
    # <ExternalReference system="corepoixml">

    # GC6 information is determined by looking for supplier="NOKIA_GEOCODER" in the Location field. Examples:
    # <Location label="Pommerotter Weg 19, 52076 Aachen, Deutschland" supplier="NOKIA_GEOCODER" ...
    # <Location label="37 Place de l'Hotel de Ville, 3590 Dudelange, Luxembourg" supplier="NOKIA_GEOCODER" type="MAIN" locationId="NT_bsWF2RJ30rfSgf8c2kCqcD_zcD" primary="true">

    # To get the POI PVIDS, look for type="SUPPLIER_POIID" in the ExternalReferenceID element. For example:
    # <ExternalReferenceID type="SUPPLIER_POIID">400954855</ExternalReferenceID>
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    CategoryNameList = []
    CategoryNames = Place.findall(ns+"CategoryName")
    for Category in CategoryNames:
        Name = Category.find(ns+"Text").text
        CategoryNameList.append(Name)
    try:
        FirstCategory = CategoryNameList[0]
    except:
        FirstCategory = "None"

    ExternalReferenceList = Place.findall(ns+"ExternalReference")
    ExternalRefSystem = ""
    POI_PVID = ""
    for ExternalReference in ExternalReferenceList:
        ExternalRefattrib = ExternalReference.attrib
        if 'system' in ExternalRefattrib.keys():
            ExternalRefSystem = ExternalRefattrib['system']
        ExternalReferenceID = ExternalReference.find(ns+"ExternalReferenceID")
        ExternalReferenceID_attrib = ExternalReferenceID.attrib
        if 'type' in ExternalReferenceID_attrib.keys():
            ExternalReftype = ExternalReferenceID_attrib['type']
        if ExternalReftype == "SUPPLIER_POIID":
            POI_PVID = ExternalReferenceID.text

    return_emits = []
##    if CountryCode == "LUX":
    LocationList = Place.findall(ns+"Location")
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'supplier' in LocationAttributes.keys():
            Location_supplier_value = LocationAttributes['supplier']
        try:
            POIName = Location.find(ns+"BaseText").text
        except:
            POIName = "None"

        Link = Location.find(ns+"Link")
        LinkAttributes = Link.attrib
        if 'linkPvid' in LinkAttributes.keys():
            LinkPvid = LinkAttributes['linkPvid']
        GeoPositionList = Location.findall(ns+"GeoPosition")
        for GeoPosition in GeoPositionList:
            GeoPositionAttributes = GeoPosition.attrib
            if 'type' in GeoPositionAttributes.keys():
                Routing_Display = GeoPositionAttributes['type']
            Longitude = GeoPosition.find(ns+"Longitude")
            Latitude = GeoPosition.find(ns+"Latitude")

            if Routing_Display == "ROUTING" and ExternalRefSystem == "corepoixml" and POI_PVID and Location_supplier_value == "NOKIA_GEOCODER":
                try:
                    ROUTING_LONG = str(Longitude.text)
                    ROUTING_LAT = str(Latitude.text)
                except:
                    ROUTING_LONG = ""
                    ROUTING_LAT = ""
                try:
                    # GEO_0003|CountryCode|PlaceId|LinkPvid|Category|POIName|POI_PVID|ROUTING_LAT|ROUTING_LONG
                    emit_string = 'GEO_0003|'+CountryCode+'|'+PlaceId+'|'+LinkPvid+'|'+FirstCategory+'|'+POIName+'|'+POI_PVID+'|'+ROUTING_LAT+'|'+ROUTING_LONG
                    emit_string = emit_string.encode('UTF-8')
                    return_emits.append(emit_string)
                except:
                    pass
    return return_emits


# GEO_0004 ------------------------------------------------------
def GEO_0004 (Place):
    """
    # For every Place, return CountryCode, PlaceId, linkPvid, category, POI name, Routing Lat/Long
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Latitude
    # GEO_0004|CountryCode|PlaceId|LinkPvid|Category|POIName|LAT|LONG
    # Use case: EVA TMOB Validation analysis
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []

    # Get category
    CategoryNameList = []
    CategoryNames = Place.findall(ns+"CategoryName")
    for Category in CategoryNames:
        Name = Category.find(ns+"Text").text
        CategoryNameList.append(Name)
    try:
        FirstCategory = CategoryNameList[0]
    except:
        FirstCategory = "None"

    # Get POIName, linkPvid and lat/longs
    LocationList = Place.findall(ns+"Location")
    for Location in LocationList:
        LocationAttributes = Location.attrib
        try:
            POIName = Location.find(ns+"BaseText").text
        except:
            POIName = "None"
        Link = Location.find(ns+"Link")
        LinkAttributes = Link.attrib
        if 'linkPvid' in LinkAttributes.keys():
            LinkPvid = LinkAttributes['linkPvid']
        GeoPositionList = Location.findall(ns+"GeoPosition")
        for GeoPosition in GeoPositionList:
            GeoPositionAttributes = GeoPosition.attrib
            if 'type' in GeoPositionAttributes.keys():
                Routing_Display = GeoPositionAttributes['type']
            Longitude = GeoPosition.find(ns+"Longitude")
            Latitude = GeoPosition.find(ns+"Latitude")
            if Routing_Display == "ROUTING":
                try:
                    Long = str(Longitude.text)
                    LAT = str(Latitude.text)
                except:
                    Long = ""
                    LAT = ""
            try:
                # GEO_0004|CountryCode|PlaceId|Primary= True or False|ROUTING|LAT|LONG
                emit_string = 'GEO_0004|'+CountryCode+'|'+PlaceId+'|'+LinkPvid+'|'+FirstCategory+'|'+POIName+'|'+LAT+'|'+Long
                return_emits.append(emit_string)
            except:
                pass
    return return_emits


# GEO_0005 ------------------------------------------------------
def GEO_0005 (Place):
    """
    # For every Place, return CountryCode, PlaceId, linkPvid, category, POI name, Routing Lat/Long
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Display']/Latitude
    # xpath: PlaceList/Place/LocationList/Location/GeopositionList/Geoposition[@type='Routing']/Latitude
    # GEO_0005|CountryCode|PlaceId|LinkPvid|Category|POIName|LAT|LONG
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    if CountryCode == "CAN":    # Target results for just this one country
        # Get category
        CategoryNameList = []
        CategoryNames = Place.findall(ns+"CategoryName")
        for Category in CategoryNames:
            Name = Category.find(ns+"Text").text
            CategoryNameList.append(Name)
        try:
            FirstCategory = CategoryNameList[0]
        except:
            FirstCategory = "None"
        # Get POIName, linkPvid and lat/longs
        BaseTextList = Place.findall(ns+"BaseText")
        BaseText = BaseTextList[0]      # The first name
        POIName = BaseText.text
        LocationList = Place.findall(ns+"Location")
        for Location in LocationList:
            Link = Location.find(ns+"Link")
            LinkAttributes = Link.attrib
            if 'linkPvid' in LinkAttributes.keys():
                LinkPvid = LinkAttributes['linkPvid']
            # Get additional info
        AdditionalDataList = Location.findall(ns+"AdditionalData")
        for AdditionalData in AdditionalDataList:
            AdditionalDataAttribute = AdditionalData.attrib
            #print "AA:", AdditionalDataAttribute
            try:
            	if AdditionalDataAttribute['key'] == "LocationType":
            		LocationType = AdditionalData.text
            except:
            	LocationType = 'None'
            try:
            	if AdditionalDataAttribute['key'] =="MatchLevel":
            		MatchLevel = AdditionalData.text
            except:
            	MatchLevel = 'None'

            GeoPositionList = Location.findall(ns+"GeoPosition")
            for GeoPosition in GeoPositionList:
                GeoPositionAttributes = GeoPosition.attrib
                if 'type' in GeoPositionAttributes.keys():
                    Routing_Display = GeoPositionAttributes['type']
                Longitude = GeoPosition.find(ns+"Longitude")
                Latitude = GeoPosition.find(ns+"Latitude")
                if Routing_Display == "ROUTING":
                    try:
                        Long = str(Longitude.text)
                        LAT = str(Latitude.text)
                    except:
                        Long = ""
                        LAT = ""
            try:
                # ModID|CountryCode|PlaceId|Primary= True or False|ROUTING|LAT|LONG
                POIName = POIName.encode('UTF-8')   # convert the unicode to bytestrings for the return
                emit_string = 'ModID|'+CountryCode+'|'+PlaceId+'|'+LinkPvid+'|'+FirstCategory+'|'+POIName+'|'+LAT+'|'+Long+'|'+LocationType+'|'+MatchLevel
                return_emits.append(emit_string)
            except:
                pass
        return return_emits


# STATS_0001 ------------------------------------------------------
def Stats_0001(Place):
    """
    # Count Places per CountryCode
    """
    try:
        CountryCode = Place.find(ns+"CountryCode").text
    except:
        CountryCode = 'None'
    emit_string = 'Stats_0001|'+CountryCode
    return emit_string


# STATS_0002 ------------------------------------------------------
def Stats_0002(Place):
    """
    # Count Locations per CountryCode
    """
    try:
        CountryCode = Place.find(ns+"CountryCode").text
    except:
        CountryCode = 'None'
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        emit_string = 'Stats_0002|'+CountryCode
        return_emits.append(emit_string)
    return return_emits


# STATS_0003 ------------------------------------------------------
def Stats_0003(Place):
    """
    # Global counts of each unique CategoryId
    """
    CategoryIdList = Place.findall(ns+"CategoryId")
    return_emits = []
    for CategoryId in CategoryIdList:
        CategoryIdText = CategoryId.text
        emit_string = 'Stats_0003|'+CategoryIdText
        return_emits.append(emit_string)
    return return_emits


# STATS_0004 ------------------------------------------------------
def Stats_0004(Place):
    """
    # CategoryId per CountryCode
    """
    try:
        CountryCode = Place.find(ns+"CountryCode").text
    except:
        CountryCode = 'None'
    CategoryIdList = Place.findall(ns+"CategoryId")
    return_emits = []
    for CategoryId in CategoryIdList:
        CategoryIdText = CategoryId.text
        emit_string = 'Stats_0004|'+CategoryIdText+'|'+CountryCode
        return_emits.append(emit_string)
    return return_emits


# STATS_0005 ------------------------------------------------------
def Stats_0005(Place):
    """
    # Provides a count of the values present within the 'primary' and 'supplier' elements per 'CountryCode'.
    xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/CountryCode and PlaceList/Place/LocationList/Location and
    xpath: PlaceList/Place/LocationList/Location(@primary) and
    xpath: PlaceList/Place/LocationList/Location(@supplier)
    """
    try:
        CountryCode = Place.find(ns+"CountryCode").text
    except:
        CountryCode = 'None'
    LocationList = Place.findall(ns+"Location")
    return_emits = []
    for Location in LocationList:
        LocationAttributes = Location.attrib
        # LocationAttributes['primary']
        try:
            Location_primary_value = LocationAttributes['primary']
        except:
            Location_primary_value = 'None'
        # LocationAttributes['supplier']
        try:
            Location_supplier_value = LocationAttributes['supplier']
        except:
            Location_supplier_value = 'None'
        emit_string = 'Stats_0005|'+Location_primary_value+'|'+Location_supplier_value+'|'+CountryCode
        return_emits.append(emit_string)
    return return_emits


# New_0001 ------------------------------------------------------
def New_0001(Place):
    """
    # Print all ContactStrings in order to troubleshoot possible issues with line breaks that would break the flow of xml
    xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=FAX)/ContactString
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    ContactStrings = Place.findall(ns+"ContactString")
    for ContactString in ContactStrings:
        if CountryCode == "USA":
            emit_string = 'New_0001|'+CountryCode+'|'+PlaceId+'|'+ContactString.text
            return emit_string


# New_0002 ------------------------------------------------------
def New_0002(Place):
    """
    # Look for "Churchh" or "churchh" in BaseTexts to identify the scope of an incorrect sub-string replace
    xpath: PlaceList/Place/Content/Base/NameList/Name/TextList/Text/BaseText
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)

    return_emits = []

    BaseTextList = Place.findall(ns+"BaseText")
    for BaseText in BaseTextList:
        btext = BaseText.text
        if "Churchh" in btext or "churchh" in btext:
            btext = btext.rstrip("\n")
            btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
            emit_string = 'New_0002|'+CountryCode+'|'+PlaceId+'|Churchh found in BaseText|'+btext
            return_emits.append(emit_string)
    return return_emits


# New_0003 ------------------------------------------------------
def New_0003(Place):
    """
    # Count the total number of non-core POIs per country. Use region/country lookup dictionary to query by region
    xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=FAX)/ContactString
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    ContactStrings = Place.findall(ns+"ContactString")
    for ContactString in ContactStrings:
        emit_string = 'New_0003|'+CountryCode+'|'+PlaceId+'|'+ContactString.text
        return emit_string


# New_0004 ------------------------------------------------------
def New_0004(Place):
    """
    # Look up a Place_ID to see if it is in a given country
    """
    PlaceID_to_search = '276u33dc-915a1c96ef2b44deb456109cd81ecb50'
##    PlaceID_to_search = '442u0ue6-1ce317a5b64947ea8fcf959ca31a7773'
    Country_to_search = 'DEU'
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    if CountryCode == Country_to_search:    # Target results for just this one country
        if PlaceID_to_search == PlaceId:
            emit_string = 'New_0004|'+CountryCode+'|'+PlaceId+' Found.'
            return emit_string


# New_0005 ------------------------------------------------------
def New_0005(Place):
    """
    # Look up a Place_ID to see if it is in a given country,
    and return ALL of the Category names and IDs as well
    """
    PlaceID_to_search = '276u33dc-915a1c96ef2b44deb456109cd81ecb50'
##    PlaceID_to_search = '442u0ue6-1ce317a5b64947ea8fcf959ca31a7773'
    Country_to_search = 'DEU'
    return_emits = []
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    if CountryCode == Country_to_search:    # Target results for just this one country
        if PlaceID_to_search == PlaceId:

            CategoryIds = Place.findall(ns+"CategoryId")
            CategoryNames = Place.findall(ns+"CategoryName")
            x = 0
            while x < len(CategoryIds):
                try:
                    CategoryID = CategoryIds[x].text
                except:
                    CategoryID = "None"
                try:
                    CategoryName = CategoryNames[x].find(ns+"Text").text
                except:
                    CategoryName = "None"
                emit_string = 'New_0005|'+CountryCode+'|'+PlaceId+'|'+CategoryID+'|'+CategoryName
                return_emits.append(emit_string)
                x += 1
    return return_emits


# New_0006 ------------------------------------------------------
def New_0006(Place):
    """
    # Profanity check
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    profanity_list = ['2g1c', '2 girls 1 cup', 'acrotomophilia', 'anal', 'anilingus', 'anus', 'arsehole', 'ass', 'asshole', 'assmunch', 'auto erotic', 'autoerotic', 'babeland', 'baby batter', 'ball gag', 'ball gravy', 'ball kicking', 'ball licking', 'ball sack', 'ball sucking', 'bangbros', 'bareback', 'barely legal', 'barenaked', 'bastardo', 'bastinado', 'bbw', 'bdsm', 'beaver cleaver', 'beaver lips', 'bestiality', 'bi curious', 'big black', 'big breasts', 'big knockers', 'big tits', 'bimbos', 'birdlock', 'bitch', 'black cock', 'blonde action', 'blonde on blonde action', 'blow j', 'blow your l', 'blue waffle', 'blumpkin', 'bollocks', 'bondage', 'boner', 'boob', 'boobs', 'booty call', 'brown showers', 'brunette action', 'bukkake', 'bulldyke', 'bullet vibe', 'bung hole', 'bunghole', 'busty', 'butt', 'buttcheeks', 'butthole', 'camel toe', 'camgirl', 'camslut', 'camwhore', 'carpet muncher', 'carpetmuncher', 'chocolate rosebuds', 'circlejerk', 'cleveland steamer', 'clit', 'clitoris', 'clover clamps', 'clusterfuck', 'cock', 'cocks', 'coprolagnia', 'coprophilia', 'cornhole', 'cum', 'cumming', 'cunnilingus', 'cunt', 'darkie', 'date rape', 'daterape', 'deep throat', 'deepthroat', 'dick', 'dildo', 'dirty pillows', 'dirty sanchez', 'dog style', 'doggie style', 'doggiestyle', 'doggy style', 'doggystyle', 'dolcett', 'domination', 'dominatrix', 'dommes', 'donkey punch', 'double dong', 'double penetration', 'dp action', 'eat my ass', 'ecchi', 'ejaculation', 'erotic', 'erotism', 'escort', 'ethical slut', 'eunuch', 'faggot', 'fecal', 'felch', 'fellatio', 'feltch', 'female squirting', 'femdom', 'figging', 'fingering', 'fisting', 'foot fetish', 'footjob', 'frotting', 'fuck', 'fucking', 'fuck buttons', 'fudge packer', 'fudgepacker', 'futanari', 'g-spot', 'gang bang', 'gay sex', 'genitals', 'giant cock', 'girl on', 'girl on top', 'girls gone wild', 'goatcx', 'goatse', 'gokkun', 'golden shower', 'goo girl', 'goodpoop', 'goregasm', 'grope', 'group sex', 'guro', 'hand job', 'handjob', 'hard core', 'hardcore', 'hentai', 'homoerotic', 'honkey', 'hooker', 'hot chick', 'how to kill', 'how to murder', 'huge fat', 'humping', 'incest', 'intercourse', 'jack off', 'jail bait', 'jailbait', 'jerk off', 'jigaboo', 'jiggaboo', 'jiggerboo', 'jizz', 'juggs', 'kike', 'kinbaku', 'kinkster', 'kinky', 'knobbing', 'leather restraint', 'leather straight jacket', 'lemon party', 'lolita', 'lovemaking', 'make me come', 'male squirting', 'masturbate', 'menage a trois', 'milf', 'missionary position', 'motherfucker', 'mound of venus', 'mr hands', 'muff diver', 'muffdiving', 'nambla', 'nawashi', 'negro', 'neonazi', 'nig nog', 'nigga', 'nigger', 'nimphomania', 'nipple', 'nipples', 'nsfw images', 'nude', 'nudity', 'nympho', 'nymphomania', 'octopussy', 'omorashi', 'one cup two girls', 'one guy one jar', 'orgasm', 'orgy', 'paedophile', 'panties', 'panty', 'pedobear', 'pedophile', 'pegging', 'penis', 'phone sex', 'piece of shit', 'piss pig', 'pissing', 'pisspig', 'playboy', 'pleasure chest', 'pole smoker', 'ponyplay', 'poof', 'poop chute', 'poopchute', 'porn', 'porno', 'pornography', 'prince albert piercing', 'pthc', 'pubes', 'pussy', 'queaf', 'raghead', 'raging boner', 'rape', 'raping', 'rapist', 'rectum', 'reverse cowgirl', 'rimjob', 'rimming', 'rosy palm', 'rosy palm and her 5 sisters', 'rusty trombone', 's&m', 'sadism', 'scat', 'schlong', 'scissoring', 'semen', 'sex', 'sexo', 'sexy', 'shaved beaver', 'shaved pussy', 'shemale', 'shibari', 'shit', 'shota', 'shrimping', 'slanteye', 'slut', 'smut', 'snatch', 'snowballing', 'sodomize', 'sodomy', 'spic', 'spooge', 'spread legs', 'strap on', 'strapon', 'strappado', 'strip club', 'style doggy', 'suck', 'sucks', 'suicide girls', 'sultry women', 'swastika', 'swinger', 'tainted love', 'taste my', 'tea bagging', 'threesome', 'throating', 'tied up', 'tight white', 'tit', 'tits', 'titties', 'titty', 'tongue in a', 'topless', 'tosser', 'towelhead', 'tranny', 'tribadism', 'tub girl', 'tubgirl', 'tushy', 'twat', 'twink', 'twinkie', 'two girls one cup', 'undressing', 'upskirt', 'urethra play', 'urophilia', 'vagina', 'venus mound', 'vibrator', 'violet blue', 'violet wand', 'vorarephilia', 'voyeur', 'vulva', 'wank', 'wet dream', 'wetback', 'white power', 'women rapping', 'wrapping men', 'wrinkled starfish', 'xx', 'xxx', 'yaoi', 'yellow showers', 'yiffy', 'zoophilia', 'big-dick', 'big-prick', 'super-prick', 'meaty-ball', 'deez-nut', 'big-n-hard', 'big-and-hard', 'chester-the-pussy-molester', 'hard-on', 'hot-cock', 'bull-shit', 'load-of-crap', 'cock-suck', 'suck-my-cock', 'blow-job', 'facial-fetish', 'fuck', 'suck-(cock|dick)', 'hand-job', 'jack-off', 'jerk-off', '(lick|suck)-(cock|dick|nipples|tits)', 'crotch', 'ass-crack', 'butt-crack', 'dick-head', 'prick-head', 'ass-hole', 'bastard', 'punk-ass', 'pussy-ass', 'faggot', 'dick-less', 'm(o|u)th(er|a|)-fuck', 'god-dam', 'shitty-ass', 'nigg?(a|er|uh)', 'bitch', 'whore', 'suck-my-ass', 'hug-my-nuts', 'goto-hell', 'eat-shit', 'shit-eater', 'shit-head', 'turd-head', 'shit-face', 'suck-my-cock', 'fuck-off', 'eat-poop', 'smell-farts', 'half-assed', 'piss--face', 'piss--ass', 'poop--face', 'piss-drink', 'drink-piss', 'pussies', 'hot-puss', 'juicy-puss', 'smelly-puss', 'funky-puss', 'white-puss', 'black-puss', 'asian-puss', 'sex-puss', 'sex-clit', 'juic-clit', 'milk-my-breasts']
    try:
        BaseTextList = Place.findall(ns+"BaseText")
        for BaseText in BaseTextList:
            btext = BaseText.text
            btext_attrib = BaseText.attrib
            for word in profanity_list:
                word = ' '+word+' '# sandwich the word with spaces
                if word in btext:
                    btext = btext.replace('|', '#')  # remove pipe symbols so that they don't mess up the output
                    btext = btext.encode('UTF-8')   # convert the unicode to bytestrings for the return
                    emit_string = 'New_0006|'+CountryCode+'|'+PlaceId+'|Profanity found in BaseText|'+word+'|'+btext
                    return_emits.append(emit_string)
                    break
    except:
        pass
    return return_emits


# New_0007 ------------------------------------------------------
def New_0007(Place):
    """
    # Point Binding POC Analysis
    (Primary Location vs PA_BINDING)

    1. street name (check for delta)
    2. language (check for delta)
    3. lat/long distance xy (Primary Location vs PA_Resolving)
    4. link pvids (check for delta)
    5. house numbers (check for delta)
    6. volume (number of new vs existing)

    Primay location:
    <Location supplier="Source" type="MAIN" primary="true">

    """
    def returnElementName(tag):
        cleaned_element = tag.split('}')
        cleaned_element = cleaned_element[1]
        return cleaned_element


    def element_recurse(scopeElement, x=0):
        indent = "\t"
        scope = list(scopeElement)
        for element in scope:
            print x*indent+returnElementName(element.tag), element.attrib, element.text
            child = list(element)
            if child:
                element_recurse(child, x+1)

    def dict_compare(d1, d2):
        d1_keys = set(d1.keys())
        d2_keys = set(d2.keys())
        intersect_keys = d1_keys.intersection(d2_keys)
        modified = {o : (d1[o], d2[o]) for o in intersect_keys if d1[o] != d2[o]}
        same = set(o for o in intersect_keys if d1[o] == d2[o])
        return modified

    primary_loc = {}
    pa_binding = {}
    pa_resolving = {}
    emit_string = ""
    pa_resolvingLinkPvid = "None"
    pa_bindingLinkPvid = "None"
    pblangCode = "None"
    prlangCode = "None"
    pbStreetName = "None"
    prStreetName = "None"
    pbHouseNumber = "None"
    prHouseNumber = "None"
    DISPLAY_LAT = ""; ROUTING_LAT = ""
    DISPLAY_Long = ""; ROUTING_Long = ""
    prDISPLAY_LAT = ""; prROUTING_LAT = ""
    prDISPLAY_Long = ""; prROUTING_Long = ""
    pbDISPLAY_LAT = ""; pbROUTING_LAT = ""
    pbDISPLAY_Long = ""; pbROUTING_Long = ""

    CountryCode, PlaceId = CountryCode_PlaceID(Place)

    # Determine if it is a Core POI or not
    Core_POI = core_or_non_core(Place)

    LocationList = Place.findall(ns+"Location")
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if 'primary' in LocationAttributes.keys() and 'type' in LocationAttributes.keys():
            # Location = primary
            if LocationAttributes['primary'] == 'true' and LocationAttributes['type'] == 'MAIN':
                # Note: In LUX I am seeing that the primary locations have ONLY ROUTING, but the PA_BINDING and PA_RESOLVING loccations also have a DISPLAY
                # The PA_RESOLVING lat/longs needed to be divided by 100000. For example 4985222.0 should be 49.85222

                # Side
                try:
                    Side = Location.find(ns+"Side").text
                except:
                    Side = "None"

                # Spot
                try:
                    Spot = Location.find(ns+"Spot").text
                except:
                    Spot = "None"

                #element_recurse(Location, 0)
                Parsed = Place.find(ns+'Parsed')
                ParsedAttr = Parsed.attrib
                if 'languageCode' in ParsedAttr.keys():
                    langCode = ParsedAttr['languageCode']
                else:
                    langCode = "None"
                try:
                    StreetName = Location.find(ns+"BaseName").text
                    StreetName = StreetName.encode('UTF-8')   # convert the unicode to bytestrings for the return
                except:
                    StreetName = "None"
                try:
                    HouseNumber = Location.find(ns+"HouseNumber").text
                except:
                    HouseNumber = "None"
                try:
                    Link = Location.find(ns+"Link")
                except:
                    Link = "None"
                try:
                    LinkAttributes = Link.attrib
                except:
                    LinkAttributes = "None"
                primaryLinkPvid = ''
                if LinkAttributes != "None":
                    if 'linkPvid' in LinkAttributes.keys():
                        primaryLinkPvid = LinkAttributes['linkPvid']
                GeoPositionList = Location.findall(ns+"GeoPosition")
                for GeoPosition in GeoPositionList:
                    GeoPositionAttributes = GeoPosition.attrib
                    if 'type' in GeoPositionAttributes.keys():
                        Routing_Display = GeoPositionAttributes['type']
                        Longitude = GeoPosition.find(ns+"Longitude")
                        Latitude = GeoPosition.find(ns+"Latitude")
                        if Routing_Display == "ROUTING":
                            ROUTING_Long = str(Longitude.text)
                            ROUTING_LAT = str(Latitude.text)
                        if Routing_Display == "DISPLAY":
                            DISPLAY_Long = str(Longitude.text)
                            DISPLAY_LAT = str(Latitude.text)

                # |'+primaryLinkPvid+'|'+langCode+'|'+StreetName+'|'+HouseNumber+'|'+ROUTING_LAT+'|'+ROUTING_Long+'|'+DISPLAY_LAT+'|'+DISPLAY_Long+'
                primary_loc["linkPvid"] = primaryLinkPvid
                primary_loc["langCode"] = langCode
                primary_loc["StreetName"] = StreetName
                primary_loc["HouseNumber"] = HouseNumber
                ROUTING_xy = ROUTING_LAT+'|'+ROUTING_Long
                DISPLAY_xy = DISPLAY_LAT+'|'+DISPLAY_Long
                primary_loc["ROUTING_xy"] = ROUTING_xy
                primary_loc["DISPLAY_xy"] = DISPLAY_xy

        # Location supplier = 'PA_BINDING'
        if LocationAttributes['supplier'] == 'PA_BINDING':
            Parsed = Place.find(ns+'Parsed')
            try:
                ParsedAttr = Parsed.attrib
            except:
                pblangCode = "None"
            if 'languageCode' in ParsedAttr.keys():
                pblangCode = ParsedAttr['languageCode']
            else:
                pblangCode = "None"
            try:
                pbStreetName = Location.find(ns+"BaseName").text
                pbStreetName = pbStreetName.encode('UTF-8')   # convert the unicode to bytestrings for the return
            except:
                pbStreetName = "None"
            try:
                pbHouseNumber = Location.find(ns+"HouseNumber").text
            except:
                pbHouseNumber = "None"

            # New column "Bound to PA?" - <AdditionalData key="RequestForPA">
            Bound_to_PA = ''
            AdditionalData_list = Location.findall(ns+"AdditionalData")
            for AdditionalData in AdditionalData_list:
                try:
                    AdditionalData_attrib = AdditionalData.attrib
                    if 'key' in AdditionalData_attrib.keys():
                        if AdditionalData_attrib['key'] == 'RequestForPA':
                            RequestForPA = AdditionalData.text
                            if '/pointBinding/bind/pointaddress/' in RequestForPA:
                                Bound_to_PA = "Bound by PA"
                except:
                    pass

            try:
                Link = Location.find(ns+"Link")
            except:
                Link = "None"
            try:
                LinkAttributes = Link.attrib
            except:
                LinkAttributes = "None"
                pa_bindingLinkPvid = "None"
            if LinkAttributes != "None":
                if 'linkPvid' in LinkAttributes.keys():
                    pa_bindingLinkPvid = LinkAttributes['linkPvid']
            GeoPositionList = Location.findall(ns+"GeoPosition")
            for GeoPosition in GeoPositionList:
                GeoPositionAttributes = GeoPosition.attrib
                if 'type' in GeoPositionAttributes.keys():
                    Routing_Display = GeoPositionAttributes['type']
                    Longitude = GeoPosition.find(ns+"Longitude")
                    Latitude = GeoPosition.find(ns+"Latitude")
                    if Routing_Display == "ROUTING":
                        pbROUTING_Long = str(Longitude.text)
                        pbROUTING_LAT = str(Latitude.text)
                    if Routing_Display == "DISPLAY":
                        pbDISPLAY_Long = str(Longitude.text)
                        pbDISPLAY_LAT = str(Latitude.text)

            # +pa_bindingLinkPvid+'|'+pblangCode+'|'+pbStreetName+'|'+pbHouseNumber+'|'+pbROUTING_LAT+'|'+pbROUTING_Long+'|'+pbDISPLAY_LAT+'|'+pbDISPLAY_Long+'
            pa_binding["linkPvid"] = pa_bindingLinkPvid
            pa_binding["langCode"] = pblangCode
            pa_binding["StreetName"] = pbStreetName
            pa_binding["HouseNumber"] = pbHouseNumber
            pa_binding["ROUTING_LAT"] = pbROUTING_LAT
            pa_binding["ROUTING_Long"] = pbROUTING_Long
            pa_binding["DISPLAY_LAT"] = pbDISPLAY_LAT
            pa_binding["DISPLAY_Long"] = pbDISPLAY_Long

        # Location supplier = 'PA_RESOLVING'
        if LocationAttributes['supplier'] == 'PA_RESOLVING':
            try:
                ParsedAttr = Parsed.attrib
            except:
                prlangCode = "None"
            ParsedAttr = Parsed.attrib
            if 'languageCode' in ParsedAttr.keys():
                prlangCode = ParsedAttr['languageCode']
            else:
                prlangCode = "None"
            try:
                prStreetName = Location.find(ns+"BaseName").text
                prStreetName = prStreetName.encode('UTF-8')   # convert the unicode to bytestrings for the return
            except:
                prStreetName = "None"
            try:
                prHouseNumber = Location.find(ns+"HouseNumber").text
            except:
                prHouseNumber = "None"

            # Side
            try:
                prSide = Location.find(ns+"Side").text
            except:
                prSide = "None"

            # Spot
            try:
                prSpot = Location.find(ns+"Spot").text
            except:
                prSpot = "None"

            # New column "ErrorMessage" - <AdditionalData key="ErrorMessage">
            ErrorMessage = ''
            AdditionalData_list = Location.findall(ns+"AdditionalData")
            for AdditionalData in AdditionalData_list:
                try:
                    AdditionalData_attrib = AdditionalData.attrib
                    if 'key' in AdditionalData_attrib.keys():
                        if AdditionalData_attrib['key'] == 'ErrorMessage':
                            ErrorMessage = AdditionalData.text
                except:
                    pass

            Link = Location.find(ns+"Link")
            try:
                LinkAttributes = Link.attrib
            except:
                pa_resolvingLinkPvid = "None"
            if 'linkPvid' in LinkAttributes.keys():
                pa_resolvingLinkPvid = LinkAttributes['linkPvid']
            GeoPositionList = Location.findall(ns+"GeoPosition")
            for GeoPosition in GeoPositionList:
                GeoPositionAttributes = GeoPosition.attrib
                if 'type' in GeoPositionAttributes.keys():
                    Routing_Display = GeoPositionAttributes['type']
                    Longitude = GeoPosition.find(ns+"Longitude")
                    Latitude = GeoPosition.find(ns+"Latitude")
                    if Routing_Display == "ROUTING":
                        prROUTING_Long = str(Longitude.text)
                        prROUTING_LAT = str(Latitude.text)
                    if Routing_Display == "DISPLAY":
                        prDISPLAY_Long = str(Longitude.text)
                        prDISPLAY_LAT = str(Latitude.text)

            # pa_resolvingLinkPvid+'|'+prlangCode+'|'+prStreetName+'|'+prHouseNumber+'|'+prROUTING_LAT+'|'+prROUTING_Long+'|'+prDISPLAY_LAT+'|'+prDISPLAY_Long
            pa_resolving["linkPvid"] = pa_resolvingLinkPvid
            pa_resolving["langCode"] = prlangCode
            pa_resolving["StreetName"] = prStreetName
            pa_resolving["HouseNumber"] = prHouseNumber
            try:
                prROUTING_xy = str(float(prROUTING_LAT))+'|'+str(float(prROUTING_Long))
            except:
                prROUTING_xy = prROUTING_LAT+'|'+prROUTING_Long
            try:
                prDISPLAY_xy = str(float(prDISPLAY_LAT))+'|'+str(float(prDISPLAY_Long))
            except:
                prDISPLAY_xy = prDISPLAY_LAT+'|'+prDISPLAY_Long
            pa_resolving["ROUTING_xy"] = prROUTING_xy
            pa_resolving["DISPLAY_xy"] = prDISPLAY_xy


    # Deltas (left blank if no delta)
    StreetName_delta = ''; StreetName_prev = ''; StreetName_new = ''
    langCode_delta = ''
    LinkPvid_delta = ''; LinkPvid_prev = ''; LinkPvid_new = ''
    HouseNumber_delta = ''; HouseNumber_prev = ''; HouseNumber_new = ''
    ROUTING_delta = ''
    DISPLAY_delta = ''

    modified = dict_compare(primary_loc, pa_resolving)
    if modified:
        # StreetName
        if 'StreetName' in modified.keys():
            ms = list(modified['StreetName'])
            StreetName_new = ms[1]
            StreetName_prev = StreetName

            if modified['StreetName'][0] == "None":
                StreetName_delta = 'StreetName added'
            elif modified['StreetName'][1] == "None":
                StreetName_delta = 'StreetName dropped'
            else:
                StreetName_delta = 'StreetName changed'

        # langCode
        if 'langCode' in modified.keys():
            if modified['langCode'][0] == "None":
                langCode_delta = 'langCode added: '+str(modified['langCode'])
            elif modified['langCode'][1] == "None":
                langCode_delta = 'langCode dropped: '+str(modified['langCode'])
            else:
                langCode_delta = 'langCode changed: '+str(modified['langCode'])

        # LinkPvid
        if 'LinkPvid' in modified.keys():
            LinkPvid_new = pa_resolvingLinkPvid
            LinkPvid_prev = primaryLinkPvid
            if modified['LinkPvid'][0] == "None":
                LinkPvid_delta = 'LinkPvid added'
            elif modified['LinkPvid'][1] == "None":
                LinkPvid_delta = 'LinkPvid dropped'
            else:
                LinkPvid_delta = 'LinkPvid changed'

        # HouseNumber
        if 'HouseNumber' in modified.keys():
            HouseNumber_new = prHouseNumber
            HouseNumber_prev = HouseNumber
            if modified['HouseNumber'][0] == "None":
                HouseNumber_delta = 'HouseNumber added'
            elif modified['HouseNumber'][1] == "None":
                HouseNumber_delta = 'HouseNumber dropped'
            else:
                HouseNumber_delta = 'HouseNumber changed'


        # xy differences -------------------------------------
        routing_labels = set(["ROUTING_xy"])
        display_labels = set(["DISPLAY_xy"])
        mod_keys = set(modified.keys())

        if routing_labels & mod_keys:
            rxy = modified["ROUTING_xy"]
            a = rxy[0].split('|')
            b = rxy[1].split('|')

            # Handle the case where one xy is empty
            if '' in a:
                ROUTING_delta = "Routing point added in PA_RESOLVING"
            elif '' in b:
                ROUTING_delta = "Routing point dropped in PA_RESOLVING"
            else:
                routing_diff = math_distance(a, b, unit="m")
                if routing_diff == 0:
                    ROUTING_delta  = "Same ROUTING location but decimal rounding difference in xy."
                else:
                    ROUTING_delta = str(routing_diff)

        if display_labels & mod_keys:
            dxy = modified["DISPLAY_xy"]
            a = dxy[0].split('|')
            b = dxy[1].split('|')

            # Handle the case where one xy is empty
            if '' in a:
                DISPLAY_delta = "Display point added in PA_RESOLVING"
            elif '' in b:
                DISPLAY_delta = "Display point dropped in PA_RESOLVING"
            else:
                display_diff = math_distance(a, b, unit="m")
                if display_diff == 0:
                    DISPLAY_delta = "Same DISPLAY location but decimal rounding difference in xy."
                else:
                    DISPLAY_delta = str(display_diff)

        # Mitigate the Excel bug that turns HouseNumbers into dates
        if '-' in prHouseNumber:
            prHouseNumber = "'"+prHouseNumber+"'"
        if '-' in HouseNumber_new:
            HouseNumber_new = "'"+HouseNumber_new+"'"
        if '-' in HouseNumber:
            HouseNumber = "'"+HouseNumber+"'"

        emit_string = PlaceId+'|'+CountryCode+'|'+Core_POI+'|'+primaryLinkPvid+'|'+langCode+'|'+StreetName+'|'+Side+'|'+Spot+'|'+ \
                      HouseNumber+'|'+ROUTING_LAT+'|'+ROUTING_Long+'|'+DISPLAY_LAT+'|'+DISPLAY_Long+'|PA_RESOLVING|'+pa_resolvingLinkPvid+'|'+ \
                      prlangCode+'|'+prStreetName+'|'+prSide+'|'+prSpot+'|'+prHouseNumber+'|'+prROUTING_LAT+'|'+prROUTING_Long+'|'+ \
                      prDISPLAY_LAT+'|'+prDISPLAY_Long+'|Deltas|'+StreetName_delta+'|'+StreetName_prev+'|'+StreetName_new+'|'+ \
                      langCode_delta+'|'+LinkPvid_delta+'|'+LinkPvid_prev+'|'+LinkPvid_new+'|'+HouseNumber_delta+'|'+HouseNumber_prev+'|'+ \
                      HouseNumber_new+'|'+ROUTING_delta+'|'+DISPLAY_delta+'|'+Bound_to_PA+'|'+ErrorMessage
        return emit_string


# New_0008 ------------------------------------------------------
def New_0008(Place):
    """
    # Get volume listings for Point Binding POC
    # Number of Places that have PA_RESOLVING (New_0008a)
    # Number of Places that do NOT have PA_RESOLVING (New_0008b)
    # Each PlaceId that doesn't have PA_RESOLVING (New_0008c)
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)

    # Determine if it is a Core POI or not
    Core_POI = core_or_non_core(Place)

    return_emits = []
    PAR = 0     # Flag to identify if PA_RESOLVING is found in the LocationList
    LocationList = Place.findall(ns+"Location")
    for Location in LocationList:
        LocationAttributes = Location.attrib
        if LocationAttributes['supplier'] == 'PA_RESOLVING':
            PAR = 1
    if PAR == 1:
        emit_string = 'New_0008a|'+Core_POI+'|Has PA_RESOLVING'
        return_emits.append(emit_string)
    else:
        emit_string = 'New_0008b|'+Core_POI+'|Does not have PA_RESOLVING'
        return_emits.append(emit_string)
        emit_string = 'New_0008c|'+PlaceId+'|'+Core_POI+'|Does not have PA_RESOLVING'
        return_emits.append(emit_string)
    return return_emits


# New_0009 ------------------------------------------------------
def New_0009(Place):
    """
    # Search for facebook in contact strings. If found, output the full URL.
    xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=FAX)/ContactString
    # http://www.facebook.com
    # facebook or fb
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    ContactStrings = Place.findall(ns+"ContactString")
    for ContactString in ContactStrings:
        if "facebook" in ContactString or "fb" in ContactString:
            emit_string = 'New_0009|'+CountryCode+'|'+ContactString.text
        return emit_string


# New_0010 ------------------------------------------------------
def New_0010(Place):
    """
    # Get counts of Unparsed per Place to see if each Place has only one Unparsed
    xpath: PlaceList/Place/LocationList/Location/Address/UnparsedList/Unparsed
    """
    Core_POI = core_or_non_core(Place)
    return_emits = []
    try:
        UnparsedList = Place.findall(ns+"Unparsed")
        num_unparsed = len(UnparsedList)
        # Print the unparsed addresses. Not recomnended for large datasets.
        ##        Unparsed = UnparsedList[0]
        ##        emit_string = 'New_0010|'+Unparsed.text
        ##        return_emits.append(emit_string)
    except:
        num_unparsed = 0

    emit_string = 'New_0010|'+Core_POI+'|'+str(num_unparsed)
    return_emits.append(emit_string)

    return return_emits


# New_0011 ------------------------------------------------------
def New_0011(Place):
    """
    # Get counts of AdditionalData key="state"
    xpath: PlaceList/Place/LocationList/Location/Address/AdditionalData@key="state"
    """
    Core_POI = core_or_non_core(Place)
    AddDataList = Place.findall(ns+"AdditionalData")
    for AdditionalData in AddDataList:
        AdditionalData_attrib = AdditionalData.attrib
        if 'key' in AdditionalData_attrib.keys():
            if AdditionalData_attrib['key'] == 'state':
                emit_string = 'New_0011|'+Core_POI+'|Has state'
                break
            else:
                emit_string = 'New_0011|'+Core_POI+'|Does not have state'
    return emit_string


# New_0012 ------------------------------------------------------
def New_0012(Place):
    """
    # Get counts of AdditionalData key="state"
    xpath: PlaceList/Place/LocationList/Location/Address/AdditionalData@key="state"
    """
    Core_POI = core_or_non_core(Place)
    AddDataList = Place.findall(ns+"AdditionalData")
    for AdditionalData in AddDataList:
        AdditionalData_attrib = AdditionalData.attrib
        if 'key' in AdditionalData_attrib.keys():
            if AdditionalData_attrib['key'] == 'PA-Zone':
                emit_string = 'New_0012|'+Core_POI+'|Has PA-Zone'
                break
            else:
                emit_string = 'New_0012|'+Core_POI+'|Does not have PA-Zone'
    return emit_string


# New_0013 ------------------------------------------------------
def New_0013(Place):
    """
    # Get chains
    xpath: PlaceList/Place/Content/Base/ChainList/Chain/Name/Text
    Get the .text where @ default="true"
    <Id>20057</Id>
    <Text default="true" type="OFFICIAL" languageCode="en">Off Broadway</Text>
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)

    ChainName = ''; ChainId = ''
    ChainList = Place.findall(ns+"Chain")
    for Chain in ChainList:
        ChainId = Chain.find(ns+"Id").text
        Text = Chain.find(ns+"Text")
        TextAttrib = Text.attrib
        # default="true" type="OFFICIAL"
        if 'default' in TextAttrib.keys() and 'type' in TextAttrib.keys():
            if TextAttrib['default'] == 'true' and TextAttrib['type'] == 'OFFICIAL':
                ChainName = Text.text
    if ChainName or ChainId:
        emit_string = 'New_0013|'+PlaceId+'|'+ChainId+'|'+ChainName
        return emit_string


# New_0014 ------------------------------------------------------
def New_0014(Place):
    """
    # Get counts of ExternalReference and
    # a listing of "system=value" combinations
    """
    return_emits = []
    core_nc = core_or_non_core(Place)

    ExternalRefattrib_string = ""
    ExternalRefSystem = ""
    system_set = set()

    ExternalReferenceList = Place.findall(ns+"ExternalReference")
    num_ext_ref = len(ExternalReferenceList)

    for ExternalReference in ExternalReferenceList:
        ExternalRefattrib = ExternalReference.attrib
        if 'system' in ExternalRefattrib.keys():
            ExternalRefSystem = ExternalRefattrib['system']
            system_set.add(ExternalRefSystem)

        ExternalRefattrib_string = ' '.join(system_set)

    # Take a closer look at Places that have more than 100 ExternalReference systems
    if num_ext_ref >= 100:
        CountryCode, PlaceId = CountryCode_PlaceID(Place)
        emit_string = 'New_0014c|100+ ExternalReference|'+str(num_ext_ref)+'|'+PlaceId
        return_emits.append(emit_string)

    emit_string = 'New_0014a|Number of ExternalReference = '+str(num_ext_ref)
    return_emits.append(emit_string)
    emit_string = 'New_0014b|"system" combinations|'+ExternalRefattrib_string+'|'+core_nc
    return_emits.append(emit_string)

    ##    # Get PlaceIds to investigate the Places that don't seem to have an ExternalReference
    ##    if num_ext_ref == 0:
    ##        CountryCode, PlaceId = CountryCode_PlaceID(Place)
    ##        emit_string = 'New_0014c|No ExternalReference|'+PlaceId+'|'+core_nc
    ##        return_emits.append(emit_string)
    return return_emits


# New_0015 ------------------------------------------------------
def New_0015(Place, id):
    """Use lxml tostring() in order to return an entire Place (in a single row) by querying a PlaceID"""
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    # Now with the ability to query multiple PlaceIDs at a time:
    if ',' in id:
        id_list = id.split(',')
        for id in id_list:
            if PlaceId == id:
                # returnPlace = etree.tostring(Place) <-- this produces HTML entities!
                returnPlace = etree.tostring(Place, encoding='UTF-8')
                return_emits.append(returnPlace)
    else:
        if PlaceId == id:
            returnPlace = etree.tostring(Place, encoding='UTF-8')
            return_emits.append(returnPlace)
    return return_emits


# New_0016 ------------------------------------------------------
def New_0016(Place):
    """
    # Counts of "Bound to PA"
    # Return count of PA_BINDING records that have <AdditionalData key="RequestForPA"> where
    # the URL contains '/pointBinding/bind/pointaddress/' as opposed to '/pointBinding/bind/link/'
    """

    LocationList = Place.findall(ns+"Location")
    for Location in LocationList:
        LocationAttributes = Location.attrib

        # Location supplier = 'PA_BINDING'
        if LocationAttributes['supplier'] == 'PA_BINDING':
            # New column "Bound using PA" - <AdditionalData key="RequestForPA">
            Bound_to_PA = ''
            AdditionalData_list = Location.findall(ns+"AdditionalData")
            for AdditionalData in AdditionalData_list:
                try:
                    AdditionalData_attrib = AdditionalData.attrib
                    if 'key' in AdditionalData_attrib.keys():
                        if AdditionalData_attrib['key'] == 'RequestForPA':
                            RequestForPA = AdditionalData.text
                            if '/pointBinding/bind/pointaddress/' in RequestForPA:
                                Bound_to_PA = "Bound by PA"
                except:
                    pass

    emit_string = 'New_0016|'+Bound_to_PA
    return emit_string


# New_0017 ------------------------------------------------------
def New_0017(Place):
    """
    # ErrorMessage Counts
    # Return count of PA_RESOLVING records that have <AdditionalData key="ErrorMessage">
    """
    return_emits = []
    LocationList = Place.findall(ns+"Location")
    for Location in LocationList:
        LocationAttributes = Location.attrib

        # Location supplier = 'PA_RESOLVING'
        if LocationAttributes['supplier'] == 'PA_RESOLVING':
            # New column "ErrorMessage" - <AdditionalData key="ErrorMessage">
            ErrorMessage = ''
            AdditionalData_list = Location.findall(ns+"AdditionalData")
            for AdditionalData in AdditionalData_list:
                try:
                    AdditionalData_attrib = AdditionalData.attrib
                    if 'key' in AdditionalData_attrib.keys():
                        if AdditionalData_attrib['key'] == 'ErrorMessage':
                            ErrorMessage = AdditionalData.text
                            emit_string = 'New_0017a|Has Error'
                            return_emits.append(emit_string)
                            emit_string = 'New_0017b|'+ErrorMessage
                            return_emits.append(emit_string)
                        else:
                            emit_string = 'New_0017a|None'
                            return_emits.append(emit_string)
                except:
                    pass

    return return_emits


# New_0018 ------------------------------------------------------
def New_0018(Place):
    """
    # Get counts of Prefix and Suffix, core vs. non-core
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Prefix
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Suffix
    """
    Core_POI = core_or_non_core(Place)
    return_emits = []
    suffix = ''; prefix = ''
    try:
        Suffix = Place.find(ns+"Suffix")
        suffix = Suffix.text
    except:
        pass
    try:
        Prefix = Place.find(ns+"Prefix")
        prefix = Prefix.text
    except:
        pass

    if suffix:
        emit_string = Core_POI+'|Has a Suffix'
        return_emits.append(emit_string)
    else:
        emit_string = Core_POI+'|No Suffix'
        return_emits.append(emit_string)
    if prefix:
        emit_string = Core_POI+'|Has a Prefix'
        return_emits.append(emit_string)
    else:
        emit_string = Core_POI+'|No Prefix'
        return_emits.append(emit_string)
    emit_string = 'Total number of Places:'
    return_emits.append(emit_string)
    emit_string = 'Total|'+Core_POI
    return_emits.append(emit_string)
    return return_emits


# New_0019 ------------------------------------------------------
def New_0019(Place):
    """
    # Output Prefix and Suffix, core vs. non-core
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Prefix
    # xpath: PlaceList/Place/LocationList/Location/Address/ParsedList/Parsed/Suffix
    """
    Core_POI = core_or_non_core(Place)
    emit_string = ''
    suffix = ''; prefix = ''
    try:
        Suffix = Place.find(ns+"Suffix")
        suffix = Suffix.text
    except:
        pass
    try:
        Prefix = Place.find(ns+"Prefix")
        prefix = Prefix.text
    except:
        pass

    emit_string = Core_POI+'|Prefix|'+prefix+'|Suffix|'+suffix
    return emit_string


def return_string_indices(main_string, sub_string):
    return [i for i, y in enumerate(main_string) if y == sub_string]

# New_0020 ------------------------------------------------------
def New_0020(Place):
    """
    # Find all BaseText that have HTML entities.
    # i.e. &#1053;&#1072;&#1088;&#1086;&#1076;&#1085;&#1072;
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    return_emits = []
    Place_string = etree.tostring(Place, encoding='UTF-8')
    emit_string = ''
    entity_begin = '&#'
    closing_brace = '>'
    opening_brace = '<'
    if entity_begin in Place_string:
        found_indices = return_string_indices(Place_string, '&')
        for fi in found_indices:
            elem_begin = 0
            if Place_string[fi-1] == closing_brace and Place_string[fi+1] == '#':
                string_slice = Place_string[fi-300:fi+20]                           # 100 chars before and 25 chars after
                elem_begin = string_slice.rfind(opening_brace)                      # find the right most occurence of opening brace
                if elem_begin != 0:
                    string_slice = string_slice[elem_begin:]                        # shorten the slice to just the element name at the beginning

                #focus on <BaseText where type="OFFICIAL"
                target_elem = string_slice.split('>')[0]
                if '<BaseText' in target_elem and 'type="OFFICIAL"' in target_elem:
                    emit_string = 'New_0020|'+CountryCode+'|BaseText'
                    return_emits.append(emit_string)

    return return_emits


# New_0021 ------------------------------------------------------
def New_0021(Place):
    """
    # Get stats of core vs. non-core, and
    # core vs. non-core per country code
    """
    return_emits = []
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    Core_POI = core_or_non_core(Place)

    emit_string = "Core|"+Core_POI
    return_emits.append(emit_string)

    emit_string = 'New_0021|'+CountryCode+'|'+  Core_POI
    return_emits.append(emit_string)

    return return_emits


# New_0022 ------------------------------------------------------
def New_0022(Place):
    """
    # Get stats of NationalImportance
    # <Attribute key="NationalImportance">true</Attribute>
    """
    return_emits = []
    CountryCode, PlaceId = CountryCode_PlaceID(Place)

    ni = 0
    AdditionalAttributes = Place.findall(ns+"AdditionalAttribute")
    for AdditionalAttribute in AdditionalAttributes:
        AdditionalAttribute_type = AdditionalAttribute.attrib
        if "attributeType" in AdditionalAttribute_type.keys():
            if AdditionalAttribute_type["attributeType"] == 'OTHER':
                # Attributes are the children of AdditionalAttribute
                for Attribute in AdditionalAttribute:
                    Attribute_type = Attribute.attrib
                    Attribute_text = Attribute.text
                    if Attribute_type["key"] == 'NationalImportance' and Attribute_text == "true":
                        ni = 1
                        break

    if ni == 1:
        emit_string = "NationalImportance=true"
        return_emits.append(emit_string)
    else:
    ##        emit_string = "Not NationalImportance|"+PlaceId
    ##        return_emits.append(emit_string)
        emit_string = "Not NationalImportance"
        return_emits.append(emit_string)
    return return_emits


# New_0023 ------------------------------------------------------
def New_0023(Place):
    """
    # Find all BaseText that have HTML entities.
    # i.e. &#1053;&#1072;&#1088;&#1086;&#1076;&#1085;&#1072;
    Emit PlaceIds, POI_PVIDs, Country Code, and the HTML entities
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)

    # Get POI_PVID
    try:
        ExternalReferenceList = Place.findall(ns+"ExternalReference")
        ExternalRefSystem = ""
        POI_PVID = ""
        for ExternalReference in ExternalReferenceList:
            ExternalRefattrib = ExternalReference.attrib
            if 'system' in ExternalRefattrib.keys():
                ExternalRefSystem = ExternalRefattrib['system']
                if ExternalRefSystem == "corepoixml":
                    ExternalReferenceID = ExternalReference.find(ns+"ExternalReferenceID")
                    ExternalReferenceID_attrib = ExternalReferenceID.attrib
                    if 'type' in ExternalReferenceID_attrib.keys():
                        ExternalReftype = ExternalReferenceID_attrib['type']
                    if ExternalReftype == "SUPPLIER_POIID":
                        POI_PVID = ExternalReferenceID.text
    except:
        pass

    return_emits = []
    Place_string = etree.tostring(Place, encoding='UTF-8')
    emit_string = ''
    entity_begin = '&#'
    closing_brace = '>'
    opening_brace = '<'
    if entity_begin in Place_string:
        found_indices = return_string_indices(Place_string, '&')
        for fi in found_indices:
            elem_begin = 0
            if Place_string[fi-1] == closing_brace and Place_string[fi+1] == '#':
                string_slice = Place_string[fi-300:fi+20]                           # 100 chars before and 25 chars after
                elem_begin = string_slice.rfind(opening_brace)                      # find the right most occurence of opening brace
                if elem_begin != 0:
                    string_slice = string_slice[elem_begin:]                        # shorten the slice to just the element name at the beginning

                #focus on <BaseText where type="OFFICIAL"
                target_elem = string_slice.split('>')[0]
                if '<BaseText' in target_elem and 'type="OFFICIAL"' in target_elem:
                    emit_string = PlaceId+'|'+POI_PVID+'|'+CountryCode+'|'+string_slice
                    return_emits.append(emit_string)

    return return_emits


# TQS_0001 ------------------------------------------------------
def TQS_0001(Place):
    """
    # TQS: Get counts of QualityLevel" 1, 2, 3, 4, 5..
    # <QualityLevel>4</QualityLevel>
    """
    return_emits = []
    QL = Place.find(ns+"QualityLevel")
    QualityLevel = QL.text
    emit_string = 'QualityLevel|'+QualityLevel
    return_emits.append(emit_string)
    emit_string = 'Total'
    return_emits.append(emit_string)
    return return_emits


# TQS_0002 ------------------------------------------------------
def TQS_0002(Place):
    """
    # TQS: Get PlaceId|QualityLevel
    # <QualityLevel>4</QualityLevel>
    # Goal is to get 25 delta buckets: 1 to 1, 1 to 2, ... 5 to 5
    # Run twice, once for with and once for without and then use Python on the output to create the buckets
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    QL = Place.find(ns+"QualityLevel")
    QualityLevel = QL.text
    emit_string = PlaceId+'|'+QualityLevel
    return emit_string


# TQS_0003 ------------------------------------------------------
def TQS_0003(Place):
    """
    # TQS: Get counts (core vs. non-core) of QualityLevel" 1, 2, 3, 4, 5..
    # <QualityLevel>4</QualityLevel>
    """
    return_emits = []
    Core_POI = core_or_non_core(Place)
    QualityLevel = ''
    try:
        QualityLevel = Place.find(ns+"QualityLevel").text
    except:
        pass
    emit_string = 'QualityLevel|'+QualityLevel
    return_emits.append(emit_string)
    emit_string = 'QualityLevel|'+Core_POI+'|'+QualityLevel
    return_emits.append(emit_string)
    emit_string = 'Total'
    return_emits.append(emit_string)
    emit_string = 'Total|'+Core_POI
    return_emits.append(emit_string)
    return return_emits


# TQS_0004 ------------------------------------------------------
def TQS_0004(Place):
    """
    # TQS: Get PlaceId|Core|QualityLevel
    # <QualityLevel>4</QualityLevel>
    # Goal is to get 25 delta buckets: 1 to 1, 1 to 2, ... 5 to 5
    # Run twice, once for with and once for without and then use Python on the output to create the buckets
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    Core_POI = core_or_non_core(Place)
    QL = Place.find(ns+"QualityLevel")
    QualityLevel = QL.text
    emit_string = PlaceId+'|'+Core_POI+'|'+QualityLevel
    return emit_string


# TQS_0005 ------------------------------------------------------
def TQS_0005(Place):
    """
    # TQS: Get PlaceId|Core|QualityLevel|Category (first category in list)
    # <QualityLevel>4</QualityLevel>
    # Goal is to get 25 delta buckets: 1 to 1, 1 to 2, ... 5 to 5
    # Run twice, once for with and once for without and then use Python on the output to create the buckets
    """
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    Core_POI = core_or_non_core(Place)
    try:
        QualityLevel = Place.find(ns+"QualityLevel").text
    except:
        pass

    # Get category
    CategoryNameList = []
    CategoryNames = Place.findall(ns+"CategoryName")
    for Category in CategoryNames:
        Name = Category.find(ns+"Text").text
        CategoryNameList.append(Name)
    try:
        FirstCategory = CategoryNameList[0]
    except:
        FirstCategory = "None"

    emit_string = PlaceId+'|'+Core_POI+'|'+QualityLevel+'|'+FirstCategory
    return emit_string


# TQS_0006 ------------------------------------------------------
def TQS_0006(Place):
    """
    # TQS: Compare QualityLevel to overallScore and count instances where the scores don't match
    <QualityLevel>5</QualityLevel>
    <AdditionalAttribute attributeType="QUALITY_SCORING">
        <Attribute key="overallScore">4</Attribute>
    """
    return_emits = []
    Core_POI = core_or_non_core(Place)
    QL = Place.find(ns+"QualityLevel")
    QualityLevel = QL.text
    if QualityLevel:
        emit_string = 'QualityLevel Total'
        return_emits.append(emit_string)

    overallScore = "None" # default value
    try:
        AdditionalAttributes = Place.findall(ns+"AdditionalAttribute")
        for AddAtt in AdditionalAttributes:
            AdditionalAttribute = AddAtt.attrib
            if AdditionalAttribute['attributeType'] == "QUALITY_SCORING":
                Attributes = Place.findall(ns+"Attribute")
                for att in Attributes:
                    att_dict = att.attrib
                    if att_dict["key"] == "overallScore":
                        overallScore = att.text
    except:
        pass

    if overallScore != 'None':
        emit_string = 'overallScore Total'
        return_emits.append(emit_string)

    if QualityLevel != overallScore:
        emit_string = Core_POI+'|'+QualityLevel+'|'+overallScore
        return_emits.append(emit_string)
        emit_string = Core_POI+"|Scores DON'T match"
        return_emits.append(emit_string)
    else:
        emit_string = Core_POI+"|Scores match"
        return_emits.append(emit_string)
    return return_emits


# TQS_0007 ------------------------------------------------------
def TQS_0007(Place):
    """
    # TQS: Compare QualityLevel to overallScore and count instances where the scores don't match
    <QualityLevel>5</QualityLevel>
    <AdditionalAttribute attributeType="QUALITY_SCORING">
        <Attribute key="overallScore">4</Attribute>
    # Output PlaceIds for Places that don't have either QualityLevel or overallScore
    """
    return_emits = []
    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    Core_POI = core_or_non_core(Place)
    QualityLevel = ''
    try:
        QualityLevel = Place.find(ns+"QualityLevel").text
        emit_string = 'QualityLevel Total'      # This number is incorrect for some reason
        return_emits.append(emit_string)
    except:
        emit_string = 'QualityLevel Problem'
        return_emits.append(emit_string)
    if not QualityLevel:
        emit_string = 'Missing QualityLevel|'+PlaceId+'|'+Core_POI
        return_emits.append(emit_string)

    overallScore = "None" # default value
    try:
        AdditionalAttributes = Place.findall(ns+"AdditionalAttribute")
        for AddAtt in AdditionalAttributes:
            AdditionalAttribute = AddAtt.attrib
            if AdditionalAttribute['attributeType'] == "QUALITY_SCORING":
                Attributes = Place.findall(ns+"Attribute")
                for att in Attributes:
                    att_dict = att.attrib
                    if att_dict["key"] == "overallScore":
                        overallScore = att.text
    except:
        pass

    if overallScore == "None":
        emit_string = 'Missing overallScore|'+PlaceId+'|'+Core_POI
        return_emits.append(emit_string)
    else:
        emit_string = 'overallScore Total'  # This number is incorrect for some reason
        return_emits.append(emit_string)
    return return_emits

# TQS_0008 ------------------------------------------------------
def TQS_0008(Place):
    """
    # TQS: Use lxml tostring() and search for the text '<QualityLevel>'
    # <QualityLevel>4</QualityLevel>
    Also look for the string 'overallScore'
    """
    return_emits = []
    stringified_Place = etree.tostring(Place)
    if '<QualityLevel>' in stringified_Place:
        emit_string = 'QualityLevel Total'
        return_emits.append(emit_string)
    if 'overallScore' in stringified_Place:
        emit_string = 'overallScore Total'
        return_emits.append(emit_string)
    return return_emits


# TQS_0009 ------------------------------------------------------
def TQS_0009(Place):
    """
    # TQS: For every Place, if a CategoryId is in this list, then emit 1 aggregated count
    # Get counts (core vs. non-core) of QualityLevel" 1, 2, 3, 4, 5..
    # <QualityLevel>4</QualityLevel>
    # CategoryId|Core|QL i.e. 100-1000-0000|Non-Core|4
    """

    important_categories = set(['100-1000-0000', '100-1000-0001', '100-1000-0002', '100-1000-0003', '100-1000-0004', '100-1000-0005', '100-1000-0006', '100-1000-0007', '100-1000-0008',
    '100-1000-0009', '100-1000-0230', '100-1100-0000', '100-1100-0010', '100-1100-0331', '200-2000-0000', '200-2000-0011', '200-2000-0012', '200-2000-0013', '200-2000-0014',
    '200-2000-0015', '200-2000-0016', '200-2000-0017', '200-2000-0018', '200-2000-0306', '200-2000-0368', '200-2100-0019', '200-2200-0000', '200-2200-0020', '200-2300-0000',
    '200-2300-0021', '300-3000-0000', '300-3000-0023', '300-3000-0024', '300-3000-0025', '300-3000-0065', '300-3000-0350', '300-3000-0351', '300-3100-0000', '300-3100-0026',
    '300-3100-0027', '300-3100-0028', '300-3100-0029', '300-3200-0000', '300-3200-0030', '300-3200-0031', '300-3200-0032', '300-3200-0033', '300-3200-0034', '300-3200-0309',
    '300-3200-0375', '400-4000-4580', '400-4000-4581', '400-4000-4582', '400-4100-0035', '400-4100-0036', '400-4100-0038', '400-4100-0039', '400-4100-0041', '400-4100-0044',
    '400-4100-0045', '400-4100-0046', '400-4300-0000', '400-4300-0199', '400-4300-0200', '400-4300-0201', '400-4300-0202', '400-4300-0308', '500-5000-0000', '500-5000-0053',
    '500-5000-0054', '500-5100-0000', '500-5100-0055', '500-5100-0056', '500-5100-0057', '500-5100-0058', '500-5100-0059', '500-5100-0060', '550-5510-0000', '550-5510-0202',
    '550-5510-0203', '550-5510-0204', '550-5510-0205', '550-5510-0206', '550-5510-0227', '550-5510-0242', '550-5510-0243', '550-5520-0000', '550-5520-0207', '550-5520-0208',
    '550-5520-0209', '550-5520-0210', '550-5520-0211', '550-5520-0212', '550-5520-0228', '550-5520-0357', '600-6000-0000', '600-6000-0061', '600-6100-0062', '600-6200-0063',
    '600-6300-0064', '600-6300-0066', '600-6300-0067', '600-6300-0068', '600-6300-0244', '600-6300-0245', '600-6300-0363', '600-6300-0364', '600-6400-0000', '600-6400-0069',
    '600-6400-0070', '600-6500-0072', '600-6500-0073', '600-6500-0074', '600-6500-0075', '600-6500-0076', '600-6500-0333', '600-6600-0000', '600-6600-0077', '600-6600-0078',
    '600-6600-0079', '600-6600-0080', '600-6600-0082', '600-6600-0083', '600-6600-0084', '600-6600-0085', '600-6600-0310', '600-6600-0319', '600-6700-0000', '600-6700-0087',
    '600-6800-0000', '600-6800-0089', '600-6800-0090', '600-6800-0091', '600-6800-0092', '600-6800-0093', '600-6900-0000', '600-6900-0094', '600-6900-0095', '600-6900-0096',
    '600-6900-0097', '600-6900-0098', '600-6900-0099', '600-6900-0100', '600-6900-0101', '600-6900-0102', '600-6900-0103', '600-6900-0104', '600-6900-0105', '600-6900-0106',
    '600-6900-0246', '600-6900-0247', '600-6900-0248', '600-6900-0250', '600-6900-0251', '600-6900-0305', '600-6900-0307', '600-6900-0355', '600-6900-0356', '600-6900-0358',
    '600-6900-0388', '600-6900-0389', '600-6900-0390', '600-6900-0391', '600-6900-0392', '600-6900-0393', '600-6900-0394', '600-6900-0395', '600-6900-0396', '600-6900-0397',
    '600-6900-0398', '600-6950-0000', '600-6950-0399', '600-6950-0400', '600-6950-0401', '700-7000-0107', '700-7010-0108', '700-7050-0109', '700-7050-0110', '700-7300-0111',
    '700-7300-0113', '700-7300-0280', '700-7450-0114', '700-7460-0115', '700-7600-0000', '700-7600-0116', '700-7800-0118', '700-7800-0119', '700-7800-0120', '700-7850-0000',
    '700-7850-0121', '700-7850-0122', '700-7850-0123', '700-7850-0124', '700-7850-0125', '700-7850-0126', '700-7850-0127', '700-7850-0128', '700-7850-0129', '700-7851-0117',
    '700-7900-0132', '800-8000-0000', '800-8000-0154', '800-8000-0155', '800-8000-0156', '800-8000-0157', '800-8000-0158', '800-8000-0159', '800-8000-0161', '800-8000-0162',
    '800-8000-0325', '800-8000-0340', '800-8000-0341', '800-8000-0367', '800-8100-0000', '800-8100-0163', '800-8100-0164', '800-8100-0165', '800-8100-0168', '800-8100-0169',
    '800-8100-0170', '800-8100-0171', '800-8200-0000', '800-8200-0173', '800-8200-0174', '800-8200-0295', '800-8200-0360', '800-8200-0361', '800-8200-0362', '800-8300-0000',
    '800-8300-0175', '800-8400-0000', '800-8400-0176', '800-8500-0000', '800-8500-0177', '800-8500-0178', '800-8500-0179', '800-8500-0315', '800-8600-0000', '800-8600-0180',
    '800-8600-0181', '800-8600-0182', '800-8600-0183', '800-8600-0184', '800-8600-0185', '800-8600-0186', '800-8600-0187', '800-8600-0188', '800-8600-0189', '800-8600-0190',
    '800-8600-0191', '800-8600-0192', '800-8600-0193', '800-8600-0194', '800-8600-0195', '800-8600-0196', '800-8600-0197', '800-8600-0199', '800-8600-0200', '800-8600-0314',
    '800-8600-0316', '800-8600-0376', '800-8600-0377', '800-8600-0381', '800-8700-0166', '800-8700-0167', '900-9200-0219', '900-9200-0220'])

    return_emits = []
    Core_POI = core_or_non_core(Place)
    QualityLevel = ''
    try:
        QualityLevel = Place.find(ns+"QualityLevel").text
    except:
        pass

    # Get category
    CategoryIds = []
    CategoryIdList = Place.findall(ns+"CategoryId")
    for Category in CategoryIdList:
        CatId = Category.text
        CategoryIds.append(CatId)

    if set(CategoryIds).intersection(important_categories):
        emit_string = 'Priority QualityLevel|'+QualityLevel
        return_emits.append(emit_string)
        emit_string = 'Priority QualityLevel|'+Core_POI+'|'+QualityLevel
        return_emits.append(emit_string)
        emit_string = 'Priority Total'
        return_emits.append(emit_string)
        emit_string = 'Priority Total|'+Core_POI
        return_emits.append(emit_string)
    else:
        emit_string = 'Other QualityLevel|'+QualityLevel
        return_emits.append(emit_string)
        emit_string = 'Other QualityLevel|'+Core_POI+'|'+QualityLevel
        return_emits.append(emit_string)
        emit_string = 'Other Total'
        return_emits.append(emit_string)
        emit_string = 'Other Total|'+Core_POI
        return_emits.append(emit_string)
    return return_emits


# TQS_0010 ------------------------------------------------------
def TQS_0010(Place):
    """
    # TQS: USA with TQS --> subset of categories found in USA_Core_POI_categories --> subset of Non-core
    # Place Id, Place Name, Address, Phone, delimited categories,
    # TQS model scores: Place, Open, Name, Address, Phone, TQS model version,
    # PDS category labels (corenames) and ids (coreid1) that correspond to the matchng USA_Core_POI_category
    """

    USA_Core_POI_categories = set([ '100-1000-0000', '100-1000-0001', '100-1000-0002', '100-1000-0003', '100-1000-0004', '100-1000-0005', '100-1000-0006', '100-1000-0007',
                                    '100-1000-0008', '100-1000-0009', '100-1000-0230', '100-1100-0000', '100-1100-0010', '200-2100-0019', '200-2200-0020', '200-2300-0021',
                                    '300-3000-0023', '300-3000-0024', '300-3000-0025', '300-3000-0065', '300-3100-0000', '300-3100-0026', '300-3100-0027', '300-3100-0028',
                                    '300-3100-0029', '400-4000-4581', '400-4000-4582', '400-4100-0035', '400-4100-0036', '400-4100-0037', '400-4100-0038', '400-4100-0039',
                                    '400-4100-0041', '400-4100-0044', '400-4100-0045', '400-4100-0046', '400-4300-0000', '400-4300-0199', '400-4300-0200', '400-4300-0201',
                                    '400-4300-0202', '400-4300-0308', '500-5000-0000', '500-5000-0053', '500-5000-0054', '500-5100-0056', '550-5510-0202', '550-5510-0203',
                                    '550-5510-0204', '550-5510-0205', '550-5510-0358', '550-5510-0374', '550-5520-0207', '550-5520-0208', '550-5520-0209', '550-5520-0210',
                                    '550-5520-0211', '550-5520-0212', '550-5520-0228', '550-5520-0357', '600-6000-0061', '600-6100-0062', '600-6200-0063', '600-6300-0064',
                                    '600-6300-0066', '600-6300-0244', '600-6300-0245', '600-6300-0363', '600-6300-0364', '600-6400-0000', '600-6400-0069', '600-6400-0070',
                                    '600-6500-0072', '600-6500-0073', '600-6500-0074', '600-6600-0078', '600-6600-0310', '600-6700-0087', '600-6800-0000', '600-6800-0089',
                                    '600-6800-0090', '600-6800-0091', '600-6800-0092', '600-6900-0094', '600-6900-0388', '600-6900-0389', '600-6900-0390', '600-6900-0391',
                                    '600-6900-0392', '600-6900-0393', '600-6900-0246', '600-6900-0394', '600-6900-0395', '600-6900-0396', '600-6900-0397', '600-6900-0398',
                                    '600-6900-0095', '600-6900-0096', '600-6900-0097', '600-6900-0098', '600-6900-0247', '600-6900-0247', '600-6900-0248', '600-6900-0307',
                                    '600-6900-0358', '700-7000-0107', '700-7300-0111', '700-7400-0286', '700-7450-0114', '700-7460-0115', '700-7600-0116', '700-7800-0118',
                                    '700-7800-0120', '700-7850-0000', '700-7850-0121', '700-7850-0122', '700-7850-0123', '700-7850-0124', '700-7850-0125', '700-7850-0126',
                                    '700-7850-0127', '700-7850-0129', '700-7851-0117', '700-7900-0130', '700-7900-0131', '700-7900-0132', '800-8000-0159', '800-8000-0325',
                                    '800-8100-0163', '800-8100-0164', '800-8100-0165', '800-8100-0169', '800-8100-0170', '800-8200-0173', '800-8200-0174', '800-8300-0175',
                                    '800-8400-0176', '800-8500-0177', '800-8500-0178', '800-8500-0179', '800-8500-0315', '800-8600-0180', '800-8600-0193', '900-9200-0219'])

    corename_lookups = {'600-6800-0000': 'Clothing Store', '550-5520-0207': 'Amusement Park', '400-4300-0000': 'Rest Area', '600-6900-0307': 'Specialty Store',
                        '550-5520-0209': 'Animal Park', '550-5520-0208': 'Animal Park', '600-6900-0389': 'Sporting Goods Store', '600-6900-0388': 'Sporting Goods Store',
                        '550-5510-0374': 'Park/Recreation Area', '100-1000-0230': 'Restaurant', '100-1000-0002': 'Restaurant', '300-3000-0065': 'Winery',
                        '600-6900-0394': 'Sporting Goods Store', '600-6900-0095': 'Office Supply & Services Store', '600-6900-0094': 'Sporting Goods Store',
                        '500-5100-0056': 'Campground', '200-2200-0020': 'Performing Arts', '600-6900-0396': 'Sporting Goods Store', '600-6900-0098': 'Specialty Store',
                        '550-5520-0210': 'Animal Park', '550-5520-0211': 'Animal Park', '550-5520-0212': 'Ski Resort', '600-6900-0397': 'Sporting Goods Store',
                        '600-6800-0090': 'Clothing Store', '600-6800-0091': 'Clothing Store', '600-6800-0092': 'Clothing Store', '600-6900-0390': 'Sporting Goods Store',
                        '700-7450-0114': 'Post Office', '600-6900-0391': 'Sporting Goods Store', '100-1000-0003': 'Restaurant', '600-6600-0078': 'Home Specialty Store',
                        '100-1000-0001': 'Restaurant', '100-1000-0000': 'Restaurant', '100-1000-0007': 'Restaurant', '100-1000-0006': 'Restaurant', '100-1000-0005': 'Restaurant',
                        '100-1000-0004': 'Restaurant', '100-1000-0009': 'Restaurant', '100-1000-0008': 'Restaurant', '600-6900-0398': 'Sporting Goods Store',
                        '600-6800-0089': 'Clothing Store', '200-2300-0021': 'Casino', '700-7800-0120': 'Motorcycle Dealership', '200-2100-0019': 'Cinema',
                        '600-6900-0246': 'Sporting Goods Store', '600-6900-0247': 'Indoor Market', '700-7600-0116': 'Petrol/Gasoline Station', '600-6900-0248': 'Specialty Store',
                        '600-6500-0073': 'Mobile Retailer', '500-5000-0053': 'Hotel', '500-5000-0054': 'Hotel', '600-6500-0074': 'Mobile Service Center',
                        '800-8300-0175': 'Library', '800-8100-0163': 'City Hall', '600-6300-0364': 'Specialty Store', '600-6300-0363': 'Specialty Store',
                        '600-6200-0063': 'Department Store', '800-8400-0176': 'Convention/Exhibition Centre', '800-8500-0315': 'Parking Lot', '400-4100-0035': 'Train Station',
                        '400-4100-0037': 'Underground Train/Subway', '400-4100-0036': 'Bus Station', '600-6900-0395': 'Sporting Goods Store',
                        '400-4100-0039': 'Underground Train/Subway', '400-4100-0038': 'Underground Train/Subway', '300-3100-0026': 'Museum', '300-3100-0027': 'Museum',
                        '600-6400-0000': 'Pharmacy / Drug Store', '700-7900-0131': 'Truck Parking', '300-3100-0028': 'Museum', '300-3100-0029': 'Museum',
                        '800-8100-0165': 'Military Base', '800-8100-0164': 'Embassy', '700-7851-0117': 'Rental Car Agency', '600-6700-0087': 'Bookstore',
                        '800-8100-0169': 'Civic/Community Centre', '400-4100-0041': 'Transportation Service', '400-4100-0044': 'Ferry Terminal',
                        '400-4100-0045': 'Ferry Terminal', '400-4100-0046': 'Ferry Terminal', '700-7400-0286': 'School', '600-6100-0062': 'Shopping',
                        '400-4300-0199': 'Rest Area', '400-4300-0200': 'Rest Area', '400-4300-0201': 'Rest Area', '400-4300-0202': 'Rest Area',
                        '600-6300-0064': 'Specialty Store', '600-6300-0066': 'Grocery Store', '600-6900-0358': 'Specialty Store', '700-7800-0118': 'Auto Dealerships',
                        '700-7850-0000': 'Auto Service & Maintenance', '700-7850-0129': 'Automobile Club', '700-7850-0125': 'Auto Service & Maintenance',
                        '700-7850-0124': 'Auto Service & Maintenance', '700-7850-0127': 'Auto Service & Maintenance', '700-7850-0126': 'Auto Service & Maintenance',
                        '700-7850-0121': 'Auto Service & Maintenance', '700-7850-0123': 'Auto Service & Maintenance', '700-7850-0122': 'Auto Service & Maintenance',
                        '800-8100-0170': 'Court House', '800-8000-0159': 'Hospital', '700-7000-0107': 'Bank', '500-5000-0000': 'Hotel', '900-9200-0219': 'Marina',
                        '800-8600-0180': 'Sports Complex', '600-6900-0097': 'Specialty Store', '700-7900-0130': 'Truck Dealership', '700-7300-0111': 'Police Station',
                        '700-7900-0132': 'Truck Stop/Plaza', '600-6900-0096': 'Specialty Store', '300-3100-0000': 'Museum', '550-5520-0228': 'Animal Park',
                        '600-6600-0310': 'Home Improvement & Hardware', '800-8600-0193': 'Golf Course', '600-6900-0392': 'Sporting Goods Store',
                        '550-5510-0358': 'Park/Recreation Area', '800-8500-0178': 'Parking Lot', '800-8500-0179': 'Park & Ride', '550-5520-0357': 'Amusement Park',
                        '800-8500-0177': 'Parking Garage/House', '600-6400-0069': 'Pharmacy / Drug Store', '400-4000-4582': 'Airport',
                        '600-6500-0072': 'Consumer Electronics Store', '400-4000-4581': 'Airport', '300-3000-0023': 'Tourist Attraction',
                        '600-6900-0393': 'Sporting Goods Store', '300-3000-0025': 'Historical Monument', '300-3000-0024': 'Tourist Attraction',
                        '100-1100-0010': 'Coffee Shop', '400-4300-0308': 'Rest Area', '600-6300-0244': 'Specialty Store', '600-6300-0245': 'Specialty Store',
                        '700-7460-0115': 'Tourist Information', '600-6000-0061': 'Convenience Store', '800-8200-0173': 'Higher Education', '800-8000-0325': 'Hospital',
                        '800-8200-0174': 'School', '600-6400-0070': 'Pharmacy / Drug Store', '550-5510-0204': 'Park/Recreation Area', '550-5510-0205': 'Park/Recreation Area',
                        '550-5510-0202': 'Park/Recreation Area', '550-5510-0203': 'Park/Recreation Area', '100-1100-0000': 'Coffee Shop'}

    coreid_lookup = {'600-6800-0000': '9537', '550-5520-0207': '7996', '400-4300-0000': '7897', '600-6900-0307': '9567', '550-5520-0209': '9718', '550-5520-0208': '9718',
                     '600-6900-0389': '9568', '600-6900-0388': '9568', '550-5510-0374': '7947', '100-1000-0230': '5800', '100-1000-0002': '5800', '300-3000-0065': '2084',
                     '600-6900-0394': '9568', '600-6900-0095': '9988', '600-6900-0094': '9568', '500-5100-0056': '9517', '200-2200-0020': '7929', '600-6900-0396': '9568',
                     '600-6900-0098': '9567', '550-5520-0210': '9718', '550-5520-0211': '9718', '550-5520-0212': '7012', '600-6900-0397': '9568', '600-6800-0090': '9537',
                     '600-6800-0091': '9537', '600-6800-0092': '9537', '600-6900-0390': '9568', '700-7450-0114': '9530', '600-6900-0391': '9568', '100-1000-0003': '5800',
                     '600-6600-0078': '9560', '100-1000-0001': '5800', '100-1000-0000': '5800', '100-1000-0007': '5800', '100-1000-0006': '5800', '100-1000-0005': '5800',
                     '100-1000-0004': '5800', '100-1000-0009': '5800', '100-1000-0008': '5800', '600-6900-0398': '9568', '600-6800-0089': '9537', '200-2300-0021': '7985',
                     '700-7800-0120': '5571', '200-2100-0019': '7832', '600-6900-0246': '9568', '600-6900-0247': '5400', '700-7600-0116': '5540', '600-6900-0248': '9567',
                     '600-6500-0073': '9987', '500-5000-0053': '7011', '500-5000-0054': '7011', '600-6500-0074': '9987', '800-8300-0175': '8231', '800-8100-0163': '9121',
                     '600-6300-0364': '9567', '600-6300-0363': '9567', '600-6200-0063': '9545', '800-8400-0176': '7990', '800-8500-0315': '7520', '400-4100-0035': '4013',
                     '400-4100-0037': '4100', '400-4100-0036': '4170', '600-6900-0395': '9568', '400-4100-0039': '4100', '400-4100-0038': '4100', '300-3100-0026': '8410',
                     '300-3100-0027': '8410', '600-6400-0000': '9565', '700-7900-0131': '9720', '300-3100-0028': '8410', '300-3100-0029': '8410', '800-8100-0165': '9715',
                     '800-8100-0164': '9993', '700-7851-0117': '7510', '600-6700-0087': '9995', '800-8100-0169': '7994', '400-4100-0041': '9593', '400-4100-0044': '4482',
                     '400-4100-0045': '4482', '400-4100-0046': '4482', '700-7400-0286': '8211', '600-6100-0062': '6512', '400-4300-0199': '7897', '400-4300-0200': '7897',
                     '400-4300-0201': '7897', '400-4300-0202': '7897', '600-6300-0064': '9567', '600-6300-0066': '5400', '600-6900-0358': '9567', '700-7800-0118': '5511',
                     '700-7850-0000': '7538', '700-7850-0129': '8699', '700-7850-0125': '7538', '700-7850-0124': '7538', '700-7850-0127': '7538', '700-7850-0126': '7538',
                     '700-7850-0121': '7538', '700-7850-0123': '7538', '700-7850-0122': '7538', '800-8100-0170': '9211', '800-8000-0159': '8060', '700-7000-0107': '6000',
                     '500-5000-0000': '7011', '900-9200-0219': '4493', '800-8600-0180': '7940', '600-6900-0097': '9567', '700-7900-0130': '9719', '700-7300-0111': '9221',
                     '700-7900-0132': '9522', '600-6900-0096': '9567', '300-3100-0000': '8410', '550-5520-0228': '9718', '600-6600-0310': '9986', '800-8600-0193': '7992',
                     '600-6900-0392': '9568', '550-5510-0358': '7947', '800-8500-0178': '7520', '800-8500-0179': '7522', '550-5520-0357': '7996', '800-8500-0177': '7521',
                     '600-6400-0069': '9565', '400-4000-4582': '4581', '600-6500-0072': '9987', '400-4000-4581': '4581', '300-3000-0023': '7999', '600-6900-0393': '9568',
                     '300-3000-0025': '5999', '300-3000-0024': '7999', '100-1100-0010': '9996', '400-4300-0308': '7897', '600-6300-0244': '9567', '600-6300-0245': '9567',
                     '700-7460-0115': '7389', '600-6000-0061': '9535', '800-8200-0173': '8200', '800-8000-0325': '8060', '800-8200-0174': '8211', '600-6400-0070': '9565',
                     '550-5510-0204': '7947', '550-5510-0205': '7947', '550-5510-0202': '7947', '550-5510-0203': '7947', '100-1100-0000': '9996'}

    CuisineID_lookup = {'252-000': '50', '380-000': '100', '800-066': '82', '800-067': '85', '800-064': '53', '800-065': '4', '800-062': '75',
                        '800-063': '76', '800-060': '41', '800-061': '74', '404-000': '95', '306-000': '18', '151-000': '29', '800-068': '86',
                        '800-069': '87', '211-000': '92', '401-000': '46', '501-000': '81', '373-000': '98', '311-034': '77', '402-000': '102',
                        '201-000': '3', '371-000': '30', '314-000': '35', '311-000': '47', '100-1000-0009': '27', '406-000': '83', '202-022': '8',
                        '202-023': '8', '204-000': '44', '202-021': '8', '202-024': '8', '202-025': '8', '207-000': '33', '307-000': '20',
                        '302-000': '6', '202-020': '8', '378-000': '54', '350-000': '42', '310-000': '49', 'None': '12', '103-000': '52',
                        '208-000': '104', '309-000': '25', '205-000': '14', '210-000': '103', '203-026': '88', '377-000': '40', '101-000': '1',
                        '101-001': '2', '101-002': '45', '101-003': '19', '101-004': '84', '504-000': '51', '400-000': '43', '303-000': '7',
                        '209-000': '80', '382-000': '26', '212-000': '37', '506-000': '51', '251-000': '97', '202-019': '8', '202-018': '8',
                        '152-000': '79', '202-012': '8', '202-017': '8', '202-016': '8', '202-015': '8', '202-014': '8', '308-000': '23',
                        '370-000': '26', '800-058': '73', '800-057': '58', '800-056': '48', '203-000': '10', '376-000': '38', '202-000': '8',
                        '206-000': '16', '150-000': '57', '375-000': '101', '500-000': '51', '305-000': '78', '379-000': '55', '403-000': '34',
                        '202-013': '8', '102-000': '11', '101-070': '91', '505-000': '51', '313-000': '39', '250-000': '36', '301-000': '5',
                        '304-000': '9', '800-079': '32', '800-078': '28', '800-071': '93', '800-073': '21', '800-075': '13', '800-074': '22',
                        '800-077': '15', '800-076': '89', '351-000': '56', '374-000': '99', '405-000': '96'}

    Core_POI = core_or_non_core(Place)
    emit_string = ''

    # Get category
    CategoryIds = []
    CategoryIdList = Place.findall(ns+"CategoryId")
    for Category in CategoryIdList:
        CatId = Category.text
        CategoryIds.append(CatId)

    if set(CategoryIds).intersection(USA_Core_POI_categories) and Core_POI == 'Non-Core':

        # core_category_name & coreid1
        core_cat_match = set(CategoryIds) & USA_Core_POI_categories
        if len(core_cat_match) > 1:
            core_category_names = []
            core_ids = []
            for cat in core_cat_match:
                core_category_name = corename_lookups[cat]
                core_category_names.append(core_category_name)
                coreid1 = coreid_lookup[cat]
                core_ids.append(coreid1)
            if len(set(core_category_names)) > 1:
                core_category_name = ";".join(core_category_names)
            if len(set(core_ids)) > 1:
                coreid1 = ";".join(core_ids)
        else:
            core_category_name = corename_lookups[list(core_cat_match)[0]]
            coreid1 = coreid_lookup[list(core_cat_match)[0]]

        # category
        if len(core_cat_match) > 1:
                category = ";".join(core_cat_match)
        else:
            category = list(core_cat_match)[0]

        CountryCode, PlaceId = CountryCode_PlaceID(Place)

        # Non-matching categories & CuisineID lookup
        CuisineIDList = []
        CuisineID = ''
        non_matching_cats = set(CategoryIds) - USA_Core_POI_categories
        if len(non_matching_cats) > 1:
            non_matching_cat = ";".join(non_matching_cats)

            # CuisineID
            for nmc in non_matching_cats:
                if nmc in CuisineID_lookup.keys():
                    CuisineIDList.append(CuisineID_lookup[nmc])

        elif len(non_matching_cats) == 1:
            non_matching_cat = list(non_matching_cats)[0]

            # CuisineID
            if non_matching_cat in CuisineID_lookup.keys():
                CuisineIDList.append(CuisineID_lookup[non_matching_cat])

        else:
            non_matching_cat= ''

        # CuisineID
        CuisineID = ''
        CuisineIDset = set(CuisineIDList)
        if CuisineIDset:
            if len(CuisineIDset) > 1:
                CuisineID = ";".join(CuisineIDset)
            else:
                CuisineID = list(CuisineIDset)[0]

        # Place name
        BaseTextList = Place.findall(ns+"BaseText")
        POIName = BaseTextList[0].text      # The first name
        POIName = POIName.replace('|', ':')

        AddDataList = Place.findall(ns+"AdditionalData")
        LocationList = Place.findall(ns+"Location")
        address_strings = []
        cities = []; states = []; postalcodes = []
        for Location in LocationList:

            # Create address strings that look like this:
            # 1509 Columbia Vista Dr, Point Roberts, WA 98281
            # address StreetName StreetType, city, state PostalCode
            try:
                StreetName = Location.find(ns+"BaseName").text
            except:
                StreetName = "None"
            try:
                city = Location.find(ns+"Level4").text
            except:
                # If city is not found at Level4, then try getting city via AdditionalData --> key="PA-Zone"
                for AdditionalData in AddDataList:
                    AdditionalData_attrib = AdditionalData.attrib
                    if 'key' in AdditionalData_attrib.keys():
                        if AdditionalData_attrib['key'] == 'PA-Zone':
                            city = AdditionalData.text
                            break
                        else:
                            city = "None"
            cities.append(city)
            try:
                PostalCode = Location.find(ns+"PostalCode").text
            except:
                PostalCode = "None"
            postalcodes.append(PostalCode)
            try:
                address = Location.find(ns+"HouseNumber").text
            except:
                address = "None"
            try:
                StreetType = Location.find(ns+"StreetType").text
            except:
                StreetType = "None"

            # Suffix and Prefix
            suffix = ''; prefix = ''
            try:
                Suffix = Location.find(ns+"Suffix")
                suffix = Suffix.text
            except:
                pass
            try:
                Prefix = Location.find(ns+"Prefix")
                prefix = Prefix.text
            except:
                pass

            # Try getting state abbreviation via AdditionalData --> key="state"
            for AdditionalData in AddDataList:
                AdditionalData_attrib = AdditionalData.attrib
                if 'key' in AdditionalData_attrib.keys():
                    if AdditionalData_attrib['key'] == 'state':
                        state = AdditionalData.text
                        break
                    else:
                        # otherwise, try getting the full state name via Level2
                        try:
                            state = Location.find(ns+"Level2").text
                        except:
                            state = "None"
            states.append(state)

            address_string = address+' '+StreetName+' '+StreetType
            address_string = address_string.replace('|', ':')
            address_strings.append(address_string)

        num_addresses = len(set(address_strings))
        if num_addresses > 1:
            address_string = ';'.join(address_strings)

        # In case there are more than one city, state, or postalcode
        num_cities = len(set(cities))
        if num_cities > 1:
            city = ';'.join(cities)

        num_states = len(set(states))
        if num_states > 1:
            state = ';'.join(states)

        num_postalcodes = len(set(postalcodes))
        if num_postalcodes > 1:
            PostalCode = ';'.join(postalcodes)

        # Check the number of cases that have more than one address
        #emit_string = 'TQS_0010|'+str(num_addresses)

        # Phone number
        try:
            phone = Place.find(ns+"StandardNumber").text
        except:
            phone = "None"

        # TQS Scores
        Attributes = Place.findall(ns+"Attribute")
        Attribute_dict = {}
        for Attribute in Attributes:
            Attribute_attrib = Attribute.attrib
            if 'key' in Attribute_attrib.keys():
                kv = Attribute_attrib['key']
                Attribute_dict[kv] = Attribute.text

        try:
            modelVersion = Attribute_dict['modelVersion']
        except:
            modelVersion = "None"
        try:
            overallScore = Attribute_dict['overallScore']
        except:
            overallScore = "None"
        try:
            isPlace = Attribute_dict['isPlace']
        except:
            isPlace = "None"
        try:
            isOpen = Attribute_dict['isOpen']
        except:
            isOpen = "None"
        try:
            isNameCorrect = Attribute_dict['isNameCorrect']
        except:
            isNameCorrect = "None"
        try:
            isAddressCorrect = Attribute_dict['isAddressCorrect']
        except:
            isAddressCorrect = "None"
        try:
            isPhoneCorrect = Attribute_dict['isPhoneCorrect']
        except:
            isPhoneCorrect = "None"

        # Chain IDs
        ChainId = ''
        ChainList = Place.findall(ns+"Chain")
        for Chain in ChainList:
            Text = Chain.find(ns+"Text")
            TextAttrib = Text.attrib
            # default="true" type="OFFICIAL"
            if 'default' in TextAttrib.keys() and 'type' in TextAttrib.keys():
                if TextAttrib['default'] == 'true' and TextAttrib['type'] == 'OFFICIAL':
                    ChainId = Chain.find(ns+"Id").text


        # ContactString URLs
        URL_indicators = ["http", "www", ".com", ".net", ".org", "://"]

        ContactStrings = Place.findall(ns+"ContactString")
        webURLs = []; URL = ''
        for ContactString in ContactStrings:
            cs = ContactString.text
            for URL_indicator in URL_indicators:
                if URL_indicator in cs:
                    cs = cs.strip('\n')
                    webURLs.append(cs)

        webURLset = set(webURLs)
        if len(webURLset) > 1:
            URL = "; ".join(webURLset)
        else:
            URL = list(webURLset)[0]

        # Header
        header = 'PlaceId|POIName|address_string|city|state|zip|country|phone|Matching PDSCATID|core_category_name|coreid1|Non-matching PDSCATID|modelVersion|overallScore|isPlace|isOpen|isNameCorrect|isAddressCorrect|isPhoneCorrect|ChainId|CuisineID|URLs'
        # emit string
        emit_string = PlaceId+'|'+POIName+'|'+address_string+'|'+city+'|'+state+'|'+PostalCode+'|'+CountryCode+'|'+phone+'|'+category+'|'+core_category_name+'|'+coreid1+'|'+non_matching_cat+'|'+modelVersion+'|'+overallScore+'|'+isPlace+'|'+isOpen+'|'+isNameCorrect+'|'+isAddressCorrect+'|'+isPhoneCorrect+'|'+ChainId+'|'+CuisineID+'|'+URL

        if emit_string:
            return emit_string


# TQS_0011 ------------------------------------------------------
def TQS_0011(Place):
    """
    # Count the number of URLs in contact strings of Non-Core Places for TQS
    xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=FAX)/ContactString
    """
    Core_POI = core_or_non_core(Place)
    if Core_POI == "Non-Core":
        ContactStrings = Place.findall(ns+"ContactString")
        for ContactString in ContactStrings:
            if "http" in ContactString or "www" or ".com" in ContactString:
                emit_string = 'TQS_0011|Has URL'
                break
            else:
                emit_string = 'TQS_0011|Does not have URL'
        return emit_string


# TQS_0012 ------------------------------------------------------
def TQS_0012(Place):
    """
    # For each Non-Core Place that has a category found in USA_Core_POI_categories,
    #output PlaceId and Contact String
    xpath: PlaceList/Place/Content/Base/ContactList/Contact(@type=FAX)/ContactString
    """

    USA_Core_POI_categories = set([ '100-1000-0000', '100-1000-0001', '100-1000-0002', '100-1000-0003', '100-1000-0004', '100-1000-0005', '100-1000-0006', '100-1000-0007',
                                    '100-1000-0008', '100-1000-0009', '100-1000-0230', '100-1100-0000', '100-1100-0010', '200-2100-0019', '200-2200-0020', '200-2300-0021',
                                    '300-3000-0023', '300-3000-0024', '300-3000-0025', '300-3000-0065', '300-3100-0000', '300-3100-0026', '300-3100-0027', '300-3100-0028',
                                    '300-3100-0029', '400-4000-4581', '400-4000-4582', '400-4100-0035', '400-4100-0036', '400-4100-0037', '400-4100-0038', '400-4100-0039',
                                    '400-4100-0041', '400-4100-0044', '400-4100-0045', '400-4100-0046', '400-4300-0000', '400-4300-0199', '400-4300-0200', '400-4300-0201',
                                    '400-4300-0202', '400-4300-0308', '500-5000-0000', '500-5000-0053', '500-5000-0054', '500-5100-0056', '550-5510-0202', '550-5510-0203',
                                    '550-5510-0204', '550-5510-0205', '550-5510-0358', '550-5510-0374', '550-5520-0207', '550-5520-0208', '550-5520-0209', '550-5520-0210',
                                    '550-5520-0211', '550-5520-0212', '550-5520-0228', '550-5520-0357', '600-6000-0061', '600-6100-0062', '600-6200-0063', '600-6300-0064',
                                    '600-6300-0066', '600-6300-0244', '600-6300-0245', '600-6300-0363', '600-6300-0364', '600-6400-0000', '600-6400-0069', '600-6400-0070',
                                    '600-6500-0072', '600-6500-0073', '600-6500-0074', '600-6600-0078', '600-6600-0310', '600-6700-0087', '600-6800-0000', '600-6800-0089',
                                    '600-6800-0090', '600-6800-0091', '600-6800-0092', '600-6900-0094', '600-6900-0388', '600-6900-0389', '600-6900-0390', '600-6900-0391',
                                    '600-6900-0392', '600-6900-0393', '600-6900-0246', '600-6900-0394', '600-6900-0395', '600-6900-0396', '600-6900-0397', '600-6900-0398',
                                    '600-6900-0095', '600-6900-0096', '600-6900-0097', '600-6900-0098', '600-6900-0247', '600-6900-0247', '600-6900-0248', '600-6900-0307',
                                    '600-6900-0358', '700-7000-0107', '700-7300-0111', '700-7400-0286', '700-7450-0114', '700-7460-0115', '700-7600-0116', '700-7800-0118',
                                    '700-7800-0120', '700-7850-0000', '700-7850-0121', '700-7850-0122', '700-7850-0123', '700-7850-0124', '700-7850-0125', '700-7850-0126',
                                    '700-7850-0127', '700-7850-0129', '700-7851-0117', '700-7900-0130', '700-7900-0131', '700-7900-0132', '800-8000-0159', '800-8000-0325',
                                    '800-8100-0163', '800-8100-0164', '800-8100-0165', '800-8100-0169', '800-8100-0170', '800-8200-0173', '800-8200-0174', '800-8300-0175',
                                    '800-8400-0176', '800-8500-0177', '800-8500-0178', '800-8500-0179', '800-8500-0315', '800-8600-0180', '800-8600-0193', '900-9200-0219'])


    Core_POI = core_or_non_core(Place)
    CountryCode, PlaceId = CountryCode_PlaceID(Place)

    # Get category
    CategoryIds = []
    emit_string = ''
    CategoryIdList = Place.findall(ns+"CategoryId")
    for Category in CategoryIdList:
        CatId = Category.text
        CategoryIds.append(CatId)

    URL_indicators = ["http", "www", ".com", ".net", ".org", "://"]

    if set(CategoryIds).intersection(USA_Core_POI_categories) and Core_POI == 'Non-Core':
        ContactStrings = Place.findall(ns+"ContactString")
        webURLs = []; URL = ''
        for ContactString in ContactStrings:
            cs = ContactString.text
            for ui in URL_indicators:
                if ui in cs:
                    cs = cs.strip('\n')
                    webURLs.append(cs)

        webURLset = set(webURLs)
        if len(webURLset) > 1:
            URL = "; ".join(webURLset)
        else:
            URL = list(webURLset)[0]

        emit_string = PlaceId+'|'+URL
        return emit_string

# Test_0001 ------------------------------------------------------
def Test_0001(Place):
    """
    # Test core vs non-core to see if the core_or_non_core() function is correct.
    # Update: It is confirmed that the function DID contain a bug:

        ExternalReferenceList = Place.findall(ns+"ExternalReference")
        ExternalRefSystem = ""
        for ExternalReference in ExternalReferenceList:
            ExternalRefattrib = ExternalReference.attrib
            if 'system' in ExternalRefattrib.keys():
                ExternalRefSystem = ExternalRefattrib['system']
        if ExternalRefSystem == "corepoixml":
            Core_POI = 'Core'
        else:
            Core_POI = 'Non-Core'
        return Core_POI

    # This function has now been updated with the corrected code shown below.

    """
    return_emits = []
    core_nc = core_or_non_core(Place)
    emit_string = 'Test_0001|'+core_nc
    return_emits.append(emit_string)


    ExternalReferenceList = Place.findall(ns+"ExternalReference")
    ExternalRefSystem = ""
    Core_POI = ""
    for ExternalReference in ExternalReferenceList:
        ExternalRefattrib = ExternalReference.attrib
        if 'system' in ExternalRefattrib.keys():
            ExternalRefSystem = ExternalRefattrib['system']
            if ExternalRefSystem == "corepoixml":
                Core_POI = 'Core'
    # After traversing all ExternalReferences:
    if Core_POI != 'Core':
        Core_POI = 'Non-Core'
    emit_string = 'Test_0001a|'+Core_POI
    return_emits.append(emit_string)
    return return_emits


# KVP_0001a ------------------------------------------------------
def KVP_0001a(Place):
    """
    # Captures conflicting diesel keys (true\false contradiction) in the AdditionalAttributeList
    xpath: Place/Content/Rich/AdditionalAttributeList/AdditionalAttribute[@attributeType="FUEL"]/Attribute[@key]
    For example, make sure there is not a key="Diesel" 'true' AND key="Diesel" 'false' in the same Place
    This can only be run locally at this time, due to FUEL types product not being included in EWP yet.
    """

    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    # Use a set to find conflicting values
    AdditionalAttributeSet = set()
    true_false = []
    AdditionalAttributes = Place.findall(ns+"AdditionalAttribute")
    for AdditionalAttribute in AdditionalAttributes:
        AdditionalAttribute_type = AdditionalAttribute.attrib
        if AdditionalAttribute_type["attributeType"] == 'FUEL':
            for Attribute in AdditionalAttribute:
                Attribute_type = Attribute.attrib
                if Attribute_type["key"] == 'Diesel':
                    AdditionalAttributeSet.add(Attribute.text)
                    true_false.append(Attribute.text)

    if len(AdditionalAttributeSet) > 1:
        found = ' '.join(true_false)
        emit_string = 'KVP_0001a|'+CountryCode+'|'+PlaceId+'|KVP FUEL-Diesel true\\false contradiction|'+found
        return emit_string


# KVP_0001b ------------------------------------------------------
def KVP_0001b(Place):
    """
    # Captures duplicate diesel keys (true\true or false\false) in the AdditionalAttributeList
    xpath: Place/Content/Rich/AdditionalAttributeList/AdditionalAttribute[@attributeType="FUEL"]/Attribute[@key]
    For example, make sure there is not a key="Diesel" 'true' AND key="Diesel" 'false' in the same Place
    This can only be run locally at this time, due to FUEL types product not being included in EWP yet.
    """

    CountryCode, PlaceId = CountryCode_PlaceID(Place)
    # Use a set to find conflicting values
    DupCheck = []
    AdditionalAttributes = Place.findall(ns+"AdditionalAttribute")
    for AdditionalAttribute in AdditionalAttributes:
        AdditionalAttribute_type = AdditionalAttribute.attrib
        if AdditionalAttribute_type["attributeType"] == 'FUEL':
            for Attribute in AdditionalAttribute:
                Attribute_type = Attribute.attrib
                if Attribute_type["key"] == 'Diesel':
                    DupCheck.append(Attribute.text)

    AdditionalAttributeSet = set(DupCheck)
    if len(DupCheck) > 1 and len(AdditionalAttributeSet) == 1:
        found = ' '.join(DupCheck)
        emit_string = 'KVP_0001b|'+CountryCode+'|'+PlaceId+'|KVP FUEL-Diesel duplicate true or false|'+found
        return emit_string



# -----------------------------------------------------------------------------
t = '{http://places.maps.domain.com/pds}'
ns = './/'+t

# This dictionary contains the complete list of available validations
validation_modules = { 'Stats_0001': Stats_0001,
                        'Stats_0002': Stats_0002,
                        'Stats_0003': Stats_0003,
                        'Stats_0004': Stats_0004,
                        'Stats_0005': Stats_0005,
                        'Basic_0001': Basic_0001,
                        'Basic_0002': Basic_0002,
                        'Basic_0003': Basic_0003,
                        'Basic_0004': Basic_0004,
                        'Basic_0005': Basic_0005,
                        'Basic_0006a': Basic_0006a,
                        'Basic_0006b': Basic_0006b,
                        'Basic_0008a': Basic_0008a,
                        'Basic_0008b': Basic_0008b,
                        'Basic_0008c': Basic_0008c,
                        'Basic_0008d': Basic_0008d,
                        'Basic_0008e': Basic_0008e,
                        'Basic_0008f': Basic_0008f,
                        'Basic_0008g': Basic_0008g,
                        'Basic_0008h': Basic_0008h,
                        'Basic_0015': Basic_0015,
                        'Basic_0017a': Basic_0017a,
                        'Basic_0017b': Basic_0017b,
                        'Basic_0017c': Basic_0017c,
                        'Basic_0017d': Basic_0017d,
                        'Basic_0020': Basic_0020,
                        'Basic_0022': Basic_0022,
                        'Basic_0029': Basic_0029,
                        'Basic_0030': Basic_0030,
                        'Basic_0031': Basic_0031,
                        'Basic_0032': Basic_0032,
                        'Basic_0033': Basic_0033,
                        'Basic_0034': Basic_0034,
                        'Basic_0035': Basic_0035,
                        'Basic_0036': Basic_0036,
                        'Basic_0037': Basic_0037,
                        'Basic_0040': Basic_0040,
                        'Basic_0041': Basic_0041,
                        'Basic_0045': Basic_0045,
                        'Basic_0046': Basic_0046,
                        'Basic_0048': Basic_0048,
                        'Basic_0057': Basic_0057,
                        'Basic_0087': Basic_0087,
                        'Basic_0101': Basic_0101,
                        'Basic_0104': Basic_0104,
                        'Basic_0116': Basic_0116,
                        'Basic_0117': Basic_0117,
                        'Basic_0135': Basic_0135,
                        'Basic_0144': Basic_0144,
                        'Basic_0145': Basic_0145,
                        'Basic_0147': Basic_0147,
                        'GEO_0001' : GEO_0001,
                        'GEO_0002' : GEO_0002,
                        'GEO_0003' : GEO_0003,
                        'GEO_0004' : GEO_0004,
                        'GEO_0005' : GEO_0005,
                        'DVN_0001' : DVN_0001,
                        'New_0001' : New_0001,
                        'New_0002' : New_0002,
                        'New_0004' : New_0004,
                        'New_0005' : New_0005,
                        'New_0006' : New_0006,
                        'New_0007' : New_0007,
                        'New_0008' : New_0008,
                        'New_0009' : New_0009,
                        'New_0010' : New_0010,
                        'New_0011' : New_0011,
                        'New_0012' : New_0012,
                        'New_0013' : New_0013,
                        'New_0014' : New_0014,
                        'New_0015' : New_0015,
                        'New_0016' : New_0016,
                        'New_0017' : New_0017,
                        'New_0018' : New_0018,
                        'New_0019' : New_0019,
                        'New_0020' : New_0020,
                        'New_0021' : New_0021,
                        'New_0022' : New_0022,
                        'New_0023' : New_0023,
                        'TQS_0001' : TQS_0001,
                        'TQS_0002' : TQS_0002,
                        'TQS_0003' : TQS_0003,
                        'TQS_0004' : TQS_0004,
                        'TQS_0005' : TQS_0005,
                        'TQS_0006' : TQS_0006,
                        'TQS_0007' : TQS_0007,
                        'TQS_0008' : TQS_0008,
                        'TQS_0009' : TQS_0009,
                        'TQS_0010' : TQS_0010,
                        'TQS_0011' : TQS_0011,
                        'TQS_0012' : TQS_0012,
                        'Test_0001' : Test_0001,
                        'KVP_0001a' : KVP_0001a,
                        'KVP_0001b' : KVP_0001b,
                        'Media_0002' : Media_0002 }

print len(validation_modules), "modules implemented"

# -----------------------------------------------------------------------------
def parseProductValXML():
    tree = etree.parse(xml_file)
    product_validations = {}
    for elem in tree.iter():
        if elem.tag == 'Product':
            if elem.attrib:
                prod_name = elem.attrib['name']
                product_validations[prod_name] = {}
                i = []; e = []
                for child in elem:
                    if child.text:
                        if child.tag == "Include":
                            i.append(child.text)
                        if child.tag == "Exclude":
                            e.append(child.text)
                product_validations[prod_name]["Include"] = i
                product_validations[prod_name]["Exclude"] = e
    return product_validations

# -----------------------------------------------------------------------------
# Creating this function trimmed over 300 lines of repeated code!
def CountryCode_PlaceID(Place):
    try:
        CountryCode = Place.find(ns+"CountryCode").text
    except:
        CountryCode = 'None'
    try:
        PlaceId = Place.find(ns+"PlaceId").text
    except:
        PlaceId = 'None'
    return CountryCode, PlaceId

# -----------------------------------------------------------------------------
def core_or_non_core(Place):
    """ Determine if it is a Core POI or not """

    system_set = set()
    Core_POI = ""
    ExternalReferenceList = Place.findall(ns+"ExternalReference")
    for ExternalReference in ExternalReferenceList:
        ExternalRefattrib = ExternalReference.attrib
        if 'system' in ExternalRefattrib.keys():
            ExternalRefSystem = ExternalRefattrib['system']
            system_set.add(ExternalRefSystem)
    # After traversing all ExternalReferences:
    if "corepoixml" in system_set:
        Core_POI = 'Core'
    else:
        Core_POI = 'Non-Core'
    return Core_POI

# -----------------------------------------------------------------------------
def math_distance(a, b, unit):
    """Example input (LAT, LONG):
    a = (49.8755, 6.07594)
    b = (49.87257, 6.0784)
    """
    miles = 3958.76             # miles
    km = 6.371*1000             # km
    meters = 6.371*1000*1000    # meters

    if unit == "mi":
        dist = miles
    elif unit == "km":
        dist = km
    elif unit == "m":
        dist = meters
    else:
        dist = meters

    y1  = float(a[0])
    x1  = float(a[1])
    y2  = float(b[0])
    x2  = float(b[1])

    y1 *= pi/180.0
    x1 *= pi/180.0
    y2 *= pi/180.0
    x2 *= pi/180.0

    # approximate great circle distance with law of cosines
    x = sin(y1)*sin(y2) + cos(y1)*cos(y2)*cos(x2-x1)
    if x > 1:
        x = 1
    return acos( x ) * dist

# -----------------------------------------------------------------------------
def getValidationList(Product):

    # Get product_validations hash from xml
    product_validations = parseProductValXML()

    try:
        Include = product_validations[Product]["Include"]
        Exclude = product_validations[Product]["Exclude"]
    except:
        Product = 'default'
        Include = product_validations[Product]["Include"]
        Exclude = product_validations[Product]["Exclude"]

    # An Include list was provided, so use only this list of validations
    if Include:
        runList = Include
    # No Include list was provided, but we have a small number of validations to exclude
    # so subtract the validations in this exclude list from the overall list of available validations
    elif Exclude:
        runList = sorted(validation_modules.keys())
        for e in Exclude:
            if e in runList:
                runList.remove(e)
    # No Include or Exclude lists were provided, so default to using ALL validations
    else:
        runList = sorted(validation_modules.keys())
    return tuple(runList)

