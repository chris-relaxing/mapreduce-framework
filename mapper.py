
import os
import sys
import lxml.etree as etree
import PlacesValidations as pv


t = '{http://places.maps.domain.com/pds}'

# Take in arguments from the MapReduce command
try:
    Product = sys.argv[1]
    queryPlaceId = sys.argv[2]
except:
    Product = 'EWP'
    queryPlaceId = ''

def main():

    runList = pv.getValidationList(Product)
    emit_return = ""

    if "Media_0002" or "Basic_0001" or "Basic_0002" in runList:
        try:
            map_input_file = os.environ["map_input_file"]       # Because we need the name of the xml for the output
        except:
            map_input_file = "unknown"

    for line in sys.stdin:
        try:
            if line.find("PlaceList") >= 0:
                PlaceList = line.rstrip()
                if "<PlaceList" in line:
                    if "Basic_0001" in runList:
                        emit_return = pv.validation_modules["Basic_0001"](PlaceList, map_input_file)
                        if emit_return:
                            if type(emit_return) is str:
                                sys.stdout.write("{0}\t1\n".format(emit_return))
                continue
            node = etree.fromstring(line)
            if node.tag == t+'Place':
                Place = node

                for val in runList:
                    if val in pv.validation_modules:
                        if val == "Basic_0001":
                            continue
                        if val == "Media_0002" or val == "Basic_0002":
                            emit_return = pv.validation_modules[val](Place, map_input_file)
                        if val == "New_0015":
                            emit_return = pv.validation_modules[val](Place, queryPlaceId)
                        else:
                            emit_return = pv.validation_modules[val](Place)
                        if emit_return:
                            if type(emit_return) is str:
                                sys.stdout.write("{0}\t1\n".format(emit_return))
                            elif type(emit_return) is list:
                                for emit_string in emit_return:
                                    sys.stdout.write("{0}\t1\n".format(emit_string))

            node.clear()
        except:
            continue

if __name__ == '__main__':
    main()
