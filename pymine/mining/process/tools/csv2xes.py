#from lxml import etree
import xml.dom.minidom as minidom
import xml.etree.cElementTree as ET
import csv
from datetime import datetime

def createXesSkeleton():
    """

    :return:
    """
    skeletonRoot = ET.Element("log")
    skeletonRoot.attrib["xes.version"] = "1.0"
    skeletonRoot.attrib["xmlns"] = "http://www.xes-standard.org"
    skeletonRoot.attrib["xes.creator"] = "PyXES"

    extension1 = ET.SubElement(skeletonRoot, "extension")
    extension1.attrib["name"] = "Concept"
    extension1.attrib["prefix"] = "concept"
    extension1.attrib["uri"] = "http://www.xes-standard.org/concept.xesext"

    extension2 = ET.SubElement(skeletonRoot, "extension")
    extension2.attrib["name"] = "Lifecycle"
    extension2.attrib["prefix"] = "lifecycle"
    extension2.attrib["uri"] = "http://www.xes-standard.org/lifecycle.xesext"

    extension3 = ET.SubElement(skeletonRoot, "extension")
    extension3.attrib["name"] = "Time"
    extension3.attrib["prefix"] = "time"
    extension3.attrib["uri"] = "http://www.xes-standard.org/time.xesext"

    extension4 = ET.SubElement(skeletonRoot, "extension")
    extension4.attrib["name"] = "Organizational"
    extension4.attrib["prefix"] = "org"
    extension4.attrib["uri"] = "http://www.xes-standard.org/org.xesext"

    trace = ET.SubElement(skeletonRoot, "global")
    trace.attrib["scope"] = "trace"
    key = ET.SubElement(trace, "string")
    key.attrib["key"] = "concept:name"
    key.attrib["value"] = "name"

    event = ET.SubElement(skeletonRoot, "global")
    event.attrib["scope"] = "event"
    keyname = ET.SubElement(event, "string")
    keyname.attrib["key"] = "concept:name"
    keyname.attrib["value"] = "name"
    keylifecicle = ET.SubElement(event, "string")
    keylifecicle.attrib["key"] = "lifecycle:dependency"
    keylifecicle.attrib["value"] = "dependency"
    keyres = ET.SubElement(event, "string")
    keyres.attrib["key"] = "org:resource"
    keyres.attrib["value"] = "resource"
    time = ET.SubElement(event, "date")
    time.attrib["key"] = "time:timestamp"
    time.attrib["value"] = "2015-01-09T10:45:56.332+01:00"
    keyactivity = ET.SubElement(event, "string")
    keyactivity.attrib["key"] = "activity"
    keyactivity.attrib["value"] = "string"
    keyresource = ET.SubElement(event, "string")
    keyresource.attrib["key"] = "resource"
    keyresource.attrib["value"] = "string"

    activityClassifier = ET.SubElement(skeletonRoot, "classifier")
    activityClassifier.attrib["name"] = "Activity"
    activityClassifier.attrib["keys"] = "activity"
    resourceClassifier = ET.SubElement(skeletonRoot, "classifier")
    resourceClassifier.attrib["name"] = "Resource"
    resourceClassifier.attrib["keys"] = "resource"

    modelString = ET.SubElement(skeletonRoot, "string")
    modelString.attrib["key"] = "lifecycle:model"
    modelString.attrib["value"] = "standard"

    creatorString = ET.SubElement(skeletonRoot, "string")
    creatorString.attrib["key"] = "creator"
    creatorString.attrib["value"] = "PyXES"

    libraryString = ET.SubElement(skeletonRoot, "string")
    libraryString.attrib["key"] = "library"
    libraryString.attrib["value"] = "PyXES"

    return skeletonRoot

def appendTrace(xesTree, traceName, events):
    traceElement = ET.SubElement(xesTree, "trace")
    nameString = ET.SubElement(traceElement, "string")
    nameString.attrib["key"] = "concept:name"
    nameString.attrib["value"] = traceName
    creatorString = ET.SubElement(traceElement, "string")
    creatorString.attrib["key"] = "creator"
    creatorString.attrib["value"] = "PyXES"
    for event in events:
        eventElement = ET.SubElement(traceElement, "event")
        keyname = ET.SubElement(eventElement, "string")
        keyname.attrib["key"] = "concept:name"
        keyname.attrib["value"] = event["name"]
        keylifecicle = ET.SubElement(eventElement, "string")
        keylifecicle.attrib["key"] = "lifecycle:transition"
        keylifecicle.attrib["value"] = event['lifecycle']
        keyres = ET.SubElement(eventElement, "string")
        #keyres.attrib["key"] = "org:resource"
        #keyres.attrib["value"] = event["resource"]
        time = ET.SubElement(eventElement, "date")
        time.attrib["key"] = "time:timestamp"
        time.attrib["value"] = event["timestamp"]
        keyactivity = ET.SubElement(eventElement, "string")
        keyactivity.attrib["key"] = "activity"
        keyactivity.attrib["value"] = event["activity"]
        keyresource = ET.SubElement(eventElement, "string")
        #keyresource.attrib["key"] = "resource"
        #keyresource.attrib["value"] = event["resource"]

def writeXesFile(xesTree, filename):
    """

    :param xesTree:
    :param filename:
    :return:
    """
    xesTree.write(filename)

def writePrettyXesFile(filename, prettyFilename):
    """

    :param filename:
    :param prettyFilename:
    :return:
    """
    xml = minidom.parse(filename) # or xml.dom.minidom.parseString(xml_string)
    pretty_xml_as_string = xml.toprettyxml()
    file = open(prettyFilename, 'wU')
    file.write(pretty_xml_as_string)
    file.close()

def appendCaseRow(row):
    """

    :param row:
    :return:
    """
    timestamp = datetime.strftime(datetime.strptime(row['timestamp'],'%d-%m-%Y %H:%M:%S.%f' ),'%Y-%m-%dT%H:%M:%S.%f' )
    #timestamp = timestamp.replace(" ","T")
    return {'name' : row['activity'],
            'lifecycle' : row['lifecycle'],
            #'org:resource' : row['resource'],
            'timestamp' : timestamp,
            'activity' : row['activity'],
            #'resource' : row['resource'],
            }

def loadCSVFile(filename):
    """

    :param filename:
    :return:
    """
    cases = {}
    with open(filename, 'rbU') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            case_id = row['case_id']
            if case_id not in cases:
                cases[case_id] = []
            cases[case_id].append(appendCaseRow(row))
    return cases

def main():
    """

    :return:
    """
    #print('PyXES: creating a xes file')
    xesDoc = createXesSkeleton()
    cases = loadCSVFile('/Users/ale/Desktop/protube_traces_100000.csv')
    #print "CASES: "
    #print cases
    for case in cases:
        appendTrace(xesDoc, case, cases[case])
    skeleton = ET.ElementTree(xesDoc)
    writeXesFile(skeleton, '/Users/ale/Desktop/protube_traces_100000.xes')
    #writePrettyXesFile(skeleton, '/Users/paolo/Desktop/Analytics/Paolo/testPretty.xes')

if __name__ == '__main__':
  main()