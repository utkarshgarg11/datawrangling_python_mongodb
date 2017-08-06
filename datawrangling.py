
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]') # To eliminate problematic characters from our data

# Standard keys for 'way' and 'node' tags
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

# The mapping dictionary is used to convert address streets' shorthand notations to full names for uniformity.
mapping = { "St": "Street",
            "St.": "Street",
            "Rd.": "Road",
            "Rd": "Road",
            "Ave": "Avenue",
            "Ct": "Court",
            "Ct.": "Court",
            "Pl": "Place",
            "Pl.": "Place",
            "Dr": "Drive",
            "Dr.": "Drive",
            "Sq.": "Square",
            "Sq": "Square",
            "Tr": "Trail",
            "Tr.": "Trail",
            "Pw" : "Parkway",
            "Pw.": "Parkway",
            "Co" : "Commons",
            "Co.": "Commons",
            }


def shape_element(element):
    # Defing the initial structure of the element, although not required.'node' variable is for both 'node' and 'way' here
    node = {'id':'','visible':'','type':'', 'pos':[],
            'created':{'changeset':'','user':'','version':'','uid':'','timestamp':''} }
    
    if element.tag == "node" or element.tag == "way" :
        
        if element.tag == 'node':
            node['pos'] = [element.attrib['lat'],element.attrib['lon']] # Set 'pos' key as list [latitude, longitude]
            
        if 'visible' in element.attrib:
            node['visible'] = element.attrib['visible']
            
        node['type'] = element.tag
        node['id'] = element.attrib['id']
        
        # Adding key/value pairs from main attributes
        for item in CREATED:
            node['created'][item] = element.attrib[item]
            
        # Way tags also contain node_refs
        if element.tag == 'way':
            node['node_refs'] = []
            
        # Iterating inside the main element
        for item in element.iter():
            
            # For 'tag' elements only
            if item.tag == 'tag':
                k = item.attrib['k']
                split_k = k.split(':') # Splitting from ':' to check if 'addr' or not
                
                # For checking for address street for problemchars
                if not problemchars.search(k) and len(split_k) <= 2:
                    
                    if split_k[0] == 'addr':
                        if 'address' not in node:
                            node['address'] = {}   # Create address subkey if it doesn't exist
                        
                        if k == 'addr:street':
                            st = item.attrib['v'].split()[-1]   # Shorthand notation for street type.
                            
                            # Replace the shorthand notation with the full type.
                            if st in mapping:
                                node['address']['street'] = item.attrib['v'].replace(st,mapping[st])
                                # Separate attribute for street type for simpilfying querying based on type of street
                                node['address']['street_type'] = mapping[st]
                        else:   
                            # Making 'address.state' uniform for 'Michigan' state
                            if split_k[1] == 'state' and (item.attrib['v'] in ['MI','mi','Mchigan','MICHIGAN']):
                                node['address']['state'] = 'Michigan'
                            else:
                                node['address'][split_k[1]] = item.attrib['v']
                    else:
                        node[k] = item.attrib['v']
                        
            # For 'nd' elements only. Occurs in 'way' elements only.
            elif item.tag == 'nd':
                node['node_refs'].append(item.attrib['ref'])
                
        # Cleaning the 'lanes' attribute, if it exists
        if 'lanes' in node:
            node['lanes'] = str(node['lanes']).replace(' ','')
            if ';' in node['lanes']:
                node['lanes'] = node['lanes'].split(';')
        return node
    else:
        return None

def get_element(osm_file, tags=('node', 'way')):
    
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

def process_map(file_in, pretty = False):
    
    file_out = "{0}.json".format(file_in)
    
    with codecs.open(file_out, "w") as fo:
        
        for i, elem in enumerate(get_element(file_in)):
            el = shape_element(elem)
            if el:
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
            elem.clear()

# Calling the function to carry out the parsing            
process_map('D:\DANDProjects\detroit_michigan.osm')


from pymongo import MongoClient
client = MongoClient()
db = client.openstreetmap
collection = db.detroit_2


print '\n\nSize of the Collection (in bytes):'
print db.command('collstats','detroit')['size']

import pprint
pp = pprint.PrettyPrinter(indent = 4)


print '\n\nTotal number of documents:'
print collection.count()


print '\n\nTotal number of nodes:'
print collection.find({'type':'node'}).count()


print '\n\nTotal number of ways:'
print collection.find({'type':'way'}).count()


print '\n\nNumber of tags which are neither nodes, nor ways:'
not_way_or_node = collection.find({'type':{'$nin':['node','way']}})
print not_way_or_node.count()


print '\n\nNumber of nodes which are visible and which are not:'
print collection.find({'visible':'false'}).count()

print collection.find({'visible':'true'}).count()


print '\n\nSince we cannot see any document with \'visible\' key set to \'true\' or \'false\', we remove the field from our documents:'
remove_visible = collection.update_many({'visible':''}, { '$unset' : {'visible': 1}})
pp.pprint(collection.find_one())


print '\n\nUnwanted types are present due to second level tag\'s \'k\' attribute:'
print collection.distinct('type')


print '\n\nRemoving the documents wuth type other than "node" or "way":'
collection.delete_many({'type':{'$nin':['node','way']}})


print '\n\nNew document count after deleting the unrequired documents:'
new_length = collection.find().count()
print new_length


print '\n\nSample document of the type "way":'
pp.pprint(collection.find_one({'type':'way'}))


print '\n\nThis shows the distinct number of lanes present in "way" tags. As we can see it contains instances with more than one integers separated with semicolons.'
print collection.distinct('lanes',{'type':'way','lanes':{'$exists':1}})

print '\n\nLanes with more than one entry. The entries have been split into a list for convenience:'
list_lanes = collection.aggregate([{'$match':{'lanes':{'$size':2}}},{'$project':{'type':1, 'lanes':1}}])
for item in list_lanes:
    print item

print '\n\nDifferent types of highways the "way" tags contain:'
print collection.distinct('highway', {'type':'way'})


print '\n\nHighway counts grouped by their types:'
highway_count = collection.aggregate([{'$match':{'type':'way','highway':{'$exists':1}}},
                                      {'$group':{'_id':'$highway','count':{'$sum':1}}},{'$sort':{'count':-1}}])
for item in highway_count:
    print item


print '\n\nTotal node_refs present in way tags with highways:'
total_node_refs_with_highways = collection.aggregate([{'$match':{'type':'way','highway':{'$exists':1}}},
                                                    {'$group':{'_id':'Highway_refs','total':{'$sum':{'$size':'$node_refs'}}}}
                                                    ])
for item in total_node_refs_with_highways:
    print item


print '\n\nTotal node_refs present in each type of highway:'
node_refs_groupedby_highway = collection.aggregate([{'$match':{'type':'way','highway':{'$exists':1}}},
                     {'$unwind':'$node_refs'},
                     {'$group':{'_id':'$highway','total_node_refs':{'$sum':1}}},{'$sort':{'total_node_refs':-1}}])
for item in node_refs_groupedby_highway:
    print item


print '\n\nThe Geographical location of Detroit,Michigan to be used for some of the following queries - LATITUDE = 42.331429, LONGITUDE = -83.045753'

print '\n\nDocuments which have no latitude or longitude:'
print collection.find({'pos':[]}).count()


print '\n\nUnsetting the field \'pos\'(position) from such documents...'
collection.update_many({'pos':[]},{'$unset':{'pos':''}})


print '\n\nNumber of elements with empty position key(after unsetting "pos" key):'
print collection.find({'pos':[]}).count()


print '\n\nElements situated NORTH of the standard Detroit latitude:'
print collection.find({'pos.0':{'$gt':'42.331429'}}).count()


print '\n\nElements situated SOUTH of or ON the standard latitude of Detroit:'
print collection.find({'pos.0':{'$lte':'42.331429'}}).count()


print '\n\nElements due EAST of standard Detroit longitude:' 
print collection.find({'pos.1':{'$gt':'-83.045753'}}).count()


print '\n\nElements WEST of standard Detroit longitude:'
print collection.find({'pos.1':{'$lte':'-83.045753'}}).count()


print '\n\nThe elements have been created by these distinct users:'
print collection.distinct('created_by')


print '\n\nNumber of documents which have a creator mentioned:'
print collection.find({'created_by':{'$exists':1}}).count()


print '\n\nTags created by each user (Descending order of total tags created). A significantly large percentage of tags did not have any creator mentioned:'
groups_by_creator = collection.aggregate([{'$group':{'_id':'$created_by', 'count':{'$sum':1}}},{'$sort':{'count':-1}}])
for item in groups_by_creator:
    print item

print '\n\nDistinct streets present in our data, along with the count of how many unique of them there are:'
distinct_streets = collection.distinct('address.street')
print distinct_streets
print 'Number of disctinct streets-',len(distinct_streets)
                
print '\n\nCount of streets by their types:'
count_by_street_types = collection.aggregate([{'$match':{'address.street_type':{'$exists':1}}},
                                        {'$group':{'_id':'$address.street_type','count':{'$sum':1}}}])
for item in count_by_street_types:
    print item

