#!/usr/bin/env python

import yaml
import sys
import urllib2
import json
import codecs
import collections

ECOWARE_INFRASTRUCTURE_TYPE = 'ecoware.infrastructure'
ECOWARE_APP_TYPE = 'ecoware.app'

IMPORTS_KEY = 'imports'
INPUTS_KEY = 'inputs'
NODE_TYPES_KEY = 'node_types'
NODE_TEMPLATES_KEY = 'node_templates'
TYPE_KEY = 'type'
DERIVED_FROM_KEY = 'derived_from'
PROPERTIES_KEY = 'properties'
INTERFACES_KEY = 'interfaces'
GET_INPUT_KEY = 'get_input'

ECOWARE_APP_TIERS_KEY = 'tiers'
ECOWARE_APP_NAME_KEY = 'name'
ECOWARE_MAX_VMS_KEY = 'max_vms'
ECOWARE_CONTAINER_TYPE_KEY = 'container_type'
ECOWARE_CONTAINER_IMAGE_TYPE_KEY = 'image_type'
ECOWARE_CONTAINER_IMAGE_KEY = 'image'
ECOWARE_THRESHOLDS_KEY = 'thresholds'

ECOWARE_JSON_APP_NAME_KEY = 'name'
ECOWARE_JSON_APP_TIERS_KEY = 'tiers'
ECOWARE_JSON_MAX_VMS_KEY = 'max_vms'
ECOWARE_JSON_INFRASTRUCTURE_KEY = 'infrastructure'
ECOWARE_JSON_APPS_KEY = 'apps'
ECOWARE_JSON_CLOUD_DRIVER_KEY = 'cloud_driver'
ECOWARE_JSON_CONTAINER_IMAGE_SUFFIX_KEY = '_image'


ECOWARE_TOSCA_JSON_CLOUD_DRIVER_BINDINGS = {'provider_name': 'name', 'vm_image': 'vm_image', 'vm_flavor': 'vm_flavor', 'api_key' : 'api_key', 'api_secret' : 'api_secret'}
ECOWARE_TOSCA_JSON_APPS_BINDINGS = {'name': 'name', 'tiers': 'tiers'}
ECOWARE_TOSCA_JSON_TIER_BINDINGS = {'name': 'name', 'max_response_time': 'max_rt', 'max_nodes' : 'max_nodes', 'depends_on' : 'depends_on'}
ECOWARE_TOSCA_JSON_CONTAINER_TYPE_BINDINGS = {'run_parameters' : 'run_parameters', 'port_bindings' : 'port_bindings', 'on_dependency_scale' : 'on_dependency_scale', 'on_node_scale' : 'on_node_scale'}

def nodeTempleteIsA(nodeTemplate, type, lib):
    nodeTypes = lib[NODE_TYPES_KEY]
    def checkTypeHierarchy(nodeType):
        derivedFromType = nodeTypes[nodeType].get(DERIVED_FROM_KEY, None)
        if derivedFromType is None:
            return 0
        elif derivedFromType == type:
            return 1
        else:
            checkTypeHierarchy(derivedFromType)

    nodeType = nodeTemplate[TYPE_KEY]
    if nodeType == type:
        return 1
    else:
        return checkTypeHierarchy(nodeType)

def flatNodeProperties(nodeTemplate, lib, inputs):
    if isinstance(nodeTemplate, dict) and nodeTemplate.get(PROPERTIES_KEY, None) is None:
        res = {}
        for key, value in nodeTemplate.items():
            res[key] = flatNodeProperties(value, lib, inputs)
        return res
    elif isinstance(nodeTemplate, list):
        res = []
        for value in nodeTemplate:
            res.append(flatNodeProperties(value, lib, inputs))
        return res
    elif not isinstance(nodeTemplate, dict) and not isinstance(nodeTemplate, list):
        return nodeTemplate

    res = dict(nodeTemplate[PROPERTIES_KEY])
    nodeTypes = lib[NODE_TYPES_KEY]

    def enrichWithHierarchy(nodeTypeName):
        nodeType = nodeTypes[nodeTypeName]
        properties = nodeType[PROPERTIES_KEY]
        if properties is None:
            return
        for key, value in properties.items():
            if isinstance(value, dict) and value[TYPE_KEY] is not None:
                valueType = value[TYPE_KEY]
                if inputs is not None and isinstance(valueType, dict) and valueType.get(GET_INPUT_KEY, None) is not None:
                    getInputValue = valueType[GET_INPUT_KEY]
                    if inputs.get(getInputValue, None) is not None:
                        print 'Insert value for ' + getInputValue
                        res[key] = raw_input()
            else:
                res[key] = value
        derivedFromType = nodeType.get(DERIVED_FROM_KEY, None)
        if derivedFromType is not None:
            return enrichWithHierarchy(derivedFromType)

    enrichWithHierarchy(nodeTemplate[TYPE_KEY])

    newNodeTemplate = dict(nodeTemplate)

    flattedRes = {}

    for key, value in res.items():
        flattedRes[key] = flatNodeProperties(value, lib, inputs)

    newNodeTemplate[PROPERTIES_KEY] = flattedRes

    return newNodeTemplate

def bindDic(dic, bindings, res):
    for key, value in dic.items():
        outKey = bindings.get(key, None)
        if outKey is not None:
            res[outKey] = value
    return res

f = open(sys.argv[1])
blueprint = yaml.load(f)
f.close()

ecowareLib = yaml.load(urllib2.urlopen(blueprint[IMPORTS_KEY][0]).read())
nodeTemplates = blueprint[NODE_TEMPLATES_KEY]
inputs = blueprint[INPUTS_KEY]
res = collections.OrderedDict({})
res[ECOWARE_JSON_APPS_KEY] = []

for key, value in nodeTemplates.items():
    if nodeTempleteIsA(value, ECOWARE_INFRASTRUCTURE_TYPE, ecowareLib):
        nodeTemplate = flatNodeProperties(value, ecowareLib, inputs)
        res[ECOWARE_JSON_INFRASTRUCTURE_KEY] = {}
        res[ECOWARE_JSON_INFRASTRUCTURE_KEY][ECOWARE_JSON_CLOUD_DRIVER_KEY] = bindDic(nodeTemplate[PROPERTIES_KEY], ECOWARE_TOSCA_JSON_CLOUD_DRIVER_BINDINGS, {})
        res[ECOWARE_JSON_INFRASTRUCTURE_KEY][ECOWARE_JSON_MAX_VMS_KEY] = nodeTemplate[PROPERTIES_KEY][ECOWARE_MAX_VMS_KEY]
    elif nodeTempleteIsA(value, ECOWARE_APP_TYPE, ecowareLib):
        nodeTemplate = flatNodeProperties(value, ecowareLib, inputs)
        app = collections.OrderedDict({})
        app[ECOWARE_APP_NAME_KEY] =  nodeTemplate[PROPERTIES_KEY][ECOWARE_APP_NAME_KEY]

        app[ECOWARE_JSON_APP_TIERS_KEY] = collections.OrderedDict({})
        for tierObj in nodeTemplate[PROPERTIES_KEY][ECOWARE_APP_TIERS_KEY]:
            key, tier = tierObj.popitem()
            jsonTier = bindDic(tier[PROPERTIES_KEY], ECOWARE_TOSCA_JSON_TIER_BINDINGS, {})
            for thresholdKey, thresholdValue  in tier[PROPERTIES_KEY][ECOWARE_THRESHOLDS_KEY].items():
                jsonTier[thresholdKey]=thresholdValue
            containerType = tier[PROPERTIES_KEY][ECOWARE_CONTAINER_TYPE_KEY]
            jsonTier = bindDic(containerType[PROPERTIES_KEY], ECOWARE_TOSCA_JSON_CONTAINER_TYPE_BINDINGS, jsonTier)
            jsonTier[containerType[PROPERTIES_KEY][ECOWARE_CONTAINER_IMAGE_TYPE_KEY]+ECOWARE_JSON_CONTAINER_IMAGE_SUFFIX_KEY] = containerType[PROPERTIES_KEY][ECOWARE_CONTAINER_IMAGE_KEY]
            jsonTier = bindDic(containerType[INTERFACES_KEY], ECOWARE_TOSCA_JSON_CONTAINER_TYPE_BINDINGS, jsonTier)
            app[ECOWARE_JSON_APP_TIERS_KEY][key] = jsonTier

        res[ECOWARE_JSON_APPS_KEY].append(app)


with codecs.open(sys.argv[1].split('.')[0]+'.json', 'w+', 'utf8') as outfile:
    outfile.write(json.dumps(res, indent=4, ensure_ascii=False))
