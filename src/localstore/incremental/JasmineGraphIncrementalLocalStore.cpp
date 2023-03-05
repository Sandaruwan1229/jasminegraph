/**
Copyright 2021 JasminGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "JasmineGraphIncrementalLocalStore.h"

#include <memory>
#include <stdexcept>

#include "../../nativestore/RelationBlock.h"
#include "../../util/logger/Logger.h"

Logger incremental_localstore_logger;

JasmineGraphIncrementalLocalStore::JasmineGraphIncrementalLocalStore(unsigned int graphID, unsigned int partitionID) {
    gc.graphID = graphID;
    gc.partitionID = partitionID;
    gc.maxLabelSize = 43;   // TODO tmkasun: read from .properties file
    gc.openMode = "trunk";  // TODO tmkasun: read from .properties file
    this->nm = new NodeManager(gc);
};

std::pair<std::string, std::string> JasmineGraphIncrementalLocalStore::getIDs(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);
        if (edgeJson.contains("source") && edgeJson.contains("destination")){
            auto sourceJson = edgeJson["source"];
            if (sourceJson.contains("properties")) {
                auto edgeProperties = sourceJson["properties"];
                std::string graphId = std::string(edgeProperties["graphId"]);
                std::string pId = to_string(sourceJson["pid"]);
                return {graphId, pId};
            }
        }else{
            if (edgeJson.contains("properties")) {
                auto edgeProperties = edgeJson["properties"];
                std::string graphId = std::string(edgeProperties["graphId"]);
                std::string pId = to_string(edgeJson["pid"]);

                return {graphId, pId};
            }
        }

    } catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
            "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
            "error");
    }
}

std::string JasmineGraphIncrementalLocalStore::addNodeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);
        std::string sId = std::string(edgeJson["id"]);
        NodeBlock* nodeBlock = this->nm->addNode(sId);
    }
    catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
                "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
                "error");
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO tmkasun: handle JSON errors
    }
}

std::string JasmineGraphIncrementalLocalStore::addEdgeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);

        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];

        std::string sId = std::string(sourceJson["id"]);
        std::string dId = std::string(destinationJson["id"]);

        RelationBlock* newRelation = this->nm->addEdge({sId, dId});
        if (!newRelation) {
            return "";
        }
        char value[PropertyLink::MAX_VALUE_SIZE] = {};

        if (edgeJson.contains("properties")) {
            auto edgeProperties = edgeJson["properties"];
            for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                newRelation->addProperty(std::string(it.key()), &value[0]);
            }
        }

//        if (sourceJson.contains("properties")) {
//            auto sourceProps = sourceJson["properties"];
//            for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
//                strcpy(value, it.value().get<std::string>().c_str());
//                newRelation->getSource()->addProperty(std::string(it.key()), &value[0]);
//            }
//        }
//        if (destinationJson.contains("properties")) {
//            auto destProps = destinationJson["properties"];
//            for (auto it = destProps.begin(); it != destProps.end(); it++) {
//                strcpy(value, it.value().get<std::string>().c_str());
//                newRelation->getDestination()->addProperty(std::string(it.key()), &value[0]);
//            }
//        }

        incremental_localstore_logger.log("Added successfully!", "Info");
    }
    catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
                "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
                "error");
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO tmkasun: handle JSON errors
    }

}

std::string JasmineGraphIncrementalLocalStore::addCentralEdgeFromString(std::string edgeString) {
    try {
        auto edgeJson = json::parse(edgeString);

        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];

        std::string sId = std::string(sourceJson["id"]);
        std::string dId = std::string(destinationJson["id"]);

        RelationBlock* newCentralRelation = this->nm->addCentralEdge({sId, dId});
        if (!newCentralRelation) {
            return "";
        }
        char value[PropertyLink::MAX_VALUE_SIZE] = {};

        if (edgeJson.contains("properties")) {
            auto edgeProperties = edgeJson["properties"];
            for (auto it = edgeProperties.begin(); it != edgeProperties.end(); it++) {
                strcpy(value, it.value().get<std::string>().c_str());
                newCentralRelation->addCentralProperty(std::string(it.key()), &value[0]);
            }
        }

//        if (sourceJson.contains("properties")) {
//            auto sourceProps = sourceJson["properties"];
//            for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
//                strcpy(value, it.value().get<std::string>().c_str());
//                newCentralRelation->getSource()->addProperty(std::string(it.key()), &value[0]);
//            }
//        }
//        if (destinationJson.contains("properties")) {
//            auto destProps = destinationJson["properties"];
//            for (auto it = destProps.begin(); it != destProps.end(); it++) {
//                strcpy(value, it.value().get<std::string>().c_str());
//                newCentralRelation->getDestination()->addProperty(std::string(it.key()), &value[0]);
//            }
//        }

        incremental_localstore_logger.log("Added successfully!", "Info");
    }
    catch (const std::exception&) {  // TODO tmkasun: Handle multiple types of exceptions
        incremental_localstore_logger.log(
                "Error while processing edge data = " + edgeString +
                "Could be due to JSON parsing error or error while persisting the data to disk",
                "error");
        incremental_localstore_logger.log("Error malformed JSON attributes!", "error");
        // TODO tmkasun: handle JSON errors
    }

}

