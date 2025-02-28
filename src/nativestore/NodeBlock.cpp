/**
Copyright 2020 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#include "NodeBlock.h"

#include <sstream>
#include <vector>

#include "../util/logger/Logger.h"
#include "RelationBlock.h"

Logger node_block_logger;
pthread_mutex_t lockSaveNode;
pthread_mutex_t lockAddNodeProperty;


NodeBlock::NodeBlock(std::string id, unsigned int nodeId, unsigned int address, unsigned int propRef, unsigned int edgeRef,unsigned int centralEdgeRef,
                     unsigned char edgeRefPID, char _label[], bool usage)
    : id(id),nodeId(nodeId), addr(address), propRef(propRef), edgeRef(edgeRef),centralEdgeRef(centralEdgeRef), edgeRefPID(edgeRefPID), usage(usage) {
    strcpy(label, _label);
};

bool NodeBlock::isInUse() { return this->usage == '\1'; }

std::string NodeBlock::getLabel() { return std::string(this->label); }

void NodeBlock::save() {
//    pthread_mutex_lock(&lockSaveNode);
    char _label[PropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(_label, id.c_str());

    bool isSmallLabel = id.length() <= sizeof(label)*2;
//    if (isSmallLabel) {
//        std::strcpy(this->label, this->id.c_str());
//    }
    NodeBlock::nodesDB->seekp(this->addr);
    NodeBlock::nodesDB->put(this->usage); // 1
    NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->nodeId)), sizeof(this->nodeId)); // 4
    NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->edgeRef)), sizeof(this->edgeRef)); // 4
    NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->centralEdgeRef)), sizeof(this->centralEdgeRef)); // 4
    NodeBlock::nodesDB->put(this->edgeRefPID); // 1
    NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->propRef)), sizeof(this->propRef)); // 4
    NodeBlock::nodesDB->write(this->label, sizeof(this->label)); // 6
    NodeBlock::nodesDB->flush();  // Sync the file with in-memory stream
//    pthread_mutex_unlock(&lockSaveNode);

//    if (!isSmallLabel) {
//        this->addProperty("label", _label);
//    }
}

void NodeBlock::addProperty(std::string name, char* value) {

    if (this->propRef == 0) {
        PropertyLink* newLink = PropertyLink::create(name, value);
//        pthread_mutex_lock(&lockAddNodeProperty);
        if (newLink) {
            this->propRef = newLink->blockAddress;
            // If it was an empty prop link before inserting, Then update the property reference of this node
            // block
//            node_block_logger.info("propRef = " + std::to_string(this->propRef));
            NodeBlock::nodesDB->seekp(this->addr + sizeof(this->nodeId) + sizeof(this->edgeRef) + sizeof(this->centralEdgeRef) + sizeof(this->edgeRefPID));
            NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(this->propRef)), sizeof(this->propRef));
            NodeBlock::nodesDB->flush();
        } else {
            throw "Error occurred while adding a new property link to " + std::to_string(this->addr) + " node block";
        }
//        pthread_mutex_unlock(&lockAddNodeProperty);
    }
    else {
        this->propRef = this->getPropertyHead()->insert(name, value);
    }
}

bool NodeBlock::updateRelation(RelationBlock* newRelation, bool relocateHead) {
    unsigned int edgeReferenceAddress = newRelation->addr;
    unsigned int thisAddress = this->addr;
    RelationBlock* currentHead = this->getRelationHead();
    if (relocateHead) {  // Insert new relation link to the head of the link list
        if (currentHead) {
            if (thisAddress == currentHead->source.address) {
                currentHead->setPreviousSource(newRelation->addr);
            } else if (thisAddress == currentHead->destination.address) {
                currentHead->setPreviousDestination(newRelation->addr);
            } else {
                throw std::to_string(thisAddress) +
                    " relation head does not contain current node in its source or destination";
            }
            if (thisAddress == newRelation->source.address) {
                newRelation->setNextSource(currentHead->addr);
            } else if (thisAddress == newRelation->destination.address) {
                newRelation->setNextDestination(currentHead->addr);
            } else {
                throw std::to_string(thisAddress) +
                    " new relation does not contain current node in its source or destination";
            }
        }
        return this->setRelationHead(*newRelation);
    } else {
        RelationBlock* currentRelation = currentHead;
        while (currentRelation != NULL) {
            if (currentRelation->source.address == this->addr) {
                if (currentRelation->source.nextRelationId == 0) {
                    return currentRelation->setNextSource(edgeReferenceAddress);
                } else {
                    currentRelation = currentRelation->nextSource();
                }
            } else if (!this->isDirected && currentRelation->destination.address == this->addr) {
                if (currentRelation->destination.nextRelationId == 0) {
                    return currentRelation->setNextDestination(edgeReferenceAddress);
                } else {
                    currentRelation = currentRelation->nextDestination();
                }
            } else {
                node_block_logger.warn("Invalid relation block" + std::to_string(currentRelation->addr));
            }
        }
        return false;
    }
    return false;
}


bool NodeBlock::updateCentralRelation(RelationBlock* newRelation, bool relocateHead) {
    unsigned int edgeReferenceAddress = newRelation->addr;
    unsigned int thisAddress = this->addr;
    RelationBlock* currentHead = this->getCentralRelationHead();
    if (relocateHead) {  // Insert new relation link to the head of the link list
        if (currentHead) {
            if (thisAddress == currentHead->source.address) {
                currentHead->setCentralPreviousSource(newRelation->addr);
            } else if (thisAddress == currentHead->destination.address) {
                currentHead->setCentralPreviousDestination(newRelation->addr);
            } else {
                throw std::to_string(thisAddress) +
                      " relation head does not contain current node in its source or destination";
            }
            if (thisAddress == newRelation->source.address) {
                newRelation->setCentralNextSource(currentHead->addr);
            } else if (thisAddress == newRelation->destination.address) {
                newRelation->setCentralNextDestination(currentHead->addr);
            } else {
                throw std::to_string(thisAddress) +
                      " new relation does not contain current node in its source or destination";
            }
        }
        return this->setCentralRelationHead(*newRelation);
    } else {
        RelationBlock* currentRelation = currentHead;
        while (currentRelation != NULL) {
            if (currentRelation->source.address == this->addr) {
                if (currentRelation->source.nextRelationId == 0) {
                    return currentRelation->setCentralNextSource(edgeReferenceAddress);
                } else {
                    currentRelation = currentRelation->nextCentralSource();
                }
            } else if (!this->isDirected && currentRelation->destination.address == this->addr) {
                if (currentRelation->destination.nextRelationId == 0) {
                    return currentRelation->setCentralNextDestination(edgeReferenceAddress);
                } else {
                    currentRelation = currentRelation->nextCentralDestination();
                }
            } else {
                node_block_logger.warn("Invalid relation block" + std::to_string(currentRelation->addr));
            }
        }
        return false;
    }
    return false;
}


RelationBlock* NodeBlock::getRelationHead() {
    RelationBlock* relationsHead = NULL;
    if (this->edgeRef != 0) {
        relationsHead = RelationBlock::get(this->edgeRef);
    }
    return relationsHead;
};

RelationBlock* NodeBlock::getCentralRelationHead() {
    RelationBlock* relationsHead = NULL;
    if (this->centralEdgeRef != 0) {
        relationsHead = RelationBlock::getCentral(this->centralEdgeRef);
    }
    return relationsHead;
};

bool NodeBlock::setRelationHead(RelationBlock newRelation) {
    unsigned int edgeReferenceAddress = newRelation.addr;
    int edgeReferenceOffset = sizeof(this->usage) + sizeof(this->nodeId);
    NodeBlock::nodesDB->seekp(this->addr + edgeReferenceOffset);
    if (!NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(edgeReferenceAddress)), sizeof(unsigned int))) {
        node_block_logger.error("ERROR: Error while updating edge reference address of " +
                                std::to_string(edgeReferenceAddress) + " for node " + std::to_string(this->addr));
        return false;
    }
    NodeBlock::nodesDB->flush();  // Sync the file with in-memory stream
    this->edgeRef = edgeReferenceAddress;
    return true;
}

bool NodeBlock::setCentralRelationHead(RelationBlock newRelation) {
    unsigned int centralEdgeReferenceAddress = newRelation.addr;
    int edgeReferenceOffset = sizeof(this->usage) + sizeof(this->nodeId);
    NodeBlock::nodesDB->seekp(this->addr + edgeReferenceOffset + sizeof(this->edgeRef));
    if (!NodeBlock::nodesDB->write(reinterpret_cast<char*>(&(centralEdgeReferenceAddress)), sizeof(unsigned int))) {
        node_block_logger.error("ERROR: Error while updating edge reference address of " +
                                std::to_string(centralEdgeReferenceAddress) + " for node " + std::to_string(this->addr));
        return false;
    }
    NodeBlock::nodesDB->flush();  // Sync the file with in-memory stream
    this->centralEdgeRef = centralEdgeReferenceAddress;
    return true;
}

/**
 * Return a pointer to matching relation block with the given node if found, Else return NULL
 * **/
RelationBlock* NodeBlock::searchRelation(NodeBlock withNode) {
    RelationBlock* found = NULL;
    RelationBlock* currentRelation = this->getRelationHead();
    while (currentRelation) {
        if (currentRelation->source.address == this->addr) {
            if (currentRelation->destination.address == withNode.addr) {
                found = currentRelation;
                break;
            } else {
                currentRelation = currentRelation->nextSource();
            }
        } else if (!this->isDirected && (currentRelation->destination.address == this->addr)) {
            if (currentRelation->source.address == withNode.addr) {
                found = currentRelation;
                break;
            } else {
                currentRelation = currentRelation->nextDestination();
            }
        } else {
            throw "Exception: Unrelated relation block for " + std::to_string(this->addr) +
                " found in relation block " + std::to_string(currentRelation->addr);
        }
    }

    return found;
}

RelationBlock* NodeBlock::searchCentralRelation(NodeBlock withNode) {
    RelationBlock* found = NULL;
    RelationBlock* currentRelation = this->getCentralRelationHead();
    while (currentRelation) {
        if (currentRelation->source.address == this->addr) {
            if (currentRelation->destination.address == withNode.addr) {
                found = currentRelation;
                break;
            } else {
                currentRelation = currentRelation->nextSource();
            }
        } else if (!this->isDirected && (currentRelation->destination.address == this->addr)) {
            if (currentRelation->source.address == withNode.addr) {
                found = currentRelation;
                break;
            } else {
                currentRelation = currentRelation->nextDestination();
            }
        } else {
            throw "Exception: Unrelated relation block for " + std::to_string(this->addr) +
                  " found in relation block " + std::to_string(currentRelation->addr);
        }
    }

    return found;
}


std::list<NodeBlock> NodeBlock::getEdges() {
    std::list<NodeBlock> edges;
    RelationBlock* currentRelation = this->getRelationHead();
    while (currentRelation != NULL) {
        NodeBlock* node = NULL;
        if (currentRelation->source.address == this->addr) {
            node = NodeBlock::get(currentRelation->destination.address);
            currentRelation = currentRelation->nextSource();
        } else if (currentRelation->destination.address == this->addr) {
            node = NodeBlock::get(currentRelation->source.address);
            currentRelation = currentRelation->nextDestination();
        } else {
            throw "Error: Unrecognized relation for " + std::to_string(this->addr) + " in relation block " +
                std::to_string(currentRelation->addr);
        }
        if (!node) {
            throw "Error creating node in the relation";
        }
        edges.push_back(*node);
    }
    return edges;
}

std::map<std::string, char*> NodeBlock::getAllProperties() {
    std::map<std::string, char*> allProperties;
    PropertyLink* current = this->getPropertyHead();
    while (current) {
        allProperties.insert({current->name, current->value});
        PropertyLink* temp = current->next();
        delete current;  // To prevent memory leaks
        current = temp;
    }
    delete current;
    return allProperties;
}

NodeBlock* NodeBlock::get(unsigned int blockAddress) {
    NodeBlock* nodeBlockPointer = NULL;
    NodeBlock::nodesDB->seekg(blockAddress);
    unsigned int nodeId;
    unsigned int edgeRef;
    unsigned int centralEdgeRef;
    unsigned char edgeRefPID;
    unsigned int propRef;
    char usageBlock;
    char label[NodeBlock::LABEL_SIZE];
    std::string id;

    if (!NodeBlock::nodesDB->get(usageBlock)) {
        node_block_logger.error("Error while reading usage data from block " + std::to_string(blockAddress));
    }
    if (!NodeBlock::nodesDB->read(reinterpret_cast<char*>(&nodeId), sizeof(unsigned int))) {
        node_block_logger.error("Error while reading nodeId  data from block " + std::to_string(blockAddress));
    }
    if (!NodeBlock::nodesDB->read(reinterpret_cast<char*>(&edgeRef), sizeof(unsigned int))) {
        node_block_logger.error("Error while reading edge reference data from block " + std::to_string(blockAddress));
    }
    if (!NodeBlock::nodesDB->read(reinterpret_cast<char*>(&centralEdgeRef), sizeof(unsigned int))) {
        node_block_logger.error("Error while reading central edge reference data from block " + std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char*>(&edgeRefPID), sizeof(unsigned char))) {
        node_block_logger.error("Error while reading edge reference partition ID data from block " +
                                std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(reinterpret_cast<char*>(&propRef), sizeof(unsigned int))) {
        node_block_logger.error("Error while reading prop reference data from block " + std::to_string(blockAddress));
    }

    if (!NodeBlock::nodesDB->read(&label[0], NodeBlock::LABEL_SIZE)) {
        node_block_logger.error("Error while reading label data from block " + std::to_string(blockAddress));
    }
    bool usage = usageBlock == '\1';
    node_block_logger.debug("Label = " + std::string(label));
    node_block_logger.debug("Label = " + std::string(label));
    node_block_logger.debug("Length of label = " + std::to_string(strlen(label)));
    node_block_logger.debug("edgeRef = " + std::to_string(edgeRef));
    if (strlen(label) != 0) {
        id = std::string(label);
    }
    nodeBlockPointer = new NodeBlock(id, nodeId,blockAddress, propRef, edgeRef,centralEdgeRef, edgeRefPID, label, usage);
    if (nodeBlockPointer->id.length() == 0) {  // if label not found in node block look in the properties
        std::map<std::string, char*> props = nodeBlockPointer->getAllProperties();
        if (props["label"]) {
            nodeBlockPointer->id = props["label"];
        } else {
            throw "Could not find node ID/Label for node with block address = " +
                std::to_string(nodeBlockPointer->addr);
        }
    }
    node_block_logger.debug("Edge ref = " + std::to_string(nodeBlockPointer->edgeRef));
    if (nodeBlockPointer->edgeRef % RelationBlock::BLOCK_SIZE != 0) {
        throw "Exception: Invalid edge reference address = " + nodeBlockPointer->edgeRef;
    }
    return nodeBlockPointer;
}

PropertyLink* NodeBlock::getPropertyHead() { return PropertyLink::get(this->propRef); }
std::fstream* NodeBlock::nodesDB = NULL;