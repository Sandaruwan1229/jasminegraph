/*
 * Copyright 2019 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef JASMINE_PARTITIONER_HEADER
#define JASMINE_PARTITIONER_HEADER
#include <vector>

#include "../../nativestore/NodeManager.h"
#include "./Partition.h"

typedef std::vector<std::pair<std::string, long>> partitionedEdge;
namespace spt { // spt : Streaming Partitioner
enum Algorithms { HASH, FENNEL, LDG };

}

class Partitioner {
    std::vector<Partition> partitions;
    int numberOfPartitions;
    long totalVertices = 0;
    long totalEdges = 0;
    int graphID;
    spt::Algorithms algorithmInUse;
    // perPartitionCap is : Number of vertices that can be store in this partition, This is a dynamic shared pointer
    // containing a value depending on the whole graph size and # of partitions

   public:
    Partitioner(int numberOfPartitions, int graphID, spt::Algorithms alog)
        : numberOfPartitions(numberOfPartitions), graphID(graphID), algorithmInUse(alog) {
        for (size_t i = 0; i < numberOfPartitions; i++) {
            this->partitions.push_back(Partition(i, numberOfPartitions));
        };
    };
    void printStats();
    long getTotalVertices();
    void setAlgorithm(std::string algo);
    partitionedEdge addEdge(std::pair<std::string, std::string> edge);
    partitionedEdge hashPartitioning(std::pair<std::string, std::string> edge);
    partitionedEdge fennelPartitioning(std::pair<std::string, std::string> edge);
    partitionedEdge ldgPartitioning(std::pair<std::string, std::string> edge);
    static std::pair<long, long> deserialize(std::string data);
};

#endif  // !JASMINE_PARTITIONER_HEADER