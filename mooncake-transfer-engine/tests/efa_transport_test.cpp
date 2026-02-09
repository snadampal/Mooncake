// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <thread>

#include "config.h"
#include "topology.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/efa_transport/efa_transport.h"

namespace mooncake {

class EfaTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize logging
        google::InitGoogleLogging("efa_transport_test");
        FLAGS_logtostderr = 1;

        // Create topology
        topology_ = std::make_shared<Topology>();
        
        // Try to detect EFA devices
        auto hca_list = topology_->getHcaList();
        if (hca_list.empty()) {
            GTEST_SKIP() << "No EFA devices found, skipping test";
        }

        // Create metadata with connection string
        metadata_ = std::make_shared<TransferMetadata>("P2PHANDSHAKE");
        
        // Create local server name
        local_server_name_ = "test_efa_server";
    }

    void TearDown() override {
        metadata_.reset();
        topology_.reset();
    }

    std::shared_ptr<Topology> topology_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
};

TEST_F(EfaTransportTest, InitializeTransport) {
    EfaTransport transport;
    int ret = transport.install(local_server_name_, metadata_, topology_);
    
    // On non-EFA instances, this will fail gracefully
    if (ret != 0) {
        LOG(INFO) << "EFA transport initialization failed (expected on non-EFA instances)";
        GTEST_SKIP() << "EFA devices not available on this instance type";
    }
    
    EXPECT_EQ(ret, 0) << "Failed to initialize EFA transport";
}

TEST_F(EfaTransportTest, GetTransportName) {
    EfaTransport transport;
    EXPECT_STREQ(transport.getName(), "efa");
}

TEST_F(EfaTransportTest, AllocateBatchWithoutInstall) {
    // Test that we can allocate batches even without installing
    EfaTransport transport;
    
    const size_t batch_size = 10;
    auto batch_id = transport.allocateBatchID(batch_size);
    EXPECT_NE(batch_id, 0);

    auto status = transport.freeBatchID(batch_id);
    EXPECT_TRUE(status.ok());
}

TEST_F(EfaTransportTest, RegisterMemory) {
    EfaTransport transport;
    int ret = transport.install(local_server_name_, metadata_, topology_);
    
    if (ret != 0) {
        GTEST_SKIP() << "EFA devices not available";
    }
    
    ASSERT_EQ(ret, 0);

    const size_t buffer_size = 4096;
    void *buffer = aligned_alloc(4096, buffer_size);
    ASSERT_NE(buffer, nullptr);

    ret = transport.registerLocalMemory(buffer, buffer_size, "cpu", true, true);
    EXPECT_EQ(ret, 0) << "Failed to register memory";

    ret = transport.unregisterLocalMemory(buffer, true);
    EXPECT_EQ(ret, 0) << "Failed to unregister memory";

    free(buffer);
}

TEST_F(EfaTransportTest, BatchMemoryRegistration) {
    EfaTransport transport;
    int ret = transport.install(local_server_name_, metadata_, topology_);
    
    if (ret != 0) {
        GTEST_SKIP() << "EFA devices not available";
    }
    
    ASSERT_EQ(ret, 0);

    const size_t buffer_size = 4096;
    const size_t num_buffers = 4;
    std::vector<void *> buffers;
    std::vector<Transport::BufferEntry> buffer_entries;

    for (size_t i = 0; i < num_buffers; ++i) {
        void *buffer = aligned_alloc(4096, buffer_size);
        ASSERT_NE(buffer, nullptr);
        buffers.push_back(buffer);
        buffer_entries.push_back({buffer, buffer_size});
    }

    ret = transport.registerLocalMemoryBatch(buffer_entries, "cpu");
    EXPECT_EQ(ret, 0) << "Failed to register memory batch";

    ret = transport.unregisterLocalMemoryBatch(buffers);
    EXPECT_EQ(ret, 0) << "Failed to unregister memory batch";

    for (auto buffer : buffers) {
        free(buffer);
    }
}

TEST_F(EfaTransportTest, AllocateBatch) {
    EfaTransport transport;
    int ret = transport.install(local_server_name_, metadata_, topology_);
    
    if (ret != 0) {
        GTEST_SKIP() << "EFA devices not available";
    }
    
    ASSERT_EQ(ret, 0);

    const size_t batch_size = 10;
    auto batch_id = transport.allocateBatchID(batch_size);
    EXPECT_NE(batch_id, 0);

    auto status = transport.freeBatchID(batch_id);
    EXPECT_TRUE(status.ok());
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
