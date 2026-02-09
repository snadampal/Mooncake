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

#ifndef EFA_CONTEXT_H_
#define EFA_CONTEXT_H_

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "transport/efa_transport/efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

class EfaTransport;
class EfaEndPoint;
class EfaWorkerPool;
class EfaEndpointStore;


struct EfaCq {
    EfaCq() : native(nullptr), outstanding(0) {}
    fid_cq *native;
    volatile int outstanding;
};


class EfaContext {
//    friend class EfaTransport;
 //   friend class EfaEndPoint;
  //  friend class EfaWorkerPool;

   public:
    EfaContext(EfaTransport &transport, const std::string &device_name);
    ~EfaContext();

    int construct(size_t num_cq = 1, size_t num_comp_channels = 1, uint8_t port = 1,
                  int gid_index = -1, size_t max_cqe = 4096, int max_ep = 256);

   private:
    int deconstruct();

   public:
    int registerMemoryRegion(void *addr, size_t length, uint64_t access);
    int unregisterMemoryRegion(void *addr);

    int preTouchMemory(void *addr, size_t length);

    uint64_t lkey(void *addr);
    uint64_t rkey(void *addr);

    std::shared_ptr<EfaEndPoint> endpoint(const std::string &peer_nic_path);
    int deleteEndpoint(const std::string &peer_nic_path);
    int cqCount() const { return cq_list_.size(); }

   fid_cq *cq();

       volatile int *cqOutstandingCount(int cq_index) {
        return &cq_list_[cq_index].outstanding;
    }


     std::atomic<int> next_cq_list_index_;
    const std::string &deviceName() const { return device_name_; }
    std::string getDeviceAddress() const;


    std::string nicPath() const;

    bool active() const { return active_; }
    void setActive(bool active) { active_ = active; }

    EfaTransport &engine() const { return engine_; }

    int submitPostSend(const std::vector<Transport::Slice *> &slice_list);

   public:
    struct MemoryRegion {
        struct fid_mr *mr;
        void *addr;
        size_t length;
        uint64_t lkey;
        uint64_t rkey;
    };

    EfaTransport &engine_;
    std::string device_name_;
    bool active_;

    // Libfabric resources
    struct fi_info *info_;
    struct fid_fabric *fabric_;
    struct fid_domain *domain_;
    struct fid_av *av_;
    std::vector<struct EfaCq> cq_list_;
    std::vector<struct fid_ep *> ep_list_;

    // Memory registration
    std::unordered_map<void *, MemoryRegion> mr_map_;
    std::mutex mr_mutex_;

    // Endpoints
    std::unordered_map<std::string, std::shared_ptr<EfaEndPoint>> endpoint_map_;
    std::mutex endpoint_mutex_;

    std::shared_ptr<EfaEndpointStore> endpoint_store_;

    std::vector<std::thread> background_thread_;
    std::atomic<bool> threads_running_;

    // Worker pool
    std::shared_ptr<EfaWorkerPool> worker_pool_;
};

}  // namespace mooncake

#endif  // EFA_CONTEXT_H_
