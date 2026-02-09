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

#ifndef EFA_ENDPOINT_H_
#define EFA_ENDPOINT_H_

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>

#include <memory>
#include <string>
#include <vector>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

class EfaContext;

class EfaEndPoint {
    //friend class EfaContext;
    //friend class EfaWorkerPool;

   public:
    enum Status {
        INITIALIZING,
        UNCONNECTED,
        CONNECTED,
    };

    void setPeerNicPath(const std::string &peer_nic_path);
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    EfaEndPoint(EfaContext &context);
    ~EfaEndPoint();

        int construct(fid_cq *cq, size_t num_qp_list = 2, size_t max_sge = 4,
                  size_t max_wr = 256, size_t max_inline = 64);

    int setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                  HandShakeDesc &local_desc);

    int setupConnectionsByActive();

    int setupConnectionsByActive(const std::string &peer_nic_path) {
        setPeerNicPath(peer_nic_path);
        return setupConnectionsByActive();
    }

    bool hasOutstandingSlice() const;

    bool active() const { return active_; }

    void set_active(bool flag) {
        RWSpinlock::WriteGuard guard(lock_);
        active_ = flag;
        if (!flag) inactive_time_ = getCurrentTimeInNano();
    }

    double inactiveTime() {
        if (active_) return 0.0;
        return (getCurrentTimeInNano() - inactive_time_) / 1000000000.0;
    }


   public:
    bool connected() const {
        return status_.load(std::memory_order_relaxed) == CONNECTED;
    }

    // Interrupts the connection, which can be triggered by user or by internal
    // error. Use setupConnectionsByActive or setupConnectionsByPassive to
    // reconnect
    void disconnect();

    // Destroy QPs before CQs (in RDMA Context)
    int destroyQP();

       private:
    void disconnectUnlocked();

   public:
    const std::string toString() const;

   public:
    // Submit some work requests to HW
    // Submitted tasks (success/failed) are removed in slice_list
    // Failed tasks (which must be submitted) are inserted in failed_slice_list
    int submitPostSend(std::vector<Transport::Slice *> &slice_list,
                       std::vector<Transport::Slice *> &failed_slice_list);

    // Get the number of QPs in this endpoint
    size_t getQPNumber() const;

     std::string getLocalEpName() const;

   private:
    std::vector<uint32_t> qpNum() const;

    int doSetupConnection(const std::string &peer_gid, uint16_t peer_lid,
                          std::vector<uint32_t> peer_qp_num_list,
                          std::string *reply_msg = nullptr);

    int doSetupConnection(int qp_index, const std::string &peer_gid,
                          uint16_t peer_lid, uint32_t peer_qp_num,
                          std::string *reply_msg = nullptr);


    int postSend(Transport::Slice *slice);

   private:
    int createQueuePairs(size_t num_qp);

    std::atomic<Status> status_;
    RWSpinlock lock_;

    EfaContext &context_;
    std::string peer_nic_path_;
    uint8_t local_ep_name[64];
    std::vector<struct fid_ep *> qp_list_;
    std::vector<fi_addr_t> peer_addr_list_;
    bool connected_;


    volatile int *wr_depth_list_;
    int max_wr_depth_;

    volatile bool active_;
    volatile int *cq_outstanding_;
    volatile uint64_t inactive_time_;

};

}  // namespace mooncake

#endif  // EFA_ENDPOINT_H_
