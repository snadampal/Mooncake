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

#include "transport/efa_transport/efa_endpoint.h"

#include <glog/logging.h>
#include <rdma/fi_errno.h>

#include "common.h"
#include "config.h"
#include "topology.h"
#include "transport/efa_transport/efa_context.h"
#include "transport/transport.h"

namespace mooncake {

void convertHexToBuffer(const std::string& hex, uint8_t* out_buffer) {
    
// Parse Hex String (BYTE BY BYTE)
for (int i = 0; i < 32; i++) {
    out_buffer[i] = (uint8_t)std::stoul(hex.substr(i*2, 2), nullptr, 16);
}
}
	
EfaEndPoint::EfaEndPoint(EfaContext &context)
    : context_(context),
      status_(INITIALIZING),
      active_(true),
      cq_outstanding_(nullptr) { }

EfaEndPoint::~EfaEndPoint() {
    for (auto &qp : qp_list_) {
        if (qp) fi_close(&qp->fid);
    }
    qp_list_.clear();
}

int EfaEndPoint::submitPostSend(std::vector<Transport::Slice *> &slice_list,
                       std::vector<Transport::Slice *> &failed_slice_list)
{
	// Step 1: Acquire lock and check if endpoint is active
    // Equivalent to RDMA: RWSpinlock::WriteGuard guard(lock_);
    RWSpinlock::WriteGuard guard(lock_);
	
	if (!active_) return 0;


    // Step 2: Select a queue pair randomly (load balancing)
    // Equivalent to RDMA: int qp_index = SimpleRandom::Get().next(qp_list_.size());
    int qp_index = rand() % qp_list_.size();


        // Step 3: Calculate how many work requests we can submit
    // Limited by: QP depth, CQ capacity, and available slices
    // Equivalent to RDMA: Calculate wr_count based on max_wr_depth and cq_outstanding
    int wr_count = std::min(8, (int)slice_list.size());
			//std::min(max_wr_depth_ - wr_depth_list_[qp_index],
                        //    (int)slice_list.size());


        // Note: In EFA, we need to get cq_outstanding from context
    // For now, we'll use a simplified version without CQ limit check
    // TODO: Add CQ outstanding tracking in EfaContext
    // wr_count = std::min(int(globalConfig().max_cqe) - *cq_outstanding_, wr_count);


    // Step 4: Prepare work requests (libfabric equivalent)
    // In RDMA: ibv_send_wr wr_list[wr_count], ibv_sge sge_list[wr_count]
    // In libfabric: We'll use fi_msg_rma structures
    //
   
        // Allocate arrays for libfabric structures
    struct fi_msg_rma *msg_list = new struct fi_msg_rma[wr_count];
    struct iovec *iov_list = new struct iovec[wr_count];
    struct fi_rma_iov *rma_iov_list = new struct fi_rma_iov[wr_count];
    void **desc_list = new void*[wr_count];

    memset(msg_list, 0, sizeof(struct fi_msg_rma) * wr_count);

    // Step 5: Fill in work request structures
    // Equivalent to RDMA: Loop through slices and fill ibv_send_wr and ibv_sge
    for (int i = 0; i < wr_count; ++i) {
        auto slice = slice_list[i];

        // Fill scatter-gather element (local buffer)
        // Equivalent to RDMA: sge.addr, sge.length, sge.lkey
        auto &iov = iov_list[i];
        iov.iov_base = slice->source_addr;
        iov.iov_len = slice->length;

        // Fill remote memory region
        // Equivalent to RDMA: wr.wr.rdma.remote_addr, wr.wr.rdma.rkey
        auto &rma_iov = rma_iov_list[i];
        rma_iov.addr = slice->rdma.dest_addr;
        rma_iov.len = slice->length;
        rma_iov.key = slice->rdma.dest_rkey;

        // Get memory descriptor for local buffer
        // Equivalent to RDMA: sge.lkey (but libfabric uses descriptors)
        // We need to get the MR descriptor from the context
        // For now, set to nullptr and libfabric will look it up
        desc_list[i] = nullptr;  // TODO: Get proper descriptor from context

        // Fill message structure
        // Equivalent to RDMA: wr.wr_id, wr.opcode, wr.num_sge, wr.sg_list, etc.
        auto &msg = msg_list[i];
        msg.msg_iov = &iov;
        msg.desc = &desc_list[i];
        msg.iov_count = 1;
        msg.addr = peer_addr_list_[qp_index];  // Peer address from AV
        msg.rma_iov = &rma_iov;
        msg.rma_iov_count = 1;
        msg.context = slice;  // Equivalent to wr.wr_id
        msg.data = 0x0;  // Equivalent to wr.imm_data


        // Update slice metadata
        // Equivalent to RDMA: slice->ts, slice->status, slice->rdma.qp_depth
        slice->ts = getCurrentTimeInNano();
        slice->status = Transport::Slice::POSTED;
        slice->rdma.qp_depth = const_cast<int*>(&wr_depth_list_[qp_index]);
    }

    // Step 6: Update depth counters before posting
    // Equivalent to RDMA: __sync_fetch_and_add(&wr_depth_list_[qp_index], wr_count);
   //SN __sync_fetch_and_add(&wr_depth_list_[qp_index], wr_count);
    // __sync_fetch_and_add(cq_outstanding_, wr_count);  // TODO: Add when CQ tracking is available


    // Step 7: Post work requests to hardware
    // Equivalent to RDMA: ibv_post_send(qp_list_[qp_index], wr_list, &bad_wr);
    // In libfabric: We need to post each message individually or use batch API
    auto ep = qp_list_[qp_index];
    int failed_count = 0;
    int first_failed = -1;

    for (int i = 0; i < 1/* wr_count*/; ++i) {
        auto &msg = msg_list[i];
        auto slice = slice_list[i];

        int ret;
        // Determine operation type
        // Equivalent to RDMA: wr.opcode = IBV_WR_RDMA_READ or IBV_WR_RDMA_WRITE
        if (slice->opcode == Transport::TransferRequest::READ) {
            ret = fi_readmsg(ep, &msg, FI_COMPLETION);
        } else {
            ret = fi_writemsg(ep, &msg, FI_COMPLETION /*| FI_REMOTE_CQ_DATA*/);
        }

        // Step 8: Handle failures
        // Equivalent to RDMA: Check bad_wr and add failed slices to failed_slice_list
        if (ret) {
            if (first_failed == -1) {
                first_failed = i;
            }
            failed_count++;
            LOG(ERROR) << "fi_writemsg/fi_readmsg failed for slice " << i
                       << ": " << fi_strerror(-ret);
            failed_slice_list.push_back(slice);
            __sync_fetch_and_sub(&wr_depth_list_[qp_index], 1);
            // __sync_fetch_and_sub(cq_outstanding_, 1);  // TODO: Add when CQ tracking is available
        }
    }

    // Step 9: Clean up and remove processed slices from input list
    // Equivalent to RDMA: slice_list.erase(slice_list.begin(), slice_list.begin() + wr_count);
    slice_list.erase(slice_list.begin(), slice_list.begin() + wr_count);

    // Clean up allocated memory
    delete[] msg_list;
    delete[] iov_list;
    delete[] rma_iov_list;
    delete[] desc_list;

    return 0;

}

int EfaEndPoint::construct(fid_cq *cq, size_t num_qp_list,
                            size_t max_sge_per_wr, size_t max_wr_depth,
                            size_t max_inline_bytes) {
    if (status_.load(std::memory_order_relaxed) != INITIALIZING) {
        LOG(ERROR) << "Efa Endpoint has already been constructed";
        return ERR_ENDPOINT;
    }

    qp_list_.resize(num_qp_list);
    cq_outstanding_ = 0; //(volatile int *)cq->cq_context;

    auto &config = globalConfig();

        max_wr_depth_ = (int)max_wr_depth;
    wr_depth_list_ = new volatile int[num_qp_list];
    if (!wr_depth_list_) {
        LOG(ERROR) << "Failed to allocate memory for work request depth list";
        return ERR_MEMORY;
    }

    for (size_t i = 0; i < num_qp_list; ++i) {
        struct fid_ep *ep;
        int ret = fi_endpoint(context_.domain_, context_.info_, &ep, nullptr);
        if (ret) {
            LOG(ERROR) << "fi_endpoint failed: " << fi_strerror(-ret);
            return ERR_ENDPOINT;
        }

        // Bind to CQ
        size_t cq_index = i % context_.cq_list_.size();

	ret = fi_ep_bind(ep, &(context_.cq_list_[cq_index].native->fid),
                        FI_TRANSMIT | FI_RECV);
        if (ret) {
            LOG(ERROR) << "fi_ep_bind to CQ failed: " << fi_strerror(-ret);
            fi_close(&ep->fid);
            return ERR_ENDPOINT;
        }

        // Bind to AV
        ret = fi_ep_bind(ep, &context_.av_->fid, 0);
        if (ret) {
            LOG(ERROR) << "fi_ep_bind to AV failed: " << fi_strerror(-ret);
            fi_close(&ep->fid);
            return ERR_ENDPOINT;
        }

        // Enable endpoint
        ret = fi_enable(ep);
        if (ret) {
            LOG(ERROR) << "fi_enable failed: " << fi_strerror(-ret);
            fi_close(&ep->fid);
            return ERR_ENDPOINT;
        }

       size_t len = sizeof(local_ep_name);
       fi_getname(&ep->fid, local_ep_name, &len);

	qp_list_[i] = ep;
    }

    status_.store(UNCONNECTED, std::memory_order_relaxed);
    return 0;
}

int EfaEndPoint::createQueuePairs(size_t num_qp) {
    auto &config = globalConfig();
    return 0;
}

int EfaEndPoint::setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                           HandShakeDesc &local_desc) {
            RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        //LOG(WARNING) << "Re-establish connection: " << toString();
        //disconnectUnlocked();
    }

    if (peer_desc.peer_nic_path != context_.nicPath() ||
        peer_desc.local_nic_path != peer_nic_path_) {
        local_desc.reply_msg =
            "Invalid argument: peer nic path inconsistency, expect " +
            context_.nicPath() + " + " + peer_nic_path_ + ", while got " +
            peer_desc.peer_nic_path + " + " + peer_desc.local_nic_path;

        LOG(ERROR) << local_desc.reply_msg;
        return ERR_REJECT_HANDSHAKE;
    }

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_INVALID_ARGUMENT;
    }

    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = qpNum();

        auto segment_desc =
        context_.engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto &nic : segment_desc->devices) {
            if (nic.name == peer_nic_name)
                return doSetupConnection(nic.gid, nic.lid, peer_desc.qp_num,
                                        &local_desc.reply_msg);
        }
    }
    local_desc.reply_msg =
        "Peer nic not found in that server: " + peer_nic_path_;
    LOG(ERROR) << local_desc.reply_msg;
    return ERR_DEVICE_NOT_FOUND;

}


void EfaEndPoint::setPeerNicPath(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(WARNING) << "Previous connection will be discarded";
      //  disconnectUnlocked();
    }
    peer_nic_path_ = peer_nic_path;
}

int EfaEndPoint::setupConnectionsByActive() {
        RWSpinlock::WriteGuard guard(lock_);
    if (connected()) {
        LOG(INFO) << "Connection has been established";
        return 0;
    }

    // loopback mode
    if (context_.nicPath() == peer_nic_path_) {
	    auto segment_desc =
            context_.engine().meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (segment_desc) {
            for (auto &nic : segment_desc->devices)
                if (nic.name == context_.deviceName())
                    return doSetupConnection(nic.gid, nic.lid, qpNum());
        }
        LOG(ERROR) << "Peer NIC " << context_.deviceName()
                   << " not found in localhost";
        return ERR_DEVICE_NOT_FOUND;
    }

    HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = qpNum();

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        LOG(ERROR) << "Parse peer nic path failed: " << peer_nic_path_;
        return ERR_INVALID_ARGUMENT;
    }

    int rc = context_.engine().sendHandshake(peer_server_name, local_desc,
                                             peer_desc);

    if (rc) return rc;
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Reject the handshake request by peer "
                   << local_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    if (peer_desc.local_nic_path != peer_nic_path_ ||
        peer_desc.peer_nic_path != local_desc.local_nic_path) {
        LOG(ERROR) << "Invalid argument: received packet mismatch, "
                      "local.local_nic_path: "
                   << local_desc.local_nic_path
                   << ", local.peer_nic_path: " << local_desc.peer_nic_path
                   << ", peer.local_nic_path: " << peer_desc.local_nic_path
                   << ", peer.peer_nic_path: " << peer_desc.peer_nic_path;
        return ERR_REJECT_HANDSHAKE;
    }

    auto segment_desc =
        context_.engine().meta()->getSegmentDescByName(peer_server_name, true);

    if (segment_desc) {
        for (auto &nic : segment_desc->devices)
            if (nic.name == peer_nic_name)
                return doSetupConnection(nic.gid, nic.lid, peer_desc.qp_num);
    }
    LOG(ERROR) << "Peer NIC " << peer_nic_name << " not found in "
               << peer_server_name;
    return ERR_DEVICE_NOT_FOUND;
}

size_t EfaEndPoint::getQPNumber() const { return qp_list_.size(); }
    
std::vector<uint32_t> EfaEndPoint::qpNum() const {
    std::vector<uint32_t> ret;
    return ret;
} 

int EfaEndPoint::doSetupConnection(const std::string &peer_gid,
                                    uint16_t peer_lid,
                                    std::vector<uint32_t> peer_qp_num_list,
                                    std::string *reply_msg) {
    // Validate queue pairs exist
    if (qp_list_.empty()) {
        std::string message = "No queue pairs created";
        LOG(ERROR) << "[EFA Endpoint] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    // Validate peer QP number list size matches local QP list
    // Note: In EFA, peer_qp_num_list is not actually used (connectionless)
    // but we validate for consistency with RDMA API
    if (!peer_qp_num_list.empty() &&
        peer_qp_num_list.size() != qp_list_.size()) {
        std::string message = "QP count mismatch: local has " +
                            std::to_string(qp_list_.size()) +
                            " QPs, peer has " +
                            std::to_string(peer_qp_num_list.size()) + " QPs";
        LOG(WARNING) << "[EFA Endpoint] " << message;
        // Don't fail, just warn - EFA doesn't actually use QP numbers
    }

    // Setup connection for all queue pairs
    // This mirrors RDMA's approach of setting up each QP individually
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        uint32_t peer_qp_num = peer_qp_num_list.empty() ? 0 :
                              (qp_index < (int)peer_qp_num_list.size() ?
                               peer_qp_num_list[qp_index] : 0);

        int ret = doSetupConnection(qp_index, peer_gid, peer_lid,
                                   peer_qp_num, reply_msg);
        if (ret) {
            LOG(ERROR) << "[EFA Endpoint] Failed to setup connection for QP "
                       << qp_index << ", error: " << ret;
            return ret;
        }
    }

    // Mark endpoint as connected (equivalent to all QPs in RTS state)
    status_.store(CONNECTED, std::memory_order_relaxed);
    LOG(INFO) << "[EFA Endpoint] Successfully setup connections for all "
              << qp_list_.size() << " queue pairs";

    return 0;
}

int EfaEndPoint::doSetupConnection(int qp_index, const std::string &peer_gid,
                                    uint16_t peer_lid, uint32_t peer_qp_num,
                                    std::string *reply_msg) {
    // Note: peer_lid and peer_qp_num are not used in EFA (connectionless RDM)
    // They are kept for API compatibility with RDMA transport
    (void)peer_lid;
    (void)peer_qp_num;

    // Validate QP index
    if (qp_index < 0 || qp_index >= (int)qp_list_.size()) {
        std::string message = "Invalid QP index: " + std::to_string(qp_index) +
                            " (valid range: 0-" + std::to_string(qp_list_.size() - 1) + ")";
        LOG(ERROR) << "[EFA Endpoint] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_INVALID_ARGUMENT;
    }

    auto &ep = qp_list_[qp_index];
    // Validate peer address
    if (peer_gid.empty()) {
        std::string message = "Peer GID string is empty";
        LOG(ERROR) << "[EFA Endpoint] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_INVALID_ARGUMENT;
    }

    // Step 1: Verify endpoint is in proper state (equivalent to RESET state check in RDMA)
    // For libfabric RDM endpoints, we check if endpoint is enabled
    // Note: Unlike RDMA QP state machine (RESET→INIT→RTR→RTS), 
    // libfabric RDM endpoints don't have explicit state transitions
    if (!ep) {
        std::string message = "Endpoint is null at index " + std::to_string(qp_index);
        LOG(ERROR) << "[EFA Endpoint] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    // Step 2: Parse and validate peer address (equivalent to parsing GID in RDMA)
    // In RDMA: Parse peer_gid string into ibv_gid structure
    // In EFA: libfabric handles address format internally, we just validate
    LOG(INFO) << "[EFA Endpoint] Setting up connection for QP " << qp_index
              << " to peer GID: " << peer_gid;

    // Step 3: Insert peer address into Address Vector
    // This is the libfabric equivalent of RDMA's INIT→RTR transition where we:
    // - Set up Address Handle (AH) with peer GID, LID, port
    // - Configure path MTU, destination QP number
    // - Set RQ PSN, max_dest_rd_atomic, min_rnr_timer
    // 
    // In libfabric, fi_av_insert does all of this in one call:
    // - Resolves the peer address
    // - Creates internal routing information
    // - Returns a handle (fi_addr_t) for future operations

    uint8_t raw_addr[32];
convertHexToBuffer(peer_gid.c_str(), raw_addr);

    fi_addr_t peer_addr;
    int ret = fi_av_insert(context_.av_, (void*)raw_addr, 1,
                          &peer_addr, 0, nullptr);   

    if (ret < 0) {
        std::string message = "Failed to insert peer address into AV (fi_av_insert): " +
                            std::string(fi_strerror(-ret)) +
                            " - check peer GID format and network connectivity";
        LOG(ERROR) << "[EFA Endpoint] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    if (ret != 1) {
        std::string message = "fi_av_insert returned unexpected count: " +
                            std::to_string(ret) + " (expected 1)";
        LOG(ERROR) << "[EFA Endpoint] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }
    
    // Step 4: Store the peer address handle
    // This is equivalent to RDMA's RTR→RTS transition where we:
    // - Set timeout, retry_cnt, rnr_retry
    // - Set SQ PSN, max_rd_atomic
    // - Transition QP to Ready To Send state
    //
    // In libfabric RDM, the endpoint is already "ready to send" after fi_enable
    // We just need to store the peer address for use in send/recv operations
    if (qp_index >= (int)peer_addr_list_.size()) {
        peer_addr_list_.resize(qp_index + 1, FI_ADDR_UNSPEC);
    }
    peer_addr_list_[qp_index] = peer_addr;

    // Step 5: Verify endpoint is ready for communication
    // In RDMA, this verifies QP is in RTS state
    // In libfabric, we verify the peer address is valid
    if (peer_addr == FI_ADDR_UNSPEC) {
        std::string message = "Peer address is FI_ADDR_UNSPEC after insertion";
        LOG(ERROR) << "[EFA Endpoint] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_ENDPOINT;
    }

    LOG(INFO) << "[EFA Endpoint] Successfully setup connection for QP " << qp_index
              << " with peer address handle: 0x" << std::hex << peer_addr << std::dec;

    return 0;
}

std::string EfaEndPoint::getLocalEpName() const {

    // Convert to Hex String (BYTE BY BYTE)
std::stringstream ss;
for (int i = 0; i < 32; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)local_ep_name[i];
}
return ss.str();
}

}  // namespace mooncake
