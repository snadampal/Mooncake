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

#include "transport/efa_transport/efa_context.h"

#include <glog/logging.h>
#include <rdma/fi_errno.h>

#include "common.h"
#include "config.h"
#include "transport/efa_transport/efa_endpoint.h"
#include "transport/efa_transport/efa_worker_pool.h"
#include "transport/efa_transport/efa_endpoint_store.h"
#include "transport/efa_transport/efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

EfaContext::EfaContext(EfaTransport &engine, const std::string &device_name)
    : engine_(engine),
      device_name_(device_name),
      active_(false),
      info_(nullptr),
      fabric_(nullptr),
      domain_(nullptr),
      av_(nullptr),
	next_cq_list_index_(0),
      worker_pool_(nullptr)	{}

EfaContext::~EfaContext() {
    if (worker_pool_) {
        worker_pool_->stop();
        worker_pool_.reset();
    }

    endpoint_map_.clear();

    for (auto &ep : ep_list_) {
        if (ep) fi_close(&ep->fid);
    }
    ep_list_.clear();

        for (size_t i = 0; i < cq_list_.size(); ++i) {
        if (!cq_list_[i].native) continue;

        int ret = fi_close(&((cq_list_[i].native)->fid));
        if (ret) {
            PLOG(ERROR) << "Failed to destroy completion queue";
        }
    }
    cq_list_.clear();

    {
        std::lock_guard<std::mutex> lock(mr_mutex_);
        for (auto &entry : mr_map_) {
            if (entry.second.mr) {
                fi_close(&entry.second.mr->fid);
            }
        }
        mr_map_.clear();
    }

    if (av_) fi_close(&av_->fid);
    if (domain_) fi_close(&domain_->fid);
    if (fabric_) fi_close(&fabric_->fid);
    if (info_) fi_freeinfo(info_);
}

int EfaContext::construct(size_t num_cq_list, size_t num_comp_channels,
                          uint8_t port, int gid_index, size_t max_cqe,
                          int max_ep) {
    (void)port;
    (void)gid_index;
    (void)num_comp_channels;

        // Create endpoint store based on configuration
    auto &config = globalConfig();
    switch (config.endpoint_store_type) {
        case EndpointStoreType::FIFO:
            endpoint_store_ =
                std::make_shared<FIFOEfaEndpointStore>(max_ep);
            LOG(INFO) << "Using FIFO endpoint store";
            break;
        case EndpointStoreType::SIEVE:
        default:
            endpoint_store_ =
                std::make_shared<SIEVEEfaEndpointStore>(max_ep);
            LOG(INFO) << "Using SIEVE endpoint store";
            break;
    }
    
    struct fi_info *hints = fi_allocinfo();
    if (!hints) {
        LOG(ERROR) << "Failed to allocate fi_info";
        return ERR_MEMORY;
    }

    // Configure hints for EFA
    hints->ep_attr->type = FI_EP_RDM;  // Reliable datagram endpoint
    hints->caps = FI_MSG | FI_RMA | FI_TAGGED;
    hints->mode = FI_CONTEXT;
    hints->domain_attr->mr_mode = FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY; //FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR | FI_MR_HMEM;
    hints->domain_attr->threading = FI_THREAD_SAFE;
    hints->domain_attr->control_progress = FI_PROGRESS_MANUAL;
    hints->domain_attr->data_progress = FI_PROGRESS_MANUAL;  	    
    hints->fabric_attr->prov_name = strdup("efa");

    int ret = fi_getinfo(FI_VERSION(1, 9), nullptr, nullptr, 0, hints, &info_);
    fi_freeinfo(hints);

    if (ret) {
        LOG(ERROR) << "fi_getinfo failed for device " << device_name_
                   << ": " << fi_strerror(-ret);
        return ERR_DEVICE_NOT_FOUND;
    }

    // Open fabric
    ret = fi_fabric(info_->fabric_attr, &fabric_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_fabric failed: " << fi_strerror(-ret);
        return ERR_DEVICE_NOT_FOUND;
    }

    // Open domain
    ret = fi_domain(fabric_, info_, &domain_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_domain failed: " << fi_strerror(-ret);
        return ERR_DEVICE_NOT_FOUND;
    }

    // Create address vector
    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    av_attr.count = max_ep;
    ret = fi_av_open(domain_, &av_attr, &av_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_av_open failed: " << fi_strerror(-ret);
        return ERR_DEVICE_NOT_FOUND;
    }

    // Create completion queues
    struct fi_cq_attr cq_attr = {};
    cq_attr.size = max_cqe;
    cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    cq_attr.wait_obj = FI_WAIT_NONE;

    cq_list_.resize(num_cq_list);
    for (size_t i = 0; i < num_cq_list; ++i) {
        struct fid_cq *cq;
        ret = fi_cq_open(domain_, &cq_attr, &cq, nullptr);
        if (ret) {
            LOG(ERROR) << "fi_cq_open failed: " << fi_strerror(-ret);
            return ERR_DEVICE_NOT_FOUND;
        }
        cq_list_[i].native = cq;
    }

    // Create worker pool
    size_t num_workers = config.workers_per_ctx;
    worker_pool_ = std::make_shared<EfaWorkerPool>(*this, num_workers);
    ret = worker_pool_->start();
    if (ret) {
        LOG(ERROR) << "Failed to start worker pool";
        return ret;
    }

    active_ = true;
    LOG(INFO) << "EFA context initialized for device " << device_name_;
    return 0;
}

int EfaContext::registerMemoryRegion(void *addr, size_t length,
                                     uint64_t access) {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    if (mr_map_.find(addr) != mr_map_.end()) {
        LOG(WARNING) << "Memory region already registered: " << addr;
        return 0;
    }

    struct fid_mr *mr;
    int ret = fi_mr_reg(domain_, addr, length, access, 0, 0, 0, &mr, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_mr_reg failed: " << fi_strerror(-ret);
        return ERR_MEMORY;
    }
    MemoryRegion mr_entry;
    mr_entry.mr = mr;
    mr_entry.addr = addr;
    mr_entry.length = length;
    mr_entry.lkey = fi_mr_key(mr);
    mr_entry.rkey = fi_mr_key(mr);

    mr_map_[addr] = mr_entry;
    return 0;
}

int EfaContext::unregisterMemoryRegion(void *addr) {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    auto it = mr_map_.find(addr);
    if (it == mr_map_.end()) {
        LOG(WARNING) << "Memory region not found: " << addr;
        return ERR_ADDRESS_NOT_REGISTERED;
    }

    if (it->second.mr) {
        int ret = fi_close(&it->second.mr->fid);
        if (ret) {
            LOG(ERROR) << "fi_close failed for MR: " << fi_strerror(-ret);
            return ERR_MEMORY;
        }
    }

    mr_map_.erase(it);
    return 0;
}

uint64_t EfaContext::lkey(void *addr) {
    std::lock_guard<std::mutex> lock(mr_mutex_);
    auto it = mr_map_.find(addr);
    if (it != mr_map_.end()) {
        return it->second.lkey;
    }
    return 0;
}

uint64_t EfaContext::rkey(void *addr) {
    std::lock_guard<std::mutex> lock(mr_mutex_);
    auto it = mr_map_.find(addr);
    if (it != mr_map_.end()) {
        return it->second.rkey;
    }
    return 0;
}


fid_cq *EfaContext::cq() {
    int index = (next_cq_list_index_++) % cq_list_.size();
    return cq_list_[index].native;
}

std::shared_ptr<EfaEndPoint> EfaContext::endpoint(
    const std::string &peer_nic_path) {
    std::lock_guard<std::mutex> lock(endpoint_mutex_);

    if (!active_) {
        LOG(ERROR) << "Context is not active: " << deviceName();
        return nullptr;
    }
        
    if (peer_nic_path.empty()) {
        LOG(ERROR) << "Invalid peer NIC path: " << deviceName();
        return nullptr;
    }
    
    auto endpoint = endpoint_store_->getEndpoint(peer_nic_path);
    if (endpoint) { 
	        LOG(WARNING) << "retutning ep from cache";
        return endpoint;
    }
        
    endpoint = endpoint_store_->insertEndpoint(peer_nic_path, this);
    endpoint_store_->reclaimEndpoint();
 
    return endpoint;
}

int EfaContext::deleteEndpoint(const std::string &peer_nic_path) {
    std::lock_guard<std::mutex> lock(endpoint_mutex_);
    auto it = endpoint_map_.find(peer_nic_path);
    if (it != endpoint_map_.end()) {
        endpoint_map_.erase(it);
        return 0;
    }
    return ERR_ENDPOINT;
}

std::string EfaContext::getDeviceAddress() const {
    if (!info_ || !info_->src_addr) {
        return "";
    }
    // Return a string representation of the address
    char addr_str[256];
    size_t addr_len = sizeof(addr_str);
    fi_av_straddr(av_, info_->src_addr, addr_str, &addr_len);
    return std::string(addr_str);
}

int EfaContext::preTouchMemory(void *addr, size_t length) {
    volatile char *ptr = static_cast<volatile char *>(addr);
    const size_t page_size = 4096;
    for (size_t i = 0; i < length; i += page_size) {
        ptr[i] = ptr[i];
    }
    return 0;
}

int EfaContext::submitPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
    return worker_pool_->submitPostSend(slice_list);
}

std::string EfaContext::nicPath() const {
    return MakeNicPath(engine_.local_server_name_, device_name_);
}

}  // namespace mooncake
