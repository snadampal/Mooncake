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

#include "transport/efa_transport/efa_worker_pool.h"

#include <glog/logging.h>
#include <rdma/fi_errno.h>

#include "common.h"
#include "config.h"
#include "transport/efa_transport/efa_context.h"
#include "transport/efa_transport/efa_endpoint.h"
#include "transport/transport.h"

namespace mooncake {

const static int kTransferWorkerCount = 1; //globalConfig().workers_per_ctx;

EfaWorkerPool::EfaWorkerPool(EfaContext &context, int numa_socket_id)
    : context_(context),
      numa_socket_id_(numa_socket_id),
      workers_running_(true),
      suspended_flag_(0),
      redispatch_counter_(0),
      submitted_slice_count_(0),
      processed_slice_count_(0) {
    for (int i = 0; i < kShardCount; ++i)
        slice_queue_count_[i].store(0, std::memory_order_relaxed);
    collective_slice_queue_.resize(kTransferWorkerCount);
    for (int i = 0; i < kTransferWorkerCount; ++i)
        worker_thread_.emplace_back(
            std::thread(std::bind(&EfaWorkerPool::transferWorker, this, i)));
    worker_thread_.emplace_back(
        std::thread(std::bind(&EfaWorkerPool::monitorWorker, this)));
}           

EfaWorkerPool::~EfaWorkerPool() {
	    if (workers_running_) {
        cond_var_.notify_all();
        workers_running_.store(false);
        for (auto &entry : worker_thread_) entry.join();
    } 
}

int EfaWorkerPool::start() {
    if (running_.load()) {
        LOG(WARNING) << "Worker pool already running";
        return 0;
    }

    running_.store(true);
    return 0;
}

void EfaWorkerPool::stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);

    for (auto &worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    workers_.clear();
    LOG(INFO) << "Stopped worker pool";
}

void EfaWorkerPool::workerThread(size_t worker_id) {
    size_t cq_index = worker_id % context_.cq_list_.size();
    auto cq = context_.cq_list_[cq_index];
    LOG(INFO) << "Worker thread " << worker_id << " stopped";
}


int EfaWorkerPool::submitPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
#ifdef CONFIG_CACHE_SEGMENT_DESC
    thread_local uint64_t tl_last_cache_ts = getCurrentTimeInNano();
    thread_local std::unordered_map<SegmentID,
                                    std::shared_ptr<EfaTransport::SegmentDesc>>
        segment_desc_map;
    uint64_t current_ts = getCurrentTimeInNano();

    if (current_ts - tl_last_cache_ts > 1000000000) {
        segment_desc_map.clear();
        tl_last_cache_ts = current_ts;
    }

    for (auto &slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id);
            if (!segment_desc_map[target_id]) {
                segment_desc_map.clear();
                LOG(ERROR) << "Cannot get target segment description #"
                           << target_id;
                return ERR_INVALID_ARGUMENT;
            }
        }
    }
#else
    std::unordered_map<SegmentID, std::shared_ptr<EfaTransport::SegmentDesc>>
        segment_desc_map;
    for (auto &slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id))
		LOG(WARNING) << "getsegmentby ID fpr tatrget id" << target_id;
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id);
    }
#endif  // CONFIG_CACHE_SEGMENT_DESC
	//
    SliceList slice_list_map[kShardCount];
    uint64_t submitted_slice_count = 0;
    thread_local std::unordered_map<int, uint64_t> failed_target_ids;
    for (auto &slice : slice_list) {
        if (failed_target_ids.count(slice->target_id)) {
            auto ts = failed_target_ids[slice->target_id];
            if (getCurrentTimeInNano() - ts < 100000000ull) {
                slice->markFailed();
                continue;
            } else {
                failed_target_ids.erase(slice->target_id);
            }
        }
        auto &peer_segment_desc = segment_desc_map[slice->target_id];
        int buffer_id, device_id;
        auto hint = globalConfig().enable_dest_device_affinity
                        ? context_.deviceName()
                        : "";
        if (EfaTransport::selectDevice(peer_segment_desc.get(),
                                        slice->rdma.dest_addr, slice->length,
                                        hint, buffer_id, device_id)) {
            peer_segment_desc = context_.engine().meta()->getSegmentDescByID(
                slice->target_id, true);
            if (!peer_segment_desc) {
                LOG(ERROR) << "Cannot reload target segment #"
                           << slice->target_id;
                slice->markFailed();
                failed_target_ids[slice->target_id] = getCurrentTimeInNano();
                continue;
            }

            if (EfaTransport::selectDevice(
                    peer_segment_desc.get(), slice->rdma.dest_addr,
                    slice->length, hint, buffer_id, device_id)) {
                slice->markFailed();
                context_.engine().meta()->dumpMetadataContent(
                    peer_segment_desc->name, slice->rdma.dest_addr,
                    slice->length);
                continue;
            }
        }
        if (!peer_segment_desc) {
            slice->markFailed();
            continue;
        }
        slice->rdma.dest_rkey =
            peer_segment_desc->buffers[buffer_id].rkey[device_id];
        auto peer_nic_path =
            MakeNicPath(peer_segment_desc->name,
                        peer_segment_desc->devices[device_id].name);
        slice->peer_nic_path = peer_nic_path;
        int shard_id = (slice->target_id * 10007 + device_id) % kShardCount;
        slice_list_map[shard_id].push_back(slice);
        submitted_slice_count++;
    }

    for (int shard_id = 0; shard_id < kShardCount; ++shard_id) {
	    if (slice_list_map[shard_id].empty()) {
		    continue;
	    }
        slice_queue_lock_[shard_id].lock();
        for (auto &slice : slice_list_map[shard_id]){
	     	slice_queue_[shard_id][slice->peer_nic_path].push_back(slice);
	}
        slice_queue_count_[shard_id].fetch_add(slice_list_map[shard_id].size(),
                                               std::memory_order_relaxed);
        slice_queue_lock_[shard_id].unlock();
    }

    submitted_slice_count_.fetch_add(submitted_slice_count,
                                     std::memory_order_relaxed);

    if (suspended_flag_.load(std::memory_order_relaxed)) {
        std::lock_guard<std::mutex> lock(cond_mutex_);
        cond_var_.notify_all();
    }

    return 0;
}


void EfaWorkerPool::performPostSend(int thread_id) {
    auto &local_slice_queue = collective_slice_queue_[thread_id];
    for (int shard_id = thread_id; shard_id < kShardCount;
         shard_id += kTransferWorkerCount) {
     
	    if (slice_queue_count_[shard_id].load(std::memory_order_relaxed) == 0) {
		    continue;
	    }

        slice_queue_lock_[shard_id].lock();
        for (auto &entry : slice_queue_[shard_id]) {
            for (auto &slice : entry.second) {
                local_slice_queue[entry.first].push_back(slice);
	    }
            entry.second.clear();
        }
        slice_queue_count_[shard_id].store(0, std::memory_order_relaxed);
	slice_queue_lock_[shard_id].unlock();
    }

    // Redispatch slices to other endpoints, for temporary failures
    thread_local int tl_redispatch_counter = 0;
    if (tl_redispatch_counter <
        redispatch_counter_.load(std::memory_order_relaxed)) {
        tl_redispatch_counter =
            redispatch_counter_.load(std::memory_order_relaxed);
        auto local_slice_queue_clone = local_slice_queue;
        local_slice_queue.clear();
        for (auto &entry : local_slice_queue_clone)
            redispatch(entry.second, thread_id);
	return;
    }

#ifdef CONFIG_CACHE_ENDPOINT
    thread_local uint64_t tl_last_cache_ts = getCurrentTimeInNano();
    thread_local std::unordered_map<std::string, std::shared_ptr<EfaEndPoint>>
        endpoint_map;
    uint64_t current_ts = getCurrentTimeInNano();
    if (current_ts - tl_last_cache_ts > 1000000000) {
        endpoint_map.clear();
        tl_last_cache_ts = current_ts;
    }
#endif

    SliceList failed_slice_list;
    for (auto &entry : local_slice_queue) {
        if (entry.second.empty()) 
	{
		LOG(WARNING) << "for local slice queue, the second entry is empty";
		continue;
	}

#ifdef USE_FAKE_POST_SEND
        for (auto &slice : entry.second) slice->markSuccess();
        processed_slice_count_.fetch_add(entry.second.size());
        entry.second.clear();
#else
#ifdef CONFIG_CACHE_ENDPOINT
        auto &endpoint = endpoint_map[entry.first];
        if (endpoint == nullptr || !endpoint->active())
            endpoint = context_.endpoint(entry.first);
#else
        auto endpoint = context_.endpoint(entry.first);

    auto segmentDesc = context_.engine().metadata_->getSegmentDescByName(context_.engine().local_server_name_, true);

    segmentDesc->devices[0].gid = endpoint->getLocalEpName();

    context_.engine().metadata_->updateSegmentDesc(context_.engine().local_server_name_, *segmentDesc);


#endif
        if (!endpoint) {
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->active()) {
            if (endpoint->inactiveTime() > 1.0)
                context_.deleteEndpoint(
                    entry.first);  // enable for re-establishation
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->connected() && endpoint->setupConnectionsByActive()) {	
	    LOG(ERROR) << "Worker: Cannot make connection for endpoint: "
                       << entry.first << ", mark it inactive";
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            endpoint->set_active(false);
            failed_nr_polls++;
            if (context_.active() && failed_nr_polls > 32 &&
                !success_nr_polls) {
                //LOG(WARNING)
                 //   << "Failed to establish peer endpoints in local RNIC "
                  //  << context_.nicPath() << ", mark it inactive";
                //context_.set_active(false);
            }
            entry.second.clear();
            continue;
        }
        endpoint->submitPostSend(entry.second, failed_slice_list);
#endif
    }
    //SliceList failed_slice_list;
    for (auto &entry : local_slice_queue) {
        if (entry.second.empty()) continue;

#ifdef USE_FAKE_POST_SEND
        for (auto &slice : entry.second) slice->markSuccess();
        processed_slice_count_.fetch_add(entry.second.size());
        entry.second.clear();
#else
#ifdef CONFIG_CACHE_ENDPOINT
        auto &endpoint = endpoint_map[entry.first];
        if (endpoint == nullptr || !endpoint->active())
            endpoint = context_.endpoint(entry.first);
#else
        auto endpoint = context_.endpoint(entry.first);
#endif
        if (!endpoint) {
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->active()) {
            if (endpoint->inactiveTime() > 1.0)
                context_.deleteEndpoint(
                    entry.first);  // enable for re-establishation
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->connected() && endpoint->setupConnectionsByActive()) {
            LOG(ERROR) << "Worker: Cannot make connection for endpoint: "
                       << entry.first << ", mark it inactive";
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            endpoint->set_active(false);
            failed_nr_polls++;
            if (context_.active() && failed_nr_polls > 32 &&
                !success_nr_polls) {
                //LOG(WARNING)
                 //   << "Failed to establish peer endpoints in local RNIC "
                  //  << context_.nicPath() << ", mark it inactive";
                //context_.set_active(false);
            }
            entry.second.clear();
            continue;
        }
        endpoint->submitPostSend(entry.second, failed_slice_list);
#endif
    }
    if (!failed_slice_list.empty()) {
        for (auto &slice : failed_slice_list) slice->rdma.retry_cnt++;
        redispatch(failed_slice_list, thread_id);
    }
}

int EfaWorkerPool::performPollCq(int thread_id) {
        // Step 1: Initialize local variables
    // Equivalent to RDMA: int processed_slice_count = 0; const static size_t kPollCount = 64;
    int processed_slice_count = 0;
    const static size_t kPollCount = 64;
    std::unordered_map<volatile int *, int> qp_depth_set;

    // Step 2: Iterate over completion queues assigned to this thread
    // Equivalent to RDMA: for (int cq_index = thread_id; cq_index < context_.cqCount(); ...)
    // In EFA, we distribute CQs across workers
  //  const int kTransferWorkerCount = num_workers_;
    for (size_t cq_index = thread_id; cq_index < context_.cq_list_.size();
         cq_index += kTransferWorkerCount) {

        // Step 3: Prepare completion array
        // Equivalent to RDMA: ibv_wc wc[kPollCount];
        struct fi_cq_entry completions[kPollCount];

        // Step 4: Poll completion queue
        // Equivalent to RDMA: int nr_poll = context_.poll(kPollCount, wc, cq_index);
        auto cq = context_.cq_list_[cq_index].native;
        int nr_poll = fi_cq_read(cq, completions, kPollCount);

        // Handle different return values
        if (nr_poll == -FI_EAGAIN) {
            // No completions available, continue to next CQ
            continue;
        }


	        if (nr_poll < 0) {
            // Step 5: Handle errors
            // Equivalent to RDMA: if (nr_poll < 0) { LOG(ERROR) << "Failed to poll"; continue; }
            if (nr_poll == -FI_EAVAIL) {
                // Error available, read error queue
                struct fi_cq_err_entry err_entry = { } ;
                int err_ret = fi_cq_readerr(cq, &err_entry, 0);
                if (err_ret > 0) {
			// Process error completion
                    Transport::Slice *slice = (Transport::Slice *)err_entry.op_context;
                    if (slice) {
                        // Track QP depth for later update
                        if (qp_depth_set.count(slice->rdma.qp_depth))
                            qp_depth_set[slice->rdma.qp_depth]++;
                        else
                            qp_depth_set[slice->rdma.qp_depth] = 1;

                        // Log error
                        LOG(ERROR) << "Worker: Process failed for slice (opcode: "
                                   << slice->opcode
                                   << ", source_addr: " << slice->source_addr
                                   << ", length: " << slice->length
                                   << ", dest_addr: " << (void *)slice->rdma.dest_addr
                                   << ", local_nic: " << context_.deviceName()
                                   << ", peer_nic: " << slice->peer_nic_path
                                   << ", dest_rkey: " << slice->rdma.dest_rkey
                                   << ", retry_cnt: " << slice->rdma.retry_cnt
                                   << "): " << fi_cq_strerror(cq, err_entry.prov_errno,
                                                              err_entry.err_data, nullptr, 0);

                        failed_nr_polls++;

                        // Check if too many errors (mark context inactive)
                        if (context_.active() && failed_nr_polls > 32 && !success_nr_polls) {
                            LOG(WARNING) << "Too many errors found in local NIC "
                                       << context_.deviceName() << ", mark it inactive";
                            context_.setActive(false);
                        }

                        // Delete endpoint on error
                        context_.deleteEndpoint(slice->peer_nic_path);

                        // Retry logic
                        slice->rdma.retry_cnt++;
                        if (slice->rdma.retry_cnt >= slice->rdma.max_retry_cnt) {
                            slice->markFailed();
                            processed_slice_count_++;
                        } else {
                            // Add to redispatch queue
                            collective_slice_queue_[thread_id][slice->peer_nic_path]
                                .push_back(slice);
                            redispatch_counter_++;
                        }
                    }
                }
            } else {
                LOG(ERROR) << "Worker: Failed to poll completion queue: "
                          << fi_strerror(-nr_poll);
            }
            continue;
        }

		        // Step 6: Process successful completions
        // Equivalent to RDMA: for (int i = 0; i < nr_poll; ++i) { ... }
        for (int i = 0; i < nr_poll; ++i) {
            // Step 7: Extract slice from completion context
            // Equivalent to RDMA: Transport::Slice *slice = (Transport::Slice *)wc[i].wr_id;
            Transport::Slice *slice = (Transport::Slice *)completions[i].op_context;
            //SN assert(slice);
	    if (!slice) {
			continue;
	    }

            // Step 8: Track QP depth for batch update
            // Equivalent to RDMA: if (qp_depth_set.count(...)) qp_depth_set[...]++; else qp_depth_set[...] = 1;
            if (qp_depth_set.count(slice->rdma.qp_depth))
                qp_depth_set[slice->rdma.qp_depth]++;
            else
                qp_depth_set[slice->rdma.qp_depth] = 1;

            // Step 9: Check completion status
            // In libfabric, fi_cq_read only returns successful completions
            // Errors are retrieved via fi_cq_readerr
            // So if we're here, it's a success
            // Equivalent to RDMA: if (wc[i].status != IBV_WC_SUCCESS) { ... } else { ... }
           
	   
	    slice->markSuccess();
	  
	    processed_slice_count++;
            success_nr_polls++;
        }
        // Step 10: Update CQ outstanding counter
        // Equivalent to RDMA: if (nr_poll) __sync_fetch_and_sub(context_.cqOutstandingCount(cq_index), nr_poll);
        // TODO: Add CQ outstanding tracking to EfaContext
        // if (nr_poll)
        //     __sync_fetch_and_sub(context_.cqOutstandingCount(cq_index), nr_poll);
    }

    // Step 11: Batch update QP depth counters
    // Equivalent to RDMA: for (auto &entry : qp_depth_set) __sync_fetch_and_sub(entry.first, entry.second);
    for (auto &entry : qp_depth_set)
        __sync_fetch_and_sub(entry.first, entry.second);

    // Step 12: Update processed slice count
    // Equivalent to RDMA: if (processed_slice_count) processed_slice_count_.fetch_add(processed_slice_count);
    if (processed_slice_count)
        processed_slice_count_.fetch_add(processed_slice_count);

	return 0;
}

void EfaWorkerPool::redispatch(std::vector<Transport::Slice *> &slice_list,
                            int thread_id) {
    std::unordered_map<SegmentID, std::shared_ptr<Transport::SegmentDesc>>
        segment_desc_map;
    for (auto &slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id, true);
        }
    }

    for (auto &slice : slice_list) {
        if (slice->rdma.retry_cnt >= slice->rdma.max_retry_cnt) {
            slice->markFailed();
            processed_slice_count_++;
        } else {
            auto &peer_segment_desc = segment_desc_map[slice->target_id];
            int buffer_id, device_id;
            if (!peer_segment_desc ||
                EfaTransport::selectDevice(peer_segment_desc.get(),
                                            slice->rdma.dest_addr,
                                            slice->length, buffer_id, device_id,
                                            slice->rdma.retry_cnt)) {
                slice->markFailed();
                processed_slice_count_++;
                continue;
            }
            slice->rdma.dest_rkey =
                peer_segment_desc->buffers[buffer_id].rkey[device_id];
            auto peer_nic_path =
                MakeNicPath(peer_segment_desc->name,
                            peer_segment_desc->devices[device_id].name);
            slice->peer_nic_path = peer_nic_path;
            collective_slice_queue_[thread_id][peer_nic_path].push_back(slice);
        }
    }
}

void EfaWorkerPool::transferWorker(int thread_id) {
    const static uint64_t kWaitPeriodInNano = 100000000;  // 100ms
    uint64_t last_wait_ts = getCurrentTimeInNano();
    while (workers_running_.load(std::memory_order_relaxed)) {
	    
	    auto processed_slice_count =
            processed_slice_count_.load(std::memory_order_relaxed);
        auto submitted_slice_count =
            submitted_slice_count_.load(std::memory_order_relaxed);

	if (processed_slice_count == submitted_slice_count) {
            uint64_t curr_wait_ts = getCurrentTimeInNano();
            if (curr_wait_ts - last_wait_ts > kWaitPeriodInNano) {
                std::unique_lock<std::mutex> lock(cond_mutex_);
                suspended_flag_.fetch_add(1);
                // Double-check condition after acquiring lock to avoid lost
                // wakeup
                if (processed_slice_count_.load(std::memory_order_relaxed) ==
                    submitted_slice_count_.load(std::memory_order_relaxed)) {
                    cond_var_.wait_for(lock, std::chrono::seconds(2));
                }
		suspended_flag_.fetch_sub(1);
                last_wait_ts = curr_wait_ts;
		performPollCq(thread_id);
            }
            continue;
        }

        performPostSend(thread_id);
#ifndef USE_FAKE_POST_SEND
        performPollCq(thread_id);
#endif
    }
}
int EfaWorkerPool::doProcessContextEvents() {
    return 0;
}

void EfaWorkerPool::monitorWorker() {
}
}  // namespace mooncake
