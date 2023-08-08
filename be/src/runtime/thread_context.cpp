// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/thread_context.h"

#include "common/signal_handler.h"
#include "runtime/runtime_state.h"

namespace doris {

DEFINE_STATIC_THREAD_LOCAL(ThreadContext, ThreadContextPtr, _ptr);

ThreadContextPtr::ThreadContextPtr() {
    INIT_STATIC_THREAD_LOCAL(ThreadContext, _ptr);
    init = true;
}

ScopeMemCount::ScopeMemCount(int64_t* scope_mem) {
    _scope_mem = scope_mem;
    thread_context()->thread_mem_tracker_mgr->start_count_scope_mem();
}

ScopeMemCount::~ScopeMemCount() {
    *_scope_mem += thread_context()->thread_mem_tracker_mgr->stop_count_scope_mem();
}

AttachTask::AttachTask(const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
                       const std::string& task_id, const TUniqueId& fragment_instance_id) {
    SwitchBthreadLocal::switch_to_bthread_local();
    thread_context()->attach_task(task_id, fragment_instance_id, mem_tracker);
}

AttachTask::AttachTask(RuntimeState* runtime_state) {
    SwitchBthreadLocal::switch_to_bthread_local();
    signal::set_signal_task_id(runtime_state->query_id());
    thread_context()->attach_task(print_id(runtime_state->query_id()),
                                  runtime_state->fragment_instance_id(),
                                  runtime_state->query_mem_tracker());
}

AttachTask::~AttachTask() {
    thread_context()->detach_task();
    SwitchBthreadLocal::switch_back_pthread_local();
}

SwitchThreadMemTrackerLimiter::SwitchThreadMemTrackerLimiter(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker) {
    SwitchBthreadLocal::switch_to_bthread_local();
    _old_mem_tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
    thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(mem_tracker, TUniqueId());
}

SwitchThreadMemTrackerLimiter::~SwitchThreadMemTrackerLimiter() {
    thread_context()->thread_mem_tracker_mgr->detach_limiter_tracker(_old_mem_tracker);
    SwitchBthreadLocal::switch_back_pthread_local();
}

AddThreadMemTrackerConsumer::AddThreadMemTrackerConsumer(MemTracker* mem_tracker) {
    SwitchBthreadLocal::switch_to_bthread_local();
    if (mem_tracker) {
        _need_pop = thread_context()->thread_mem_tracker_mgr->push_consumer_tracker(mem_tracker);
    }
}

AddThreadMemTrackerConsumer::AddThreadMemTrackerConsumer(
        const std::shared_ptr<MemTracker>& mem_tracker)
        : _mem_tracker(mem_tracker) {
    SwitchBthreadLocal::switch_to_bthread_local();
    if (_mem_tracker) {
        _need_pop =
                thread_context()->thread_mem_tracker_mgr->push_consumer_tracker(_mem_tracker.get());
    }
}

AddThreadMemTrackerConsumer::~AddThreadMemTrackerConsumer() {
    if (_need_pop) {
        thread_context()->thread_mem_tracker_mgr->pop_consumer_tracker();
    }
    SwitchBthreadLocal::switch_back_pthread_local();
}

} // namespace doris
