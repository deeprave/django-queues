#!lua name=django_queues
-- django-queues-library-version: 260822_160000
-- django-queues-api-version: 1

local library_version = "260822_160000"
local api_version = 1

local function register_function(name, description, callback)
    redis.register_function{
        function_name = name,
        description = description,
        callback = callback,
    }
end

local function push_priority(priority_key, sequence_key, base_score, entry_id)
    local sequence = redis.call('INCR', sequence_key)
    redis.call('ZADD', priority_key, base_score - sequence, entry_id)
end

local function pop_pending(pending_key, stack)
    if stack then return redis.call('RPOP', pending_key) end
    return redis.call('LPOP', pending_key)
end

local function pop_priority(priority_key, sequence_key)
    local top = redis.call('ZREVRANGE', priority_key, 0, 0)
    if #top == 0 then return nil end
    redis.call('ZREM', priority_key, top[1])
    if redis.call('ZCARD', priority_key) == 0 then
        redis.call('SET', sequence_key, 0, 'XX')
    end
    return top[1]
end

local function promote_one_scheduled(scheduled_key, entry_key_prefix, pending_key, stack, now_us)
    local earliest = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_us, 'WITHSCORES', 'LIMIT', 0, 1)
    if #earliest == 0 then return end
    local scheduled = redis.call('ZRANGEBYSCORE', scheduled_key, earliest[2], earliest[2])
    for index = 1, #scheduled do
        local entry_id = scheduled[index]
        local raw_entry = redis.call('GET', entry_key_prefix .. entry_id)
        local ok, entry = pcall(cjson.decode, raw_entry)
        if ok and type(entry) == 'table' and entry.status == 'queued' then
            if stack then redis.call('LPUSH', pending_key, entry_id)
            else redis.call('RPUSH', pending_key, entry_id) end
            redis.call('ZREM', scheduled_key, entry_id)
            return
        end
        redis.call('ZREM', scheduled_key, entry_id)
    end
end

local function promote_one_scheduled_priority(scheduled_key, entry_key_prefix, priority_key, sequence_key, priority_space, now_us)
    local earliest = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_us, 'WITHSCORES', 'LIMIT', 0, 1)
    if #earliest == 0 then return end
    local scheduled = redis.call('ZRANGEBYSCORE', scheduled_key, earliest[2], earliest[2])
    local selected_id
    local selected_priority
    for index = 1, #scheduled do
        local entry_id = scheduled[index]
        local raw_entry = redis.call('GET', entry_key_prefix .. entry_id)
        local ok, entry = pcall(cjson.decode, raw_entry)
        if ok and type(entry) == 'table' and entry.status == 'queued' then
            local priority = tonumber(entry.priority) or 0
            if not selected_id or priority > selected_priority then
                selected_id, selected_priority = entry_id, priority
            end
        else
            redis.call('ZREM', scheduled_key, entry_id)
        end
    end
    if selected_id then
        push_priority(priority_key, sequence_key, selected_priority * priority_space, selected_id)
        redis.call('ZREM', scheduled_key, selected_id)
    end
end

register_function('django_queue_info', 'Keys: none. Args: none. Returns: library version and API version.', function(keys, args)
    return {library_version, api_version}
end)

register_function('django_queue_store_and_push', 'Keys: entry record, pending list. Args: entry JSON, stack flag, entry ID. Returns: no value.', function(keys, args)
    redis.call('SET', keys[1], args[1])
    if args[2] == '1' then
        redis.call('LPUSH', keys[2], args[3])
    else
        redis.call('RPUSH', keys[2], args[3])
    end
end)

register_function('django_queue_push_priority', 'Keys: priority ZSET, priority sequence. Args: priority base score, entry ID. Returns: no value.', function(keys, args)
    push_priority(keys[1], keys[2], tonumber(args[1]), args[2])
end)

register_function('django_queue_pop_priority', 'Keys: priority ZSET, priority sequence. Args: none. Returns: highest-priority entry ID, or nil when empty.', function(keys, args)
    return pop_priority(keys[1], keys[2])
end)

register_function('django_queue_discard_priority', 'Keys: priority ZSET, priority sequence. Args: entry ID. Returns: no value.', function(keys, args)
    redis.call('ZREM', keys[1], args[1])
    if redis.call('ZCARD', keys[1]) == 0 then
        redis.call('SET', keys[2], 0, 'XX')
    end
end)

redis.register_function{
    function_name = 'django_queue_promote_scheduled',
    description = 'Keys: scheduled ZSET, entry-key prefix, pending list. Args: stack flag. Returns: no value after promoting at most one due availability group.',
    callback = function(keys, args)
        local now = redis.call('TIME')
        local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
        promote_one_scheduled(keys[1], keys[2], keys[3], args[1] == '1', now_us)
    end,
}

redis.register_function{
    function_name = 'django_queue_promote_scheduled_priority',
    description = 'Keys: scheduled ZSET, entry-key prefix, priority ZSET, priority sequence. Args: priority score space. Returns: no value after promoting one highest-priority valid entry from the earliest due availability group.',
    callback = function(keys, args)
        local now = redis.call('TIME')
        local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
        promote_one_scheduled_priority(keys[1], keys[2], keys[3], keys[4], tonumber(args[1]), now_us)
    end,
}

register_function('django_queue_dequeue', 'Keys: scheduled ZSET, entry-key prefix, pending list. Args: stack flag. Returns: next due entry ID, or nil when empty.', function(keys, args)
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    local stack = args[1] == '1'
    promote_one_scheduled(keys[1], keys[2], keys[3], stack, now_us)
    return pop_pending(keys[3], stack)
end)

register_function('django_queue_dequeue_priority', 'Keys: scheduled ZSET, entry-key prefix, priority ZSET, priority sequence. Args: priority score space. Returns: next due entry ID, or nil when empty.', function(keys, args)
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    promote_one_scheduled_priority(keys[1], keys[2], keys[3], keys[4], tonumber(args[1]), now_us)
    return pop_priority(keys[3], keys[4])
end)

register_function('django_queue_store_and_push_priority', 'Keys: entry record, priority ZSET, priority sequence. Args: entry JSON, entry ID, priority base score. Returns: no value.', function(keys, args)
    redis.call('SET', keys[1], args[1])
    push_priority(keys[2], keys[3], tonumber(args[3]), args[2])
end)

register_function('django_queue_store_available', 'Keys: entry record, pending list, scheduled ZSET, priority ZSET, priority sequence. Args: entry JSON, entry ID, available time, priority base score, priority flag, stack flag. Returns: no value.', function(keys, args)
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    redis.call('SET', keys[1], args[1])
    if tonumber(args[3]) > now_us then
        redis.call('ZADD', keys[3], args[3], args[2])
    elseif args[5] == '1' then
        push_priority(keys[4], keys[5], tonumber(args[4]), args[2])
    elseif args[6] == '1' then
        redis.call('LPUSH', keys[2], args[2])
    else
        redis.call('RPUSH', keys[2], args[2])
    end
end)

register_function('django_queue_store_and_discard', 'Keys: entry record, pending list, priority ZSET, priority sequence, scheduled ZSET. Args: entry JSON, entry ID. Returns: no value.', function(keys, args)
    redis.call('SET', keys[1], args[1])
    redis.call('LREM', keys[2], 0, args[2])
    redis.call('ZREM', keys[3], args[2])
    redis.call('ZREM', keys[5], args[2])
    if redis.call('ZCARD', keys[3]) == 0 then
        redis.call('SET', keys[4], 0, 'XX')
    end
end)

register_function('django_queue_store_event_and_push', 'Keys: entry record, unclaimed-deadline ZSET, pending list. Args: entry JSON, stack flag, entry ID, deadline. Returns: no value.', function(keys, args)
    redis.call('SET', keys[1], args[1])
    redis.call('ZADD', keys[2], args[4], args[3])
    if args[2] == '1' then
        redis.call('LPUSH', keys[3], args[3])
    else
        redis.call('RPUSH', keys[3], args[3])
    end
end)

register_function('django_queue_dequeue_event', 'Keys: pending list, delayed ZSET, claim-key prefix, claim-deadline ZSET, entry-key prefix, unclaimed-deadline ZSET. Args: stack flag. Returns: dequeued outcome and entry JSON, or empty outcome.', function(keys, args)
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    local delayed = redis.call('ZRANGEBYSCORE', keys[2], '-inf', now_us)
    for index = 1, #delayed do
        if args[1] == '1' then redis.call('LPUSH', keys[1], delayed[index])
        else redis.call('RPUSH', keys[1], delayed[index]) end
        redis.call('ZREM', keys[2], delayed[index])
    end
    local pending_count = redis.call('LLEN', keys[1])
    for _ = 1, pending_count do
        local entry_id
        if args[1] == '1' then entry_id = redis.call('RPOP', keys[1])
        else entry_id = redis.call('LPOP', keys[1]) end
        if not entry_id then break end
        local claim_key = keys[3] .. entry_id
        if redis.call('GET', claim_key) then
            if args[1] == '1' then redis.call('LPUSH', keys[1], entry_id)
            else redis.call('RPUSH', keys[1], entry_id) end
        else
            local entry_key = keys[5] .. entry_id
            local raw_entry = redis.call('GET', entry_key)
            local expiry_deadline = redis.call('ZSCORE', keys[6], entry_id)
            if raw_entry and expiry_deadline and tonumber(expiry_deadline) > now_us then
                redis.call('DEL', entry_key)
                redis.call('ZREM', keys[2], entry_id)
                redis.call('ZREM', keys[4], entry_id)
                redis.call('ZREM', keys[6], entry_id)
                redis.call('LREM', keys[1], 0, entry_id)
                return {'dequeued', raw_entry}
            end
            redis.call('DEL', entry_key)
            redis.call('ZREM', keys[2], entry_id)
            redis.call('ZREM', keys[4], entry_id)
            redis.call('ZREM', keys[6], entry_id)
            redis.call('LREM', keys[1], 0, entry_id)
        end
    end
    return {'empty', ''}
end)

register_function('django_queue_renew', 'Keys: claim record, claim-deadline ZSET. Args: worker ID, lease duration in microseconds, entry ID. Returns: 1 when renewed, otherwise 0.', function(keys, args)
    local raw = redis.call('GET', keys[1])
    if not raw then return 0 end
    local ok, claim = pcall(cjson.decode, raw)
    if not ok or type(claim) ~= 'table' or claim.worker_id ~= args[1] then
        return 0
    end
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    if type(claim.lease_deadline) ~= 'number' or claim.lease_deadline <= now_us then
        return 0
    end
    local deadline = now_us + tonumber(args[2])
    claim.lease_deadline = deadline
    redis.call('SET', keys[1], cjson.encode(claim))
    redis.call('ZADD', keys[2], deadline, args[3])
    return 1
end)

register_function('django_queue_release', 'Keys: claim record, claim-deadline ZSET, delayed ZSET, unclaimed-deadline ZSET. Args: worker ID, entry ID, delay in microseconds. Returns: 1 when released, otherwise 0.', function(keys, args)
    local raw = redis.call('GET', keys[1])
    if not raw then return 0 end
    local ok, claim = pcall(cjson.decode, raw)
    if not ok or type(claim) ~= 'table' or claim.worker_id ~= args[1] then
        return 0
    end
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    redis.call('DEL', keys[1])
    redis.call('ZREM', keys[2], args[2])
    redis.call('ZADD', keys[3], now_us + tonumber(args[3]), args[2])
    if type(claim.unclaimed_remaining_us) == 'number' then
        redis.call('ZADD', keys[4], now_us + claim.unclaimed_remaining_us, args[2])
    end
    return 1
end)

register_function('django_queue_release_priority', 'Keys: claim record, claim-deadline ZSET, delayed ZSET, unclaimed-deadline ZSET, priority ZSET, entry record, priority sequence. Args: worker ID, entry ID, priority score space. Returns: 1 when released, otherwise 0.', function(keys, args)
    local raw = redis.call('GET', keys[1])
    if not raw then return 0 end
    local ok, claim = pcall(cjson.decode, raw)
    if not ok or type(claim) ~= 'table' or claim.worker_id ~= args[1] then
        return 0
    end
    local raw_entry = redis.call('GET', keys[6])
    local entry_ok, entry = pcall(cjson.decode, raw_entry)
    local priority = (entry_ok and type(entry) == 'table') and tonumber(entry.priority) or 0
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    redis.call('DEL', keys[1])
    redis.call('ZREM', keys[2], args[2])
    push_priority(keys[5], keys[7], priority * tonumber(args[3]), args[2])
    if type(claim.unclaimed_remaining_us) == 'number' then
        redis.call('ZADD', keys[4], now_us + claim.unclaimed_remaining_us, args[2])
    end
    return 1
end)

register_function('django_queue_remove', 'Keys: claim record, claim-deadline ZSET, delayed ZSET, pending list, entry record, unclaimed-deadline ZSET. Args: worker ID, entry ID. Returns: 1 when removed, otherwise 0.', function(keys, args)
    local raw = redis.call('GET', keys[1])
    if not raw then return 0 end
    local ok, claim = pcall(cjson.decode, raw)
    if not ok or type(claim) ~= 'table' or claim.worker_id ~= args[1] then
        return 0
    end
    redis.call('DEL', keys[1])
    redis.call('ZREM', keys[2], args[2])
    redis.call('ZREM', keys[3], args[2])
    redis.call('LREM', keys[4], 0, args[2])
    redis.call('ZREM', keys[6], args[2])
    return redis.call('DEL', keys[5])
end)

register_function('django_queue_ack', 'Keys: claim record, claim-deadline ZSET. Args: worker ID, entry ID. Returns: 1 when acknowledged, otherwise 0.', function(keys, args)
    local raw_claim = redis.call('GET', keys[1])
    if not raw_claim then return 0 end
    local ok, claim = pcall(cjson.decode, raw_claim)
    if not ok or type(claim) ~= 'table' or claim.worker_id ~= args[1] then
        return 0
    end
    redis.call('ZREM', keys[2], args[2])
    return redis.call('DEL', keys[1])
end)

register_function('django_queue_mark_running', 'Keys: claim record, entry record. Args: worker ID, updated entry JSON. Returns: 1 when transitioned, otherwise 0.', function(keys, args)
    local raw_claim = redis.call('GET', keys[1])
    if not raw_claim then return 0 end
    local ok, claim = pcall(cjson.decode, raw_claim)
    if not ok or type(claim) ~= 'table' or claim.worker_id ~= args[1] then
        return 0
    end
    local raw_entry = redis.call('GET', keys[2])
    local entry_ok, entry = pcall(cjson.decode, raw_entry)
    if not entry_ok or type(entry) ~= 'table' or entry.status ~= 'queued' then
        return 0
    end
    redis.call('SET', keys[2], args[2])
    return 1
end)

register_function('django_queue_settle', 'Keys: claim record, claim-deadline ZSET, entry record. Args: worker ID, entry ID, settled entry JSON. Returns: 1 when settled, otherwise 0.', function(keys, args)
    local raw_claim = redis.call('GET', keys[1])
    if not raw_claim then return 0 end
    local ok, claim = pcall(cjson.decode, raw_claim)
    if not ok or type(claim) ~= 'table' or claim.worker_id ~= args[1] then
        return 0
    end
    local raw_entry = redis.call('GET', keys[3])
    local entry_ok, entry = pcall(cjson.decode, raw_entry)
    if not entry_ok or type(entry) ~= 'table' or entry.status ~= 'running' then
        return 0
    end
    redis.call('SET', keys[3], args[3])
    redis.call('ZREM', keys[2], args[2])
    return redis.call('DEL', keys[1])
end)

register_function('django_queue_expire', 'Keys: claim record, entry record, pending list, delayed ZSET, claim-deadline ZSET, unclaimed-deadline ZSET. Args: entry ID. Returns: 1 when expired, otherwise 0.', function(keys, args)
    if redis.call('GET', keys[1]) then return 0 end
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    local deadline = redis.call('ZSCORE', keys[6], args[1])
    if not deadline or tonumber(deadline) > now_us then return 0 end
    if redis.call('DEL', keys[2]) == 0 then return 0 end
    redis.call('LREM', keys[3], 0, args[1])
    redis.call('ZREM', keys[4], args[1])
    redis.call('ZREM', keys[5], args[1])
    redis.call('ZREM', keys[6], args[1])
    return 1
end)

redis.register_function{
    function_name = 'django_queue_prune',
    description = 'Keys: entry record, pending list. Args: entry ID. Returns: entry JSON, 0 when missing, or -1 when non-terminal.',
    callback = function(keys, args)
        local raw_entry = redis.call('GET', keys[1])
        if not raw_entry then return 0 end
        local ok, entry = pcall(cjson.decode, raw_entry)
        if not ok or type(entry) ~= 'table' then return 0 end
        if entry.status ~= 'succeeded' and entry.status ~= 'failed'
            and entry.status ~= 'cancelled' and entry.status ~= 'timeout' then
            return -1
        end
        redis.call('LREM', keys[2], 0, args[1])
        if redis.call('DEL', keys[1]) == 0 then return 0 end
        return raw_entry
    end,
}

redis.register_function{
    function_name = 'django_queue_delete',
    description = 'Keys: entry record, pending list, delayed ZSET, claim ZSETs, claim key, priority ZSET and sequence, scheduled ZSET. Args: entry ID. Returns: no value.',
    callback = function(keys, args)
        redis.call('DEL', keys[1])
        redis.call('LREM', keys[2], 0, args[1])
        redis.call('ZREM', keys[3], args[1])
        redis.call('ZREM', keys[4], args[1])
        redis.call('ZREM', keys[5], args[1])
        redis.call('DEL', keys[6])
        redis.call('ZREM', keys[7], args[1])
        redis.call('ZREM', keys[9], args[1])
        if redis.call('ZCARD', keys[7]) == 0 then
            redis.call('SET', keys[8], 0, 'XX')
        end
    end,
}

redis.register_function{
    function_name = 'django_queue_recover',
    description = 'Keys: claim-deadline ZSET, claim-key prefix, pending list, entry-key prefix, unclaimed-deadline ZSET. Args: stack flag, recovery batch size. Returns: recovered and discarded counts.',
    callback = function(keys, args)
        local now = redis.call('TIME')
        local deadline = tonumber(now[1]) * 1000000 + tonumber(now[2])
        local ids = redis.call('ZRANGEBYSCORE', keys[1], '-inf', deadline, 'LIMIT', 0, args[2])
        local recovered = 0
        local discarded = 0
        for index = 1, #ids do
            local entry_id = ids[index]
            local claim_key = keys[2] .. entry_id
            local raw_claim = redis.call('GET', claim_key)
            local ok, claim = pcall(cjson.decode, raw_claim)
            local lease_deadline = ok and type(claim) == 'table' and tonumber(claim.lease_deadline)
            if not lease_deadline or lease_deadline <= deadline then
                local entry_key = keys[4] .. entry_id
                local raw_entry = redis.call('GET', entry_key)
                local entry_ok, entry = pcall(cjson.decode, raw_entry)
                if entry_ok and type(entry) == 'table' and (entry.status == 'queued' or entry.status == 'running') then
                    entry.status = 'queued'
                    entry.dispatched_at = cjson.null
                    entry.finished_at = cjson.null
                    entry.result = cjson.null
                    entry.error = cjson.null
                    redis.call('SET', entry_key, cjson.encode(entry))
                    if type(claim.unclaimed_remaining_us) == 'number' then
                        redis.call('ZADD', keys[5], deadline + claim.unclaimed_remaining_us, entry_id)
                    end
                    if args[1] == '1' then redis.call('LPUSH', keys[3], entry_id)
                    else redis.call('RPUSH', keys[3], entry_id) end
                    recovered = recovered + 1
                else
                    discarded = discarded + 1
                end
                redis.call('DEL', claim_key)
                redis.call('ZREM', keys[1], entry_id)
            else
                redis.call('ZADD', keys[1], lease_deadline, entry_id)
            end
        end
        return {recovered, discarded}
    end,
}

redis.register_function{
    function_name = 'django_queue_recover_priority',
    description = 'Keys: claim-deadline ZSET, claim-key prefix, pending list, entry-key prefix, unclaimed-deadline ZSET, priority ZSET, priority sequence. Args: stack flag, recovery batch size, priority score space. Returns: recovered and discarded counts.',
    callback = function(keys, args)
        local now = redis.call('TIME')
        local deadline = tonumber(now[1]) * 1000000 + tonumber(now[2])
        local ids = redis.call('ZRANGEBYSCORE', keys[1], '-inf', deadline, 'LIMIT', 0, args[2])
        local recovered = 0
        local discarded = 0
        for index = 1, #ids do
            local entry_id = ids[index]
            local claim_key = keys[2] .. entry_id
            local raw_claim = redis.call('GET', claim_key)
            local ok, claim = pcall(cjson.decode, raw_claim)
            local lease_deadline = ok and type(claim) == 'table' and tonumber(claim.lease_deadline)
            if not lease_deadline or lease_deadline <= deadline then
                local entry_key = keys[4] .. entry_id
                local raw_entry = redis.call('GET', entry_key)
                local entry_ok, entry = pcall(cjson.decode, raw_entry)
                if entry_ok and type(entry) == 'table' and (entry.status == 'queued' or entry.status == 'running') then
                    entry.status = 'queued'
                    entry.dispatched_at = cjson.null
                    entry.finished_at = cjson.null
                    entry.result = cjson.null
                    entry.error = cjson.null
                    redis.call('SET', entry_key, cjson.encode(entry))
                    if type(claim.unclaimed_remaining_us) == 'number' then
                        redis.call('ZADD', keys[5], deadline + claim.unclaimed_remaining_us, entry_id)
                    end
                    local priority = tonumber(entry.priority) or 0
                    push_priority(keys[6], keys[7], priority * tonumber(args[3]), entry_id)
                    recovered = recovered + 1
                else
                    discarded = discarded + 1
                end
                redis.call('DEL', claim_key)
                redis.call('ZREM', keys[1], entry_id)
            else
                redis.call('ZADD', keys[1], lease_deadline, entry_id)
            end
        end
        return {recovered, discarded}
    end,
}

redis.register_function{
    function_name = 'django_queue_claim',
    description = 'Keys: pending list, delayed ZSET, claim-key prefix, claim-deadline ZSET, entry-key prefix, unclaimed-deadline ZSET, scheduled ZSET. Args: worker ID, lease duration in microseconds, stack flag, expire-unclaimed flag. Returns: claimed, conflict, expired, or empty outcome and entry ID.',
    callback = function(keys, args)
        local now = redis.call('TIME')
        local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
        promote_one_scheduled(keys[7], keys[5], keys[1], args[3] == '1', now_us)
        local delayed = redis.call('ZRANGEBYSCORE', keys[2], '-inf', now_us)
        for index = 1, #delayed do
            if args[3] == '1' then redis.call('LPUSH', keys[1], delayed[index])
            else redis.call('RPUSH', keys[1], delayed[index]) end
            redis.call('ZREM', keys[2], delayed[index])
        end
        local entry_id
        if args[3] == '1' then entry_id = redis.call('RPOP', keys[1])
        else entry_id = redis.call('LPOP', keys[1]) end
        if not entry_id then return {'empty', ''} end
        local remaining
        if args[4] == '1' then
            local expiry_deadline = redis.call('ZSCORE', keys[6], entry_id)
            if not expiry_deadline or tonumber(expiry_deadline) <= now_us then
                redis.call('DEL', keys[5] .. entry_id)
                redis.call('ZREM', keys[2], entry_id)
                redis.call('ZREM', keys[4], entry_id)
                redis.call('ZREM', keys[6], entry_id)
                return {'expired', entry_id}
            end
            remaining = tonumber(expiry_deadline) - now_us
            redis.call('ZREM', keys[6], entry_id)
        end
        local claim_key = keys[3] .. entry_id
        local deadline = now_us + tonumber(args[2])
        local claim = cjson.encode({
            worker_id = args[1],
            claimed_at = {seconds = tonumber(now[1]), microseconds = tonumber(now[2])},
            lease_deadline = deadline,
        })
        if remaining then
            claim = cjson.encode({
                worker_id = args[1],
                claimed_at = {seconds = tonumber(now[1]), microseconds = tonumber(now[2])},
                lease_deadline = deadline,
                unclaimed_remaining_us = remaining,
            })
        end
        if redis.call('SET', claim_key, claim, 'NX') then
            redis.call('ZADD', keys[4], deadline, entry_id)
            return {'claimed', entry_id}
        end
        if args[3] == '1' then redis.call('RPUSH', keys[1], entry_id)
        else redis.call('LPUSH', keys[1], entry_id) end
        return {'conflict', entry_id}
    end,
}

redis.register_function{
    function_name = 'django_queue_claim_priority',
    description = 'Keys: pending list, delayed ZSET, claim-key prefix, claim-deadline ZSET, entry-key prefix, unclaimed-deadline ZSET, priority ZSET and sequence, scheduled ZSET. Args: worker ID, lease duration in microseconds, stack flag, expire-unclaimed flag, priority score space. Returns: claimed, conflict, expired, or empty outcome and entry ID.',
    callback = function(keys, args)
        local now = redis.call('TIME')
        local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
        promote_one_scheduled_priority(keys[9], keys[5], keys[7], keys[8], tonumber(args[5]), now_us)
        local delayed = redis.call('ZRANGEBYSCORE', keys[2], '-inf', now_us)
        for index = 1, #delayed do
            if args[3] == '1' then redis.call('LPUSH', keys[1], delayed[index])
            else redis.call('RPUSH', keys[1], delayed[index]) end
            redis.call('ZREM', keys[2], delayed[index])
        end
        local entry_id
        local priority_score
        if args[3] == '1' then entry_id = redis.call('RPOP', keys[1])
        else entry_id = redis.call('LPOP', keys[1]) end
        if not entry_id then
            local top = redis.call('ZREVRANGE', keys[7], 0, 0, 'WITHSCORES')
            if #top > 0 then
                entry_id = top[1]
                priority_score = top[2]
                redis.call('ZREM', keys[7], entry_id)
            end
        end
        if not entry_id then return {'empty', ''} end
        local remaining
        if args[4] == '1' then
            local expiry_deadline = redis.call('ZSCORE', keys[6], entry_id)
            if not expiry_deadline or tonumber(expiry_deadline) <= now_us then
                redis.call('DEL', keys[5] .. entry_id)
                redis.call('ZREM', keys[2], entry_id)
                redis.call('ZREM', keys[4], entry_id)
                redis.call('ZREM', keys[6], entry_id)
                if priority_score and redis.call('ZCARD', keys[7]) == 0 then
                    redis.call('SET', keys[8], 0, 'XX')
                end
                return {'expired', entry_id}
            end
            remaining = tonumber(expiry_deadline) - now_us
            redis.call('ZREM', keys[6], entry_id)
        end
        local claim_key = keys[3] .. entry_id
        local deadline = now_us + tonumber(args[2])
        local claim = cjson.encode({
            worker_id = args[1],
            claimed_at = {seconds = tonumber(now[1]), microseconds = tonumber(now[2])},
            lease_deadline = deadline,
        })
        if remaining then
            claim = cjson.encode({
                worker_id = args[1],
                claimed_at = {seconds = tonumber(now[1]), microseconds = tonumber(now[2])},
                lease_deadline = deadline,
                unclaimed_remaining_us = remaining,
            })
        end
        if redis.call('SET', claim_key, claim, 'NX') then
            redis.call('ZADD', keys[4], deadline, entry_id)
            if priority_score and redis.call('ZCARD', keys[7]) == 0 then
                redis.call('SET', keys[8], 0, 'XX')
            end
            return {'claimed', entry_id}
        end
        if priority_score then
            redis.call('ZADD', keys[7], priority_score, entry_id)
        elseif args[3] == '1' then
            redis.call('RPUSH', keys[1], entry_id)
        else
            redis.call('LPUSH', keys[1], entry_id)
        end
        return {'conflict', entry_id}
    end,
}
