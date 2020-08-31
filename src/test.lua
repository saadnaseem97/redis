-- db0 should be passed in as params
local id=redis.call("hget", "db0","id")
local olddbsize=redis.call("dbsize")
-- eecute customer commands, make sure keys were updated with prefix
local result=redis.call("set", id..":".."foo","bar")
local newdbsize=redis.call("dbsize")
redis.call("hset", "db0","dbsize",newdbsize-olddbsize)
return result
