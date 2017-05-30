--
--  Copyright (C) 2016-2017 Tarantool AUTHORS: please see AUTHORS file.
--
--  Redistribution and use in source and binary forms, with or
--  without modification, are permitted provided that the following
--  conditions are met:
--
--  1. Redistributions of source code must retain the above
--   copyright notice, this list of conditions and the
--   following disclaimer.
--
--  2. Redistributions in binary form must reproduce the above
--   copyright notice, this list of conditions and the following
--   disclaimer in the documentation and/or other materials
--   provided with the distribution.
--
--  THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
--  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
--  TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
--  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
--  <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
--  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
--  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
--  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
--  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
--  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
--  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
--  THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
--  SUCH DAMAGE.
--

local fiber = require('fiber')
local driver = require('http.client.driver')

local curl_mt

--
--  <http> - create a new curl instance.
--
--  Parameters:
--
--  max_connectionss -  Maximum number of entries in the connection cache */
--
--  Returns:
--  curl object or raise error()
--

local http_new = function(opts)

    opts = opts or {}

    opts.max_connections = opts.max_connections or 5

    local curl = driver.new(opts.max_connections)
    return setmetatable({ curl = curl, }, curl_mt )
end

local check_args_fmt = 'Use client:%s(...) instead of client.%s(...):'

local function check_args(self, method)
    if type(self) ~= 'table' then
        error(check_args_fmt:format(method, method), 2)
    end
end

local function parse_list(list)
    local result = {}
    for _,str in pairs(list) do
        if str ~= '' and not string.match(str, "HTTP/%d%.%d %d%d%d") then
            local h = str:split(': ')
            local key = table.remove(h, 1)
            local val = table.concat(h)
            result[string.lower(key)] = val
        end
    end
    return result
end

local function parse_headers(resp)
    local list = resp.headers:split('\r\n')
    local h1 = table.remove(list, 1):split(' ')
    local proto = h1[1]:split('/')[2]:split('.')
    resp.proto = { tonumber(proto[1]), tonumber(proto[2]) }
    resp.headers = parse_list(list)
    return resp
end

--
--  <request> This function does HTTP request
--
--  Parameters:
--
--  method  - HTTP method, like GET, POST, PUT and so on
--  url     - HTTP url, like https://tarantool.org/doc
--  body    - this parameter is optional, you may use it for passing
--  options - this is a table of options.
--       data to a server. Like 'My text string!'
--
--      ca_path - a path to ssl certificate dir;
--
--      ca_file - a path to ssl certificate file;
--
--      headers - a table of HTTP headers;
--
--      keepalive_idle & keepalive_interval -
--          non-universal keepalive knobs (Linux, AIX, HP-UX, more);
--
--      low_speed_time & low_speed_limit -
--          If the download receives less than
--          "low speed limit" bytes/second
--          during "low speed time" seconds,
--          the operations is aborted.
--          You could i.e if you have
--          a pretty high speed connection, abort if
--          it is less than 2000 bytes/sec
--          during 20 seconds;
--
--      timeout - Time-out the read operation and
--          waiting for the curl api request
--          after this amount of seconds;
--
--      verbose - set on/off verbose mode
--
--  Returns:
--      {
--          status=NUMBER,
--          reason=ERRMSG
--          body=STRING,
--          headers=STRING,
--          errmsg=STRING
--      }
--
--  Raises error() on invalid arguments and OOM
--

curl_mt = {
    __index = {
        --
        --  <request> see above <request>
        --
        request = function(self, method, url, body, opts)
            if not method or not url then
                error('request(method, url [, options]])')
            end
            local resp = self.curl:request(method, url, body, opts or {})
            if resp and resp.headers then
                resp = parse_headers(resp)
            end
            return resp
        end,

        --
        -- <get> - see <request>
        --
        get = function(self, url, options)
            check_args(self, 'get')
            return self:request('GET', url, nil, options)
        end,

        --
        -- <post> - see <request>
        --
        post = function(self, url, body, options)
            check_args(self, 'post')
            return self:request('POST', url, body, options)
        end,

        --
        -- <put> - see <request>
        --
        put = function(self, url, body, options)
            check_args(self, 'put')
            return self:request('PUT', url, body, options)
        end,

        --
        -- <patch> - see <request>
        --
        patch = function(self, url, body, options)
            check_args(self, 'patch')
            return self:request('PATCH', url, body, options)
        end,

        --
        -- <options> see <request>
        --
        options = function(self, url, options)
            check_args(self, 'options')
            return self:request('OPTIONS', url, nil, options)
        end,

        --
        -- <head> see <request>
        --
        head = function(self, url, options)
            check_args(self, 'head')
            return self:request('HEAD', url, nil, options)
        end,
        --
        -- <delete> see <request>
        --
        delete = function(self, url, options)
            check_args(self, 'delete')
            return self:request('DELETE', url, nil, options)
        end,

        --
        -- <trace> see <request>
        --
        trace = function(self, url, options)
            check_args(self, 'trace')
            return self:request('TRACE', url, nil, options)
        end,

        --
        -- <connect> see <request>
        --
        connect = function(self, url, options)
            check_args(self, 'connect')
            return self:request('CONNECT', url, nil, options)
        end,

        --
        -- <stat> - this function returns a table with many values of statistic.
        --
        -- Returns {
        --
        --  active_requests - this is number of currently executing requests
        --
        --  sockets_added -
        --  this is a total number of added sockets into libev loop
        --
        --  sockets_deleted -
        --  this is a total number of deleted sockets from libev
        --                      loop
        --
        --  total_requests - this is a total number of requests
        --
        --  http_200_responses -
        --  this is a total number of requests which have
        --              returned a code HTTP 200
        --
        --  http_other_responses -
        --      this is a total number of requests which have
        --      requests not a HTTP 200
        --
        --  failed_requests - this is a total number of requests which have
        --      failed (included systeme erros, curl errors, HTTP
        --      errors and so on)
        --  }
        --  or error()
        --
        stat = function(self)
            return self.curl:stat()
        end,

    },
}

--
-- Export
--
local http_default = http_new()
local this_module = { new = http_new, }

local function http_default_wrap(fname)
    return function(...) return http_default[fname](http_default, ...) end
end

for _, name in ipairs({ 'get', 'delete', 'trace', 'options', 'head',
                     'connect', 'post', 'put', 'patch', }) do
    this_module[name] = http_default_wrap(name)
end

package.loaded['http.client'] = this_module
