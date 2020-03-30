using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    /// <summary>
    /// implementation of the Redis IDatabase
    /// that wraps all calls with a retry handler
    /// allows individual calls to recovery from
    /// ElasticCache Redis cluster changes and 
    /// network connectivity blips
    /// </summary>
    public class RedisCache 
    {
    }
}
