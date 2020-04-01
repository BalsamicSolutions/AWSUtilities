using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    /// <summary>
    /// default redis retry policy
    /// </summary>
    public class DefaultRedisRetryPolicy : RedisRetryPolicy
    {
        bool IsSocketOrRedisTimeOutException(Exception callError)
        {
            RedisConnectionException connectionException = callError as RedisConnectionException;
            if (null != connectionException)
            {
                //should retry if the db is still loading
                if (connectionException.FailureType == ConnectionFailureType.Loading)
                {
                    return true;
                }
            }
            RedisTimeoutException redisException = callError as RedisTimeoutException;
            if (null != redisException)
            {
                return true;
            }
            SocketException socketException = callError as SocketException;
            if (null != socketException)
            {
                //retry on a few of these
               if(socketException.SocketErrorCode == SocketError.TimedOut
                    || socketException.SocketErrorCode == SocketError.InProgress
                    || socketException.SocketErrorCode == SocketError.IOPending)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// return true for socket errors, and RedisTimeoutException
        /// </summary>
        /// <param name="callError"></param>
        /// <returns></returns>
        public override bool ShouldRetry(Exception callError)
        {
            if (IsSocketOrRedisTimeOutException(callError)) return true;
            System.AggregateException aggregateError = callError as System.AggregateException;
            if (null != aggregateError)
            {
                foreach (Exception innerError in aggregateError.InnerExceptions)
                {
                    if (IsSocketOrRedisTimeOutException(innerError)) return true;
                }
            }
            else
            {
                Exception innerException = callError.InnerException;
                while (null != innerException)
                {
                    if (IsSocketOrRedisTimeOutException(innerException)) return true;
                    innerException = innerException.InnerException;
                }
            }

            return false;
        }
    }
}

