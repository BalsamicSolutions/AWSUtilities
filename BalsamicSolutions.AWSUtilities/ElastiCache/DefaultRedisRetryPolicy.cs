//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
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
        private bool IsSocketOrRedisTimeOutException(Exception callError)
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
            //always retry on a timeout, this is sometimes a problem
            //with initial connections but allows for cluster reconfiguration
            //when a multiplexer is repointed to a  different server
            RedisTimeoutException redisException = callError as RedisTimeoutException;
            if (null != redisException)
            {
                return true;
            }

            SocketException socketException = callError as SocketException;
            if (null != socketException)
            {
                //retry on these
                if (socketException.SocketErrorCode == SocketError.TimedOut
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
                //this is kind of aggressive but you can replace it if you want to
                foreach (Exception innerError in aggregateError.InnerExceptions)
                {
                    if (IsSocketOrRedisTimeOutException(innerError)) return true;
                }
            }
            else
            {
                 //this is kind of aggressive but you can replace it if you want to
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