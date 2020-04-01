//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BalsamicSolutions.AWSUtilities.Extensions;
using StackExchange.Redis;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    /// <summary>
    /// simple retry extensions for Redis 
    /// operations, for more advanced
    /// </summary>
    public static class RedisRetryExtensions
    {
        private static RedisRetryPolicy _DefaultRetryPolicy = new DefaultRedisRetryPolicy();
 

        /// <summary>
        /// forward a void to our handler
        /// </summary>
        public static void ExecuteWithRetry(this IRedisRetryPolicy retryPolicy, Action redisFunction)
        {
            ExecuteWithRetryInternal(redisFunction, retryPolicy);
        }


        /// <summary>
        /// forward a method with a return result to our handler
        /// </summary>
        public static T ExecuteWithRetry<T>(this IRedisRetryPolicy retryPolicy, Func<T> redisFunction)
        {
            var returnValue = default(T);
            ExecuteWithRetryInternal(() => returnValue = redisFunction(), retryPolicy);
            return returnValue;
        }

         

        /// <summary>
        /// Ok time to call whatever it is we are trying to do
        /// </summary>
        private static T ExecuteWithRetryInternal<T>(Func<T> dbAction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy?_DefaultRetryPolicy:retryPolicy;

            int retryCount = callPolicy.MaxRetry;
            TimeSpan? delay = null;
            while (true)
            {
                try
                {
                    T returnValue = dbAction();
                    return returnValue;
                }
                catch (Exception callError)
                {
                    if (retryCount <= 0 && callPolicy.ShouldRetry(callError))
                    {
                        retryCount--;
                        delay = callPolicy.CalculateDelay(retryCount);
                    }
                    else
                    {
                        throw;
                    }
                }
                if (delay.HasValue)
                {
                    //borrowed from Entity Framework execution strategy
                    using (var waitEvent = new ManualResetEventSlim(false))
                    {
                        waitEvent.WaitHandle.WaitOne(delay.Value);
                    }
                }
            }
        }

        /// <summary>
        /// Ok time to call whatever it is we are trying to do
        /// </summary>
        private static void ExecuteWithRetryInternal(Action dbAction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy?_DefaultRetryPolicy:retryPolicy;
            int retryCount = callPolicy.MaxRetry;
            TimeSpan? delay = null;
            while (true)
            {
                try
                {
                    dbAction();
                    return;
                }
                catch (Exception callError)
                {
                    if (retryCount <= 0 && callPolicy.ShouldRetry(callError))
                    {
                        retryCount--;
                        delay = callPolicy.CalculateDelay(retryCount);
                    }
                    else
                    {
                        throw;
                    }
                }
                if (delay.HasValue)
                {
                    //borrowed from Entity Framework execution strategy
                    using (var waitEvent = new ManualResetEventSlim(false))
                    {
                        waitEvent.WaitHandle.WaitOne(delay.Value);
                    }
                }
            }
        }
    }
}