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
    public static class RedisRetryPolicyExtensions
    {
        private static RedisRetryPolicy _DefaultRetryPolicy = new DefaultRedisRetryPolicy();

        /// <summary>
        /// execute the action using the retry policy
        /// </summary>
        public static void ExecuteWithRetry(this IRedisRetryPolicy retryPolicy, Action redisAction)
        {
            ExecuteWithRetryInternal(redisAction, retryPolicy);
        }

        /// <summary>
        /// execute the action using the retry policy with async operations
        /// </summary>
        public static async Task ExecuteWithRetryAsync(this IRedisRetryPolicy retryPolicy, Func<Task> redisAction)
        {
            //TODO add CancellationToken overload
            await ExecuteWithRetryInternalAsync(redisAction, retryPolicy);
        }

        /// <summary>
        /// execute the function and return a result using the retry policy
        /// </summary>
        public static T ExecuteWithRetry<T>(this IRedisRetryPolicy retryPolicy, Func<T> redisFunction)
        {
            var returnValue = default(T);
            ExecuteWithRetryInternal(() => returnValue = redisFunction(), retryPolicy);
            return returnValue;
        }

        /// <summary>
        /// execute the function and return an async task result using the retry policy
        /// </summary>
        public async static Task<T> ExecuteWithRetryAsync<T>(this IRedisRetryPolicy retryPolicy, Func<Task<T>> asyncFunction)
        {
            //TODO add CancellationToken overload
            var returnValue = default(T);
            await ExecuteWithRetryInternalAsync(async () => returnValue = await asyncFunction(), retryPolicy);
            return returnValue;
        }

        /// <summary>
        /// Ok time to call whatever it is we are trying to do
        /// </summary>
        private static async Task ExecuteWithRetryInternalAsync(Func<Task> asyncAction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy ? _DefaultRetryPolicy : retryPolicy;
            int retryCount = callPolicy.MaxRetry;
            TimeSpan? delay = null;
            while (true)
            {
                try
                {
                    await asyncAction();
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

        /// <summary>
        /// Ok time to call whatever it is we are trying to do
        /// </summary>
        private static async Task<T> ExecuteWithRetryInternalAsync<T>(Func<Task<T>> asyncFunction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy ? _DefaultRetryPolicy : retryPolicy;

            int retryCount = callPolicy.MaxRetry;
            TimeSpan? delay = null;
            while (true)
            {
                try
                {
                   return await asyncFunction();
                   
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
        private static T ExecuteWithRetryInternal<T>(Func<T> redisFunction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy ? _DefaultRetryPolicy : retryPolicy;

            int retryCount = callPolicy.MaxRetry;
            TimeSpan? delay = null;
            while (true)
            {
                try
                {
                    T returnValue = redisFunction();
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
        private static void ExecuteWithRetryInternal(Action redisAction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy ? _DefaultRetryPolicy : retryPolicy;
            int retryCount = callPolicy.MaxRetry;
            TimeSpan? delay = null;
            while (true)
            {
                try
                {
                    redisAction();
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