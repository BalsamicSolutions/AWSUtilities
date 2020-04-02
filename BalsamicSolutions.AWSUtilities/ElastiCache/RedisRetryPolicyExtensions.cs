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
    //TODO should we move these into the retry policy class or keep them as extensions ?

    /// <summary>
    /// simple retry extensions
    /// operations 
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
        public static Task ExecuteWithRetryAsync(this IRedisRetryPolicy retryPolicy, Func<Task> redisAction)
        {
            return ExecuteWithRetryInternalAsync(redisAction, retryPolicy);
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
        public static Task<T> ExecuteWithRetryAsync<T>(this IRedisRetryPolicy retryPolicy, Func<Task<T>> asyncFunction)
        {
            return ExecuteWithRetryInternalAsync(() => asyncFunction(), retryPolicy);
        }

        /// <summary>
        /// In order for us to return the task, we need to create a new task
        /// that wraps the async action task so that it can be retried withen
        /// an external await
        /// </summary>
        private static Task ExecuteWithRetryInternalAsync(Func<Task> asyncAction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy ? _DefaultRetryPolicy : retryPolicy;
            int retryCount = callPolicy.MaxRetry;
            Task returnValue = Task.Run(async () =>
            {
                TimeSpan? delay = null;
                bool runTask = true;
                while (runTask)
                {
                    try
                    {
                        await asyncAction();
                        runTask = false;
                    }
                    catch (Exception callError)
                    {
                        if (retryCount > 0 && callPolicy.ShouldRetry(callError))
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
                        await Task.Delay(delay.Value);
                    }
                }
            });
            return returnValue;
        }

        /// <summary>
        /// In order for us to return the task, we need to create a new task
        /// that wraps the async action task so that it can be retried withen
        /// an external await
        /// </summary>
        private static Task<T> ExecuteWithRetryInternalAsync<T>(Func<Task<T>> asyncFunction, IRedisRetryPolicy retryPolicy)
        {
            IRedisRetryPolicy callPolicy = null == retryPolicy ? _DefaultRetryPolicy : retryPolicy;
            int retryCount = callPolicy.MaxRetry;
            Task<T> returnValue = Task.Run<T>(async () =>
            {
                TimeSpan? delay = null;
                while (true)
                {
                    try
                    {
                        T taskResult = await asyncFunction();
                        return taskResult;
                    }
                    catch (Exception callError)
                    {
                        if (retryCount > 0 && callPolicy.ShouldRetry(callError))
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
                        await Task.Delay(delay.Value);
                    }
                }
            });
            return returnValue;
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
                    if (retryCount > 0 && callPolicy.ShouldRetry(callError))
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
                    if (retryCount > 0 && callPolicy.ShouldRetry(callError))
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