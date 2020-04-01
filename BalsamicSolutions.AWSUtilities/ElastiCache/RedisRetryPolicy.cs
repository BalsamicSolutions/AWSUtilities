//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    /// <summary>
    /// retry policy for calls to Redis endpoints
    /// modeled in part on the Execution strategy
    /// implementation of Entity Framework core
    /// </summary>
    public abstract class RedisRetryPolicy:IRedisRetryPolicy
    {
        private static Random _Random = new Random();
        private static double _RandomFactor = 1.1;

        /// <summary>
        ///     The default maximum random factor, must not be lesser than 1.
        /// </summary>
        public static double RandomFactor
        {
            get
            {
                return _RandomFactor;
            }
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException("value", "Must be greater than 1");
                _RandomFactor = value;
            }
        }

        /// <summary>
        ///     The default base for the exponential function used to compute the delay between retries, must be positive.
        /// </summary>
        public static double RandomExponentialBase { get; set; } = 2;

        /// <summary>
        ///     The default coefficient for the exponential function used to compute the delay between retries, must be nonnegative.
        /// </summary>
        public static TimeSpan RandomCoefficient { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// maximum delay between retries
        /// </summary>
        public static TimeSpan MaxRandomRetryDelay { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// The default retry limit
        /// </summary>
        public const int DEFAULT_MAX_RETRY = 3;

        /// <summary>
        /// The default retry delay in milliseconds
        /// </summary>
        public const int DEFAULT_RETRY_DELAY = 500;

        /// <summary>
        /// Gets or sets the maximum amount of execution attempts before quitting.
        /// </summary>
        /// <value>
        /// The retry limit.
        /// </value>
        public int MaxRetry { get; protected set; }

        /// <summary>
        /// Gets or sets the amount of time to wait in milliseconds before executing another attempt a
        /// value of -1 indicates that the delay should be randomized
        /// </summary>
        /// <value>
        /// The retry delay.
        /// </value>
        public int RetryDelay { get; protected set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryPolicy"/> class.
        /// </summary>
        /// <param name="maxRetry">The maximum amount of execution attempts before quitting.</param>
        /// <param name="retryDelay">The amount of time to wait in milliseconds before executing another attempt. -1 indicates a random number should be used</param>
        public RedisRetryPolicy(int maxRetry = DEFAULT_MAX_RETRY, int retryDelay = DEFAULT_RETRY_DELAY)
        {
            MaxRetry = maxRetry;
            RetryDelay = retryDelay;
        }

        /// <summary>
        /// only retry on socket errors
        /// </summary>
        /// <param name="callError"></param>
        /// <returns></returns>
        public abstract bool ShouldRetry(Exception callError);
    

        /// <summary>
        /// calculate a retry delay
        /// logic borrowed from Entity framework
        /// execution strategy
        /// </summary>
        /// <param name="retryCount"></param>
        /// <returns></returns>
        public TimeSpan? CalculateDelay(int retryCount)
        {
            TimeSpan? returnValue = null;
            if (retryCount > 0)
            {
                if (RetryDelay == -1)
                {
                    double delta = (Math.Pow(RandomExponentialBase, retryCount) - 1.0) * (1.0 + _Random.NextDouble() * (_RandomFactor - 1.0));
                    double delay = Math.Min(RandomCoefficient.TotalMilliseconds * delta, MaxRandomRetryDelay.TotalMilliseconds);
                    return TimeSpan.FromMilliseconds(delay);
                }
                else
                {
                    return TimeSpan.FromMilliseconds(RetryDelay);
                }
            }
            return returnValue;
        }

    
 
    }
}