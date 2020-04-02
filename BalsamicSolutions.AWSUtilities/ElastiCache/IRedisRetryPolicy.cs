//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    public interface IRedisRetryPolicy
    {
        /// <summary>
        /// indicates that this exception qualifies for a retry
        /// </summary>
        /// <param name="callError"></param>
        /// <returns></returns>
        bool ShouldRetry(Exception callError);

        /// <summary>
        /// Calculate  a delay based tje teh current retry
        /// count, return null if the caller should abandon
        /// the calls due to counts
        /// </summary>
        /// <param name="retryCount"></param>
        /// <returns></returns>
        TimeSpan? CalculateDelay(int retryCount);

        /// <summary>
        /// the maximum number of retries for any given
        /// operation
        /// </summary>
        int MaxRetry { get; }
    }
}