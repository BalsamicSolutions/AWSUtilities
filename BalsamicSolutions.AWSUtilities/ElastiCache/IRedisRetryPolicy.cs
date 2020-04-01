using System;
using System.Collections.Generic;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    public interface IRedisRetryPolicy
    {
         bool ShouldRetry(Exception callError);
         TimeSpan? CalculateDelay(int retryCount);
        int MaxRetry {get;}
    }
}
