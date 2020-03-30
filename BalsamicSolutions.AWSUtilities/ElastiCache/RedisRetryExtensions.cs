//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using BalsamicSolutions.AWSUtilities.Extensions;
using StackExchange.Redis;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    /// <summary>
    /// retry extensions for Redis cloud
    /// operations, similar in concept to
    /// the Entity Framework execution policy
    /// </summary>
    internal static class RedisRetryExtensions
    {
        /// <summary>
		/// forward a void to our handler
		/// </summary>
		public static void Exec(this IDatabase redisDb, Action redisFunction)
        {
            PerformRemoteActionWithSocketCheck(redisFunction, AWS.CloudWatchAlarmTypes.RedisIsUnavailable);
        }

        /// <summary>
        /// forward a method with a return result to our handler
        /// </summary>
        public static T Exec<T>(this IDatabase redisDb, Func<T> redisFunction)
        {
            var returnValue = default(T);
            PerformRemoteActionWithSocketCheck(() => returnValue = redisFunction(), AWS.CloudWatchAlarmTypes.RedisIsUnavailable);
            return returnValue;
        }

        
        //static bool TryCast<T>(this object obj, out T result)
        //{
        //    if (obj is T)
        //    {
        //        result = (T)obj;
        //        return true;
        //    }

        //    result = default(T);
        //    return false;
        //}

        /// <summary>
        /// Ok time to call whatever it is we are trying to do
        /// </summary>
        private static void PerformRemoteActionWithSocketCheck(Action dbAction)
        {
            try
            {
                dbAction();
            }
            catch (Exception callError)
            {
                System.Diagnostics.Trace.WriteLine(string.Format("error in {0}", callError.Message));
                System.Net.Sockets.SocketException socketError = callError as System.Net.Sockets.SocketException;
                if (null != socketError)
                {

                }
                if (callError is System.AggregateException)
                {
                    System.AggregateException aggregateError = callError as System.AggregateException;
                    if (null != aggregateError)
                    {
                        foreach (Exception innerError in aggregateError.InnerExceptions)
                        {

                        }
                    }
                }
                else
                {
                    Exception innerException = callError.InnerException;
                    while (null != innerException)
                    {
                        //for databases, we only log client problems to
                        //cloud watch , which always have a socket error in them
                        if (innerException is System.Net.Sockets.SocketException)
                        {

                        }
                        innerException = innerException.InnerException;
                    }
                }
                throw;
            }
        }
    }
}