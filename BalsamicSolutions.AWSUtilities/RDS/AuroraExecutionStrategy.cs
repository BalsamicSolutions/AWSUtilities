using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using MySql.Data.MySqlClient;
using System;
using System.Net.Sockets;

namespace BalsamicSolutions.AWSUtilities.RDS
{
    public class AuroraExecutionStrategy: ExecutionStrategy
    {
         //the recomended AzureSQL defaults are 5 retries with a max of 30 seconds
        //for our Aurora/MySql impmenetation we are going to be a bit less leniant
        //we are going to use 3 retries with a max of 15 seconds because we are
        //in the same subnet/availability zone as the RDS instance.
        public static int AuroraMaxRetryCount = 3;

        public static TimeSpan AuroraMaxRetryDelay = TimeSpan.FromSeconds(15);

        public AuroraExecutionStrategy(DbContext context)
           : base(context, DefaultMaxRetryCount, AuroraMaxRetryDelay)
        {
        }

        /// <summary>
        /// decide if a retry is warrented
        /// </summary>
        /// <param name="connectionException"></param>
        /// <returns></returns>
        protected override bool ShouldRetryOn(Exception connectionException)
        {
            //Some of the MySqlExceptions wrap inner conenction issues
            //that happen when a server is being relaunched, or redirected
            //by a DNS failover. During those times we allow these exceptions
            //to be retried
            if (connectionException is MySqlException mySqlException)
            {
                switch ((MySqlErrorCode)mySqlException.Number)
                {
                    case MySqlErrorCode.UnableToConnectToHost:
                    case MySqlErrorCode.ConnectionCountError:
                    case MySqlErrorCode.LockWaitTimeout:
                    case MySqlErrorCode.LockDeadlock:
                    case MySqlErrorCode.XARBDeadlock:
                        return true;
                }
                return false;
            }
            else if (connectionException is SocketException socketException)
            {
                switch (socketException.SocketErrorCode)
                {
                    case SocketError.HostNotFound:
                    case SocketError.HostUnreachable:
                    case SocketError.NetworkUnreachable:
                    case SocketError.TimedOut:
                        return true;
                }
                return false;
            }
            return connectionException is TimeoutException;
        }

        /// <summary>
        /// get a wait timespan
        /// </summary>
        /// <param name="lastException"></param>
        /// <returns></returns>
        protected override TimeSpan? GetNextDelay(Exception lastException)
        {
            TimeSpan? baseDelay = base.GetNextDelay(lastException);
            if (null == baseDelay)
            {
                return null;
            }
            else
            {
                return baseDelay;
            }
        }
    }
}
