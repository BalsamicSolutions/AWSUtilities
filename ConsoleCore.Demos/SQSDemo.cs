//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Demo.Data;
using Demo.Data.Testing;
using BalsamicSolutions.AWSUtilities.Extensions;
using BalsamicSolutions.AWSUtilities.SQS;

namespace ConsoleCore.Demos
{
    /// <summary>
    /// demonstration of how to use the SqsQueueDispatcher and the AutoScaleLifeCycleMonitor
    /// Externally the system has posted items to the queue. If an ASG is connected to the
    /// queue then an instance will be launched. There is a configuration option on the queue
    /// for how long to wait before completing the job. This delay gives you time to attach a
    /// debugger to the queue if you want to walk through the AutoScaleLifeCycleMonitor process.
    /// Otherwise its just a simple queue handler.
    /// </summary>
    public class SQSDemo : IHostedService, IDisposable
    {
        private bool _Disposed = false;
        private CancellationTokenSource _CancellationTokenSource;
        private Task _WorkTask = null;
        private ILogger _SysLogger = null;
        private IConfiguration _Configuration = null;
        private int _QueueDelayInSeconds = 30;
        private SqsQueueDispatcher<SQSDemoQueueData> _SqsQueue = null;

        public SQSDemo(IConfiguration config, ILogger sysLogger)
        {
            _SysLogger = sysLogger;
            _Configuration = config;
        }

        /// <summary>
        /// Call from the Host builder to startup
        /// </summary>
        /// <param name="stoppingToken">stoppingToken</param>
        /// <returns></returns>
        public Task StartAsync(CancellationToken stoppingToken)
        {
            //read our configuration
            _QueueDelayInSeconds = int.Parse(_Configuration.GetValue<string>("appSettings:QueueDelay"));
            string queueName = _Configuration.GetValue<string>("appSettings:QueueName");
            int maxConcurrency = int.Parse(_Configuration.GetValue<string>("appSettings:QueueMaxConcurrency"));
            //create the queue
            _SqsQueue = new SqsQueueDispatcher<SQSDemoQueueData>(queueName, _SysLogger, maxConcurrency);
            //subscribe it to our handler
            _SqsQueue.Subscribe(QueueCallBack);
            return Task.CompletedTask;
        }

        /// <summary>
        /// this is the callback from the queue
        /// </summary>
        /// <param name="queueData">the data item returned from the queue</param>
        /// <param name="recieptHandle">the receipt handle of the data item</param>
        /// <param name="sqsQueue">the queue that captured this message</param>
        /// <returns>true to indicate we processed it, false to indicate it should remain in the queue</returns>
        private bool QueueCallBack(SQSDemoQueueData queueData, string recieptHandle, SqsQueueDispatcher<SQSDemoQueueData> sqsQueue)
        {
            bool returnValue = false;
            try
            {
                _SysLogger.LogInformation($"Recieved new message {recieptHandle} with {queueData.RandomId}/{queueData.RandomData}/{queueData.MoreRandomData}");
                //Normally you would do work here, for our demo we just sleep a bit
                Task.Delay(TimeSpan.FromSeconds(_QueueDelayInSeconds), _CancellationTokenSource.Token);
                if(!_CancellationTokenSource.IsCancellationRequested)
                {
                    returnValue = true;
                }
            }
            catch (OperationCanceledException)
            {
                _SysLogger.LogInformation($"Was cancelled for message {recieptHandle}");
                returnValue = false;
            }
            catch (Exception badThing)
            {
                _SysLogger.LogError(badThing, $"A critical error occurred during the queue callback for message {recieptHandle}");
                //the SqsQueueDispatcher handles this so throw it again
               throw;
            }
            return returnValue;
        }

        /// <summary>
        /// Call from the Host builder to stop
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        public Task StopAsync(CancellationToken stoppingToken)
        {
            _CancellationTokenSource.Cancel();
            if (null != _SqsQueue)
            {
                _SqsQueue.UnSubscribe();
                _SqsQueue.Dispose();
                _SqsQueue = null;
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// implement the disposable pattern.
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_Disposed)
            {
                if (disposing)
                {
                    if (null != _SqsQueue)
                    {
                        _SqsQueue.UnSubscribe();
                        _SqsQueue.Dispose();
                        _SqsQueue = null;
                    }
                    _CancellationTokenSource.Dispose();
                }
                _Disposed = true;
                // placeholder for cleaning up unmanaged objects
            }
        }

        /// <summary>
        /// implement the disposable pattern.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}