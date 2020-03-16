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

namespace ConsoleCore.Demos
{
    /// <summary>
    /// model hosted service
    /// </summary>
    public class ScratchPadService : IHostedService, IDisposable
    {
        private bool _Disposed = false;
        private CancellationTokenSource _CancellationTokenSource = new CancellationTokenSource();
        private Task _WorkTask = null;
        private ILogger<ScratchPadService> _SysLogger = null;

        public ScratchPadService(ILogger<ScratchPadService> sysLogger)
        {
            _SysLogger = sysLogger;
        }

        protected Task InitializeTask(CancellationToken stoppingToken)
        {
            return new Task(() =>
                 {
                     try
                     {
                         _SysLogger.LogInformation("Starting ScratchPad");

                         //TODO do something
                         Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                     }
                     catch (Exception err)
                     {
                         _SysLogger.LogCritical(err, " Critical error during ScratchPad");
                     }
                 }, stoppingToken);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _WorkTask = InitializeTask(_CancellationTokenSource.Token);
            _WorkTask.Start();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _SysLogger.LogInformation("Stopping ScratchPad");
            Dispose();
            return Task.CompletedTask;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_Disposed)
            {
                if (disposing)
                {
                    if (null != _WorkTask && !_WorkTask.IsCompleted)
                    {
                        _CancellationTokenSource.Cancel();
                        _WorkTask.Wait();
                        _WorkTask.Dispose();
                        _WorkTask = null;
                    }
                }
                _Disposed = true;
                // TODO: clean up unmanaged objects
            }
        }

        ~ScratchPadService()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}