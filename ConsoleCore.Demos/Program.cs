//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
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
    internal class Program
    {

        /// <summary>
        /// main entry point for .net core demos
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static async Task Main(string[] args)
        {
            bool efDemo = false;
            bool sqsDemo = false;
            bool sqsEnqueue = false;

            if (null != args && args.Length >= 0)
            {
                for (int argIdx = 0; argIdx < args.Length; argIdx++)
                {
                    string arg = args[argIdx].ToLowerInvariant().Trim(new char[] { '/', '-', ' ' });
                    if (arg == "sqs") sqsDemo = true;
                    if (arg == "sqsenqueue") sqsEnqueue = true;
                    if (arg == "enqueue") sqsEnqueue = true;
                    if (arg == "ef") sqsDemo = true;
                    if (arg == "sqsdemo") sqsDemo = true;
                    if (arg == "efdemo") efDemo = true;
                    if (arg == "ignoresslerrors")
                    {
                        System.Net.ServicePointManager.ServerCertificateValidationCallback = new System.Net.Security.RemoteCertificateValidationCallback(delegate { return true; });
                    }
                }
            }

            if (sqsDemo)
            {
                Console.WriteLine("starting sqs queue service (ctrl-c to exit)");
                string localPath = Directory.GetCurrentDirectory();
                IHostBuilder hostbuilder = new HostBuilder()
                    .ConfigureAppConfiguration((builderContext, config) =>
                    {
                        config.SetBasePath(localPath)
                        .AddJsonFile("appsettings.json", optional: true)
                        .AddJsonFile($"appsettings.{builderContext.HostingEnvironment.EnvironmentName}.json", optional: true)
                        .AddEnvironmentVariables(prefix: "environment:");
                    })
                    .ConfigureServices((builderContext, services) =>
                    {
                        services.AddLogging();
                        services.AddOptions();
                        services.AddHostedService<SQSDemo>();
                    })
                    .ConfigureLogging((builderContext, logging) =>
                    {
                        logging.AddConsole();
                    })
                     .UseConsoleLifetime();
                IHost svcHost = hostbuilder.Build();
                using (svcHost)
                {
                    await svcHost.StartAsync();

                    //If necessary get any service references for other
                    //stuff inbetween start/wait for shutdown
                    await svcHost.WaitForShutdownAsync();
                }
                Console.WriteLine("exiting sqs queue service");
            }
            else
            {
                if (efDemo)
                {
                    Console.WriteLine("TODO build a menu to select ef initialization and/or query examples");
                }
                else if (sqsEnqueue)
                {
                    var config = GetConfiguration();
                    string queueName = config.GetValue<string>("appSettings:QueueName");
                    Console.WriteLine($"Queueing 20 messages to {queueName}");
                    SqsQueueDispatcher<SQSDemoQueueData> sqsQueue = new SqsQueueDispatcher<SQSDemoQueueData>(queueName);
                    for (int count = 0; count < 20; count++)
                    {
                        SQSDemoQueueData queueData = SQSDemoQueueData.RandomQueueData();
                        sqsQueue.EnqueueMessage(queueData);
                    }
                    Console.WriteLine($"Completed queueing 20 messages to {queueName}");
                }
                Console.WriteLine("Press X to exit...");
                char exitChar = Console.ReadKey().KeyChar;
                while (exitChar != 'X' && exitChar != 'x')
                {
                    exitChar = Console.ReadKey().KeyChar;
                }
            }

        }

        //place holder to read configuration outside
        //of the host builder
        static IConfiguration GetConfiguration()
        {
            IConfigurationBuilder builder = new ConfigurationBuilder()
               .SetBasePath(System.IO.Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
            return builder.Build();
        }
    }
}