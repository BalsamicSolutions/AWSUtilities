using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.AutoScaling;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using System.Linq;
using BalsamicSolutions.AWSUtilities.EC2;
using BalsamicSolutions.AWSUtilities.EntityFramework.Extensions;

namespace BalsamicSolutions.AWSUtilities.SQS
{
    /// <summary>
    /// a handler for AWS SQS Queues
    /// </summary>
    public class SqsQueueDispatcher<T> : IDisposable where T : class
    {
        /// <summary>
        /// payload class for dequeing messages
        /// </summary>
        public class SqsPayload
        {
            public T MessageData { get; set; }

            public string MessageId { get; set; }

            public string RecieptHandle { get; set; }
        }


        public const int QUEUE_WAIT_INTERVAL_SECONDS = 120;
        public const int QUEUE_VISIBILITY_TIMEOUT_SECONDS = 300;
        public const int MAX_DISPATCH_COUNT = 15;
        private AutoScaleLifeCycleMonitor _AutoScaleLifeCycleMonitor = null;
        private AmazonSQSClient _AmazonSQSClient = null;
        private int _ConcurrentDispatchCount = 1;
        private object _LockProxy = new object();
        Task[] _DispatchTasks = null;
        private CancellationTokenSource _CancellationTokenSource = null;
        private string _QueueName = null;
        private string _QueueUrl = null;
        internal static ConcurrentDictionary<string, string> _QueueUrls = new ConcurrentDictionary<string, string>();
        private ILogger _SysLogger = null;
        private HashSet<string> _OpenHandles = new HashSet<string>();
        private Task _HandleRefresh = null;
        private bool _Disposed = false;

        /// <summary>
        /// ctor with no logger and autocalculate the capacity
        /// </summary>
        /// <param name="queueName"></param>
        public SqsQueueDispatcher(string queueName)
             : this(queueName, null, -1)
        {
        }

        /// <summary>
        /// ctor with no loger and a specified capacity
        /// </summary>
        /// <param name="queueName"></param>
        /// <param name="concurrentDispatchCount"></param>
        public SqsQueueDispatcher(string queueName, int concurrentDispatchCount)
            : this(queueName, null, concurrentDispatchCount)
        {
        }

        /// <summary>
        /// ctor with a logger and autocalculate the capacity
        /// </summary>
        /// <param name="queueName"></param>
        /// <param name="sysLogger"></param>
        public SqsQueueDispatcher(string queueName, ILogger sysLogger)
        : this(queueName, sysLogger, -1)
        {
        }

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="queueName"></param>
        /// <param name="awsConfig"></param>
        /// <param name="logHandler"></param>
        /// <param name="sysLogger"></param>
        /// <param name="concurrentDispatchCount"></param>
        public SqsQueueDispatcher(string queueName, ILogger sysLogger, int concurrentDispatchCount)
        {
            _AmazonSQSClient = new AmazonSQSClient();
            _QueueName = queueName.ToLowerInvariant();
            _QueueUrl = GetOrCreateQueue(_AmazonSQSClient, _QueueName);
            _SysLogger = sysLogger;
            _AutoScaleLifeCycleMonitor = AutoScaleLifeCycleMonitor.Instance;
            _AutoScaleLifeCycleMonitor.BeforeTerminate += AutoScaleLifeCycleMonitor_TerminateWait;
            _ConcurrentDispatchCount = concurrentDispatchCount;
            //-1 is the flag to calculate concurrency 
            if (_ConcurrentDispatchCount == -1)
            {
                _ConcurrentDispatchCount = (Environment.ProcessorCount / 2) - 1;
                if (_ConcurrentDispatchCount < 2) _ConcurrentDispatchCount = 2;
            }
            if (_ConcurrentDispatchCount < 1) _ConcurrentDispatchCount = 1;
            if (_ConcurrentDispatchCount > MAX_DISPATCH_COUNT) _ConcurrentDispatchCount = MAX_DISPATCH_COUNT;
        }

        /// <summary>
        /// shutdown callback cancelatioln
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void AutoScaleLifeCycleMonitor_TerminateWait(object sender, EventArgs e)
        {
            if (null != _CancellationTokenSource)
            {
                _CancellationTokenSource.Cancel();
            }
        }

        /// <summary>
        /// unsubscribe
        /// </summary>
        public void UnSubscribe()
        {
            lock (_LockProxy)
            {
                if (null != _DispatchTasks)
                {
                    _CancellationTokenSource.Cancel();
                    _HandleRefresh.Wait(QUEUE_WAIT_INTERVAL_SECONDS);
                    _HandleRefresh.Dispose();
                    _HandleRefresh = null;
                    Task.WaitAll(_DispatchTasks, QUEUE_WAIT_INTERVAL_SECONDS);
                    for (int taskIdx = 0; taskIdx < _DispatchTasks.Length; taskIdx++)
                    {
                        try
                        {
                            _DispatchTasks[taskIdx].Dispose();
                        }
                        catch { }
                    }
                    _CancellationTokenSource.Dispose();
                    _CancellationTokenSource = null;
                    _DispatchTasks = null;
                }
            }
        }

        /// <summary>
        /// extend ownerships of dispatched work items
        /// that are still pending completion
        /// </summary>
        void ExtendReceiptHandleOwnerships()
        {
            lock (_LockProxy)
            {
                foreach (string receiptHandle in _OpenHandles)
                {
                    ExtendMessageOwnerShip(receiptHandle, 300);
                }
            }
        }

        /// <summary>
        /// enque a message
        /// </summary>
        /// <returns></returns>
        public string EnqueueMessage(T queueMessage)
        {
            SendMessageRequest sendMessage = new SendMessageRequest
            {
                MessageBody = Newtonsoft.Json.JsonConvert.SerializeObject(queueMessage),
                QueueUrl = _QueueUrl,
                DelaySeconds = (int)TimeSpan.FromSeconds(5).TotalSeconds
            };
            SendMessageResponse sendMessageResponse = _AmazonSQSClient.SendMessageAsync(sendMessage).Result;
            return sendMessageResponse.MessageId;
        }

        /// <summary>
        /// add some time so that we can continue to work on this message
        /// </summary>
        /// <param name="recieptHandle"></param>
        /// <param name="extensionInSeconds"></param>
        /// <param name="highPriority"></param>
        /// <returns></returns>
        public bool ExtendMessageOwnerShip(string recieptHandle, int extensionInSeconds)
        {
            ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest
            {
                ReceiptHandle = recieptHandle,
                VisibilityTimeout = extensionInSeconds,
                QueueUrl = _QueueUrl
            };
            ChangeMessageVisibilityResponse ignoreThis = _AmazonSQSClient.ChangeMessageVisibilityAsync(changeMessageVisibilityRequest).Result;
            return ignoreThis.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }

        /// <summary>
        /// delete a message from the queue
        /// </summary>
        /// <param name="messageId"></param>
        public bool DeleteMessage(string recieptHandle)
        {
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = _QueueUrl,
                ReceiptHandle = recieptHandle
            };
            DeleteMessageResponse awsResp = _AmazonSQSClient.DeleteMessageAsync(deleteMessageRequest).Result;
            return awsResp.HttpStatusCode == System.Net.HttpStatusCode.OK;
        }

        /// <summary>
        /// gets the next message(s) to be processed
        /// </summary>
        /// <returns></returns>
        public List<SqsPayload> DequeueNext(int messageCount = 1)
        {
            if (messageCount < 0 || messageCount > 10) throw new ArgumentOutOfRangeException("messageCount");
            List<SqsPayload> returnValue = new List<SqsPayload>();
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = _QueueUrl,
                MaxNumberOfMessages = messageCount,
                VisibilityTimeout = QUEUE_VISIBILITY_TIMEOUT_SECONDS,
                WaitTimeSeconds = 20
            };
            ReceiveMessageResponse awsResp = _AmazonSQSClient.ReceiveMessageAsync(receiveMessageRequest).Result;

            if (HttpStatusCode.OK == awsResp.HttpStatusCode)
            {
                foreach (Message responseMessage in awsResp.Messages)
                {

                    SqsPayload payload = new SqsPayload
                    {
                        RecieptHandle = responseMessage.ReceiptHandle,
                        MessageId = responseMessage.MessageId,
                    };
                    if (typeof(T) == typeof(string))
                    {
                        payload.MessageData = responseMessage.Body as T;
                    }
                    else
                    {
                        payload.MessageData = responseMessage.Body.FromJson<T>();
                    }
                    returnValue.Add(payload);
                }
            }
            return returnValue;
        }

        /// <summary>
        /// gets the next message(s) to be processed
        /// </summary>
        /// <returns></returns>
        private async Task<List<SqsPayload>> DequeueNextAsync(CancellationToken cancellationToken, int messageCount)
        {
            List<SqsPayload> returnValue = new List<SqsPayload>();
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = _QueueUrl,
                MaxNumberOfMessages = messageCount,
                VisibilityTimeout = QUEUE_VISIBILITY_TIMEOUT_SECONDS,
                WaitTimeSeconds = 20
            };
            ReceiveMessageResponse awsResp = await _AmazonSQSClient.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);
            if (HttpStatusCode.OK == awsResp.HttpStatusCode && !cancellationToken.IsCancellationRequested)
            {
                foreach (Message responseMessage in awsResp.Messages)
                {
                    SqsPayload payload = new SqsPayload
                    {
                        RecieptHandle = responseMessage.ReceiptHandle,
                        MessageId = responseMessage.MessageId,
                    };
                    if (typeof(T) == typeof(string))
                    {
                        payload.MessageData = responseMessage.Body as T;
                    }
                    else
                    {
                        payload.MessageData = responseMessage.Body.FromJson<T>();
                    }
                    returnValue.Add(payload);
                }
            }
            return returnValue;
        }

        /// <summary>
        /// subscribes a callback method to this queue
        /// </summary>
        /// <param name="callBackHandler"></param>
        public void Subscribe(Func<T, string, SqsQueueDispatcher<T>, bool> callBackHandler)
        {
            lock (_LockProxy)
            {

                if (null == _DispatchTasks)
                {
                    _CancellationTokenSource = new CancellationTokenSource();
                    CancellationToken cancellationToken = _CancellationTokenSource.Token;
                    //start the handle refresh monitor
                    if (null == _HandleRefresh)
                    {
                        _HandleRefresh = Task.Factory.StartNew(async () =>
                        {
                            while (!cancellationToken.IsCancellationRequested)
                            {
                                ExtendReceiptHandleOwnerships();
                                await Task.Delay(TimeSpan.FromSeconds(QUEUE_WAIT_INTERVAL_SECONDS), cancellationToken);
                            }
                        }, cancellationToken);
                    }

                    //create a task for each _concurrent dispatcher
                    _DispatchTasks = new Task[_ConcurrentDispatchCount];
                    //await ProcessDispatchQueue(callBackHandler, cancellationToken);
                    for (int taskIdx = 0; taskIdx < _ConcurrentDispatchCount; taskIdx++)
                    {
                        _DispatchTasks[taskIdx] = Task.Factory.StartNew(async () =>
                        {
                            try
                            {
                                await DispatchOneQueuedWorkItem(callBackHandler, cancellationToken);
                            }
                            catch (OperationCanceledException)
                            {
                                //Nothing to do
                            }
                        }, cancellationToken);
                    }
                }
            }
        }

        /// <summary>
        /// calls the handler with a single entry
        /// </summary>
        /// <param name="queuePayload"></param>
        void DispatchPayloadToCallBackHandler(SqsPayload queuePayload, Func<T, string, SqsQueueDispatcher<T>, bool> callBackHandler)
        {
            try
            {
                lock (_LockProxy)
                {
                    //add the reciept handle to the list of open handlers
                    _OpenHandles.Add(queuePayload.RecieptHandle);
                }
                bool deleteMessage = callBackHandler(queuePayload.MessageData, queuePayload.RecieptHandle, this);
                if (deleteMessage)
                {
                    //if the return is true, then it was handled so remove it
                    DeleteMessage(queuePayload.RecieptHandle);
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (ThreadAbortException)
            {
                throw;
            }
            catch (Exception logThis)
            {

                if (null != _SysLogger)
                {
                    _SysLogger.LogError(logThis.ExceptionText());

                }
                if (null != _SysLogger)
                {
                    _SysLogger.LogError(logThis, string.Format("Error occurred in queue handler for {0}.", _QueueName));
                }

                System.Diagnostics.Trace.WriteLine(logThis.Message);

            }
            finally
            {
                lock (_LockProxy)
                {
                    //remove the recipet handler for the open handles list
                    _OpenHandles.Remove(queuePayload.RecieptHandle);
                }
            }
        }

        /// <summary>
        /// actual task for processing dispatchs
        /// </summary>
        async Task DispatchOneQueuedWorkItem(Func<T, string, SqsQueueDispatcher<T>, bool> callBackHandler, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (_AutoScaleLifeCycleMonitor.AddRef())
                {
                    bool shouldSleep = false;
                    try
                    {
                        //We are in a task so go get one item
                        List<SqsPayload> dispatchQueue = await DequeueNextAsync(cancellationToken, 1);
                        if (dispatchQueue.Count > 0 && !cancellationToken.IsCancellationRequested)
                        {
                            _AutoScaleLifeCycleMonitor.Tickle();
                            DispatchPayloadToCallBackHandler(dispatchQueue.First(), callBackHandler);
                        }
                        else
                        {
                            //we got nothing so pause the polling for a bit
                            shouldSleep = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (null != _SysLogger)
                        {
                            _SysLogger.LogError(ex.ToString());
                        }
                    }
                    finally
                    {
                        _AutoScaleLifeCycleMonitor.Release();
                    }
                    if (shouldSleep)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(QUEUE_WAIT_INTERVAL_SECONDS), cancellationToken);
                    }
                }
            }
        }


        /// <summary>
        /// initialize queue's for use
        /// </summary>
        /// <param name="sqsClient"></param>
        /// <param name="queueName"></param>
        /// <returns></returns>
        public static string GetOrCreateQueue(AmazonSQSClient sqsClient, string queueName)
        {
            queueName = queueName.ToLowerInvariant();
            string returnValue;
            if (!_QueueUrls.TryGetValue(queueName, out returnValue))
            {
                GetQueueUrlResponse respQueueUrl = null;
                try
                {
                    respQueueUrl = sqsClient.GetQueueUrlAsync(queueName).Result;
                }
                catch
                {
                    respQueueUrl = null;
                }
                if (null == respQueueUrl || HttpStatusCode.OK != respQueueUrl.HttpStatusCode)
                {
                    CreateQueueResponse respQueueCreate = sqsClient.CreateQueueAsync(queueName).Result;
                    if (HttpStatusCode.OK != respQueueCreate.HttpStatusCode)
                    {
                        throw new ApplicationException("Unexpected result creating SQS: " + respQueueCreate.HttpStatusCode);
                    }
                    returnValue = respQueueCreate.QueueUrl;
                }
                else
                {
                    returnValue = respQueueUrl.QueueUrl;
                }
                _QueueUrls[queueName] = returnValue;
            }
            return returnValue;
        }



        protected virtual void Dispose(bool disposing)
        {
            if (!_Disposed)
            {
                if (disposing)
                {
                    UnSubscribe();
                    if (null != _AmazonSQSClient)
                    {
                        _AmazonSQSClient.Dispose();
                        _AmazonSQSClient = null;
                    }
                }


                _Disposed = true;
            }
        }


        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
        }


    }
}
