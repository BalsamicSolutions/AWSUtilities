using Amazon.AutoScaling;
using Amazon.AutoScaling.Model;
using System;
using System.ComponentModel;
using System.Threading;
using System.Collections.Generic;
using Amazon.SQS;
using Amazon.SQS.Model;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Amazon.EC2;
using Amazon.EC2.Model;
using BalsamicSolutions.AWSUtilities.Extensions;

namespace BalsamicSolutions.AWSUtilities.EC2
{
    //	If an autoscaling instance is scaling down, and lifecycle hooks are enabled
    //  for that instance then we can detect the transition to "terminate:wait" and
    //  signal all of our queue  processors that we want to shut down cleanly
    //  This class acts as a singleton and all queues can subscribe to shtudown events
    //  shutdown cleanly if the EC2 instance is terminated.

    // If the instance is in an auto scaling group but no lifecycle hook is installed
    //then we check for a setting named ASGIdleTerminationWaitInMinutes, if that
    // is defined then we monitor for idel time in that range and this instnace then
    //scales itself down and terminates itself

    //normally this class is statically accessed via the Instance method
    //other techniques exist, but moslty for debugging purposes

    /// <summary>
    /// this class is a monitor for autoscale events
    /// it also handles holding processes in wait
    /// state until all clients are deregistered
    /// </summary>
    public class AutoScaleLifeCycleMonitor : IAutoScaleLifeCycleMonitor, IHostedService, IDisposable
    {
        public int AwsAutoScaleWatchDogPollIntervalInSeconds { get; set; } = 120;
        public int AutoScaleGroupInstanceIdleTerminationWaitInMinutes { get; set; } = 10;

        private static AutoScaleLifeCycleMonitor _Instance = null;
        private static object _StaticLockProxy = new object();
        private string _AutoScalingGroupName = null;
        private string _InstanceId = string.Empty;
        private bool _InTerminateWait = false;
        private DateTime _LastActivity = DateTime.Now;
        private string _TerminatingLifeCycleHookName = null;
        private object _LockProxy = new object();
        private long _RefCount = 0;
        private Timer _Timer = null;
        private TimeSpan _TimerInterval;

        /// <summary>
        /// normal usage for ASG hosted instance
        /// </summary>
        public AutoScaleLifeCycleMonitor()
            : this(Amazon.Util.EC2InstanceMetadata.InstanceId)
        {
        }

        public AutoScaleLifeCycleMonitor(string instanceId)
        {
            //get instance ID
            _InstanceId = instanceId;
        }

        /// <summary>
        /// ctor from config instead of from detection
        /// </summary>
        /// <param name="instanceId"></param>
        /// <param name="asgName"></param>
        public AutoScaleLifeCycleMonitor(string instanceId, string asgName)
        {
            //get instance ID
            _InstanceId = instanceId;
            _AutoScalingGroupName = asgName;
        }

        /// <summary>
        /// ctor from config instead of from detection
        /// </summary>
        /// <param name="instanceId"></param>
        /// <param name="asgName"></param>
        /// <param name="hookName"></param>
        public AutoScaleLifeCycleMonitor(string instanceId, string asgName, string hookName)
        {
            //get instance ID
            _InstanceId = instanceId;
            _AutoScalingGroupName = asgName;
            _TerminatingLifeCycleHookName = hookName;
        }

        /// <summary>
        /// static singleton without DI, its not
        /// the same as the DI instance in the container
        /// </summary>
        public static AutoScaleLifeCycleMonitor Instance
        {
            get
            {
                lock (_StaticLockProxy)
                {
                    if (null == _Instance)
                    {
                        _Instance = new AutoScaleLifeCycleMonitor();
                        _Instance.Start();
                    }
                    return _Instance;
                }
            }
            private set
            {
                lock (_StaticLockProxy)
                {
                    if (null != _Instance)
                    {
                        _Instance.Dispose();
                    }
                    _Instance = value;
                }
            }
        }

        /// <summary>
        /// Adds a reference if we are not shutting down
        /// </summary>
        /// <returns></returns>
        public bool AddRef()
        {
            bool returnValue = false;
            lock (_LockProxy)
            {
                if (!_InTerminateWait)
                {
                    _RefCount++;
                    returnValue = true;
                }
            }
            return returnValue;
        }
        /// <summary>
        /// void removes ref count
        /// </summary>
        public void Release()
        {
            long refCount = 0;
            lock (_LockProxy)
            {
                _RefCount--;
                refCount = _RefCount;
            }
            if (refCount <= 0 && _InTerminateWait)
            {
                TryCompleteTerminateWait();
            }
        }

        /// <summary>
        /// updates the last time stamp
        /// </summary>
        public void Tickle()
        {
            lock (_LockProxy)
            {
                _LastActivity = DateTime.Now;
            }
        }


        /// <summary>
        /// clean up
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (null != _Timer)
                {
                    _Timer.Change(Timeout.Infinite, 0);
                    _Timer.Dispose();
                    _Timer = null;
                    if (_RefCount > 0)
                    {
                        OnBeforeTerminate(new EventArgs());
                    }
                }
            }
        }

        /// <summary>
        /// clean up
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        /// IHostedService startup
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task StartAsync(CancellationToken cancellationToken)
        {
            lock (_StaticLockProxy)
            {
                if (null == _Instance)
                {
                    Start();
                    _Instance = this;
                }
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// IHostedService stop
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            lock (_StaticLockProxy)
            {
                Dispose();
                _Instance = null;
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// start the mointor
        /// </summary>
        public void Start()
        {
            lock (_StaticLockProxy)
            {
                //dont restart if we have a timer
                if (null == _Timer)
                {
                    //find out if we are in an autoscaling group
                    //if so start timer   to check state
                    if (!_AutoScalingGroupName.IsNullOrEmpty() || IsAutoscaledInstance())
                    {
                        if (_TerminatingLifeCycleHookName.IsNullOrEmpty())
                        {
                            //if there is a 
                            if (AutoScaleGroupInstanceIdleTerminationWaitInMinutes > 0)
                            {
                                _TimerInterval = TimeSpan.FromSeconds(60);
                                _Timer = new Timer(SelfTerminateCheckTimerCallBack, null, _TimerInterval, _TimerInterval);
                            }
                        }
                        else
                        {
                            _TimerInterval = TimeSpan.FromSeconds(AwsAutoScaleWatchDogPollIntervalInSeconds);
                            _Timer = new Timer(LifeCycleHookCheckTimerCallBack, null, _TimerInterval, _TimerInterval);
                        }
                    }
                }
            }
        }


        /// <summary>
        /// signals AWS to move on from terminate
        /// wait by completing the action
        /// </summary>
        public bool TryCompleteTerminateWait()
        {
            bool returnValue = false;
            //Ok now  its complicated, we need the LifecycleHookName and LifecycleActionToken
            //        to complete the request, there should be a message for us in the queue
            if (!(_TerminatingLifeCycleHookName.IsNullOrWhiteSpace()))
            {
                using (AmazonAutoScalingClient scalingClient = new AmazonAutoScalingClient())
                {
                    CompleteLifecycleActionRequest completeLifecycleActionRequest = new CompleteLifecycleActionRequest
                    {
                        InstanceId = _InstanceId,
                        LifecycleActionResult = "CONTINUE",
                        AutoScalingGroupName = _AutoScalingGroupName,
                        LifecycleHookName = _TerminatingLifeCycleHookName
                    };
                    try
                    {
                        CompleteLifecycleActionResponse completeLifecycleActionResponse = scalingClient.CompleteLifecycleActionAsync(completeLifecycleActionRequest).Result;
                        if (completeLifecycleActionResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                        {
                            returnValue = true;
                        }
                    }
                    catch (Exception)
                    {
                        //Yes AWS throws Exceptions and System.AggregrateExceptions
                        //so for now this is the right way to handle it
                        returnValue = false;
                    }
                }
            }
            return returnValue;
        }

        /// <summary>
        /// detact this instance from the ASG
        /// and decrement the ASG desired 
        /// </summary>
        /// <returns></returns>
        private bool DetachFromAutoScalingGroup()
        {
            bool returnValue = false;
            using (AmazonAutoScalingClient scalingClient = new AmazonAutoScalingClient())
            {
                DetachInstancesRequest detachRequest = new DetachInstancesRequest
                {
                    AutoScalingGroupName = _AutoScalingGroupName,
                    InstanceIds = new List<string>(new string[] { _InstanceId }),
                    ShouldDecrementDesiredCapacity = true
                };
                try
                {
                    DetachInstancesResponse detachResponse = scalingClient.DetachInstancesAsync(detachRequest).Result;
                    if (detachResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                    {
                        returnValue = true;
                    }
                }
                catch (Exception)
                {
                    //Yes AWS throws Exceptions and System.AggregrateExceptions
                    //so for now this is the right way to handle it
                    returnValue = false;
                }
            }
            return returnValue;
        }

        /// <summary>
        /// checks to see if this is an autoscaled instance
        /// </summary>
        /// <param name="instanceId"></param>
        /// <returns></returns>
        private bool IsAutoscaledInstance()
        {
            bool returnValue = false;
            if (!_InstanceId.IsNullOrWhiteSpace())
            {
                using (AmazonAutoScalingClient scalingClient = new AmazonAutoScalingClient())
                {
                    DescribeAutoScalingInstancesRequest describeAutoScalingInstancesRequest = new DescribeAutoScalingInstancesRequest
                    {
                        InstanceIds = new List<string>(new string[] { _InstanceId })
                    };
                    try
                    {
                        DescribeAutoScalingInstancesResponse describeAutoScalingInstancesResponse = scalingClient.DescribeAutoScalingInstancesAsync(describeAutoScalingInstancesRequest).Result;
                        if (describeAutoScalingInstancesResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                        {
                            foreach (AutoScalingInstanceDetails instDetail in describeAutoScalingInstancesResponse.AutoScalingInstances)
                            {
                                if (instDetail.InstanceId.CaseInsensitiveEquals(_InstanceId))
                                {
                                    _AutoScalingGroupName = instDetail.AutoScalingGroupName;
                                    returnValue = true;
                                    break;
                                }
                            }
                        }
                    }
                    catch (Exception)
                    {
                        //Yes AWS throws Exceptions and System.AggregrateExceptions
                        //so for now this is the right way to handle it
                        returnValue = false;
                    }
                    if (returnValue)
                    {
                        DescribeLifecycleHooksRequest describeLifecycleHooksRequest = new DescribeLifecycleHooksRequest
                        {
                            AutoScalingGroupName = _AutoScalingGroupName
                        };
                        try
                        {
                            DescribeLifecycleHooksResponse describeLifecycleHooksResponse = scalingClient.DescribeLifecycleHooksAsync(describeLifecycleHooksRequest).Result;

                            if (describeLifecycleHooksResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                            {
                                foreach (LifecycleHook lifecycleHook in describeLifecycleHooksResponse.LifecycleHooks)
                                {
                                    if (lifecycleHook.LifecycleTransition.CaseInsensitiveEquals("autoscaling:EC2_INSTANCE_TERMINATING"))
                                    {
                                        _TerminatingLifeCycleHookName = lifecycleHook.LifecycleHookName;
                                    }
                                }
                            }
                        }
                        catch (Exception)
                        {
                            //Yes AWS throws Exceptions and System.AggregrateExceptions
                            //so for now this is the right way to handle it
                            returnValue = false;
                        }
                    }
                }
            }
            return returnValue;
        }

        /// <summary>
        /// checks to see if this instance is in the
        /// terminate wait state
        /// </summary>
        /// <returns></returns>
        private bool IsInTerminateWaitState()
        {
            bool returnValue = false;
            using (AmazonAutoScalingClient scalingClient = new AmazonAutoScalingClient())
            {
                DescribeAutoScalingInstancesRequest describeAutoScalingInstancesRequest = new DescribeAutoScalingInstancesRequest
                {
                    InstanceIds = new List<string>(new string[] { _InstanceId })
                };
                try
                {
                    DescribeAutoScalingInstancesResponse describeAutoScalingInstancesResponse = scalingClient.DescribeAutoScalingInstancesAsync(describeAutoScalingInstancesRequest).Result;
                    if (describeAutoScalingInstancesResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                    {
                        foreach (AutoScalingInstanceDetails instDetail in describeAutoScalingInstancesResponse.AutoScalingInstances)
                        {
                            if (instDetail.InstanceId.Equals(_InstanceId, StringComparison.OrdinalIgnoreCase))
                            {
                                if (instDetail.LifecycleState.CaseInsensitiveEquals("Terminating:Wait"))
                                {
                                    returnValue = true;
                                }
                                break;
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    //Yes AWS throws Exceptions and System.AggregrateExceptions
                    //so for now this is the right way to handle it
                    returnValue = false;
                }
            }
            return returnValue;
        }


        /// <summary>
        /// event to signal clients that they should
        /// start to shut down
        /// </summary>
        public event EventHandler BeforeTerminate;

        /// <summary>
        /// calls the TerminateWait event
        /// </summary>
        private void OnBeforeTerminate(EventArgs e)
        {
            EventHandler beforeTerminate = Instance.BeforeTerminate;
            if (null != beforeTerminate)
            {
                beforeTerminate(this, e);
            }
        }



        /// <summary>
        /// check for the instance state and
        /// begin the shutdown process if necessary
        /// </summary>
        /// <param name="state"></param>
        private void LifeCycleHookCheckTimerCallBack(object state)
        {

            _Timer.Change(Timeout.Infinite, 0);
            if (IsInTerminateWaitState())
            {
                long refCount = 0;
                lock (_LockProxy)
                {
                    _InTerminateWait = true;
                    refCount = _RefCount;
                }
                _Timer.Dispose();
                _Timer = null;
                OnBeforeTerminate(new EventArgs());
                if (refCount <= 0)
                {
                    //we will signal that we are done
                    TryCompleteTerminateWait();
                }
                else
                {
                    _Timer.Change(_TimerInterval, _TimerInterval);
                }
            }
            else
            {
                _Timer.Change(_TimerInterval, _TimerInterval);
            }
        }

        /// <summary>
        /// terminates an instance
        /// </summary>
        /// <param name="instanceId"></param>
        /// <param name="ec2Client"></param>
        /// <returns></returns>
        private bool TerminateInstance(string instanceId)
        {
            bool returnValue = false;
            using (AmazonEC2Client ec2Client = new AmazonEC2Client())
            {

                TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest();
                terminateRequest.InstanceIds = new List<string>() { instanceId };
                try
                {
                    TerminateInstancesResponse terminateResponse = ec2Client.TerminateInstancesAsync(terminateRequest).Result;
                    foreach (InstanceStateChange stateChange in terminateResponse.TerminatingInstances)
                    {
                        if (stateChange.InstanceId.Equals(instanceId, StringComparison.OrdinalIgnoreCase))
                        {
                            returnValue = true;
                            break;
                        }
                    }
                }
                catch (AmazonEC2Exception ec2Error)
                {
                    if (ec2Error.ErrorCode.CaseInsensitiveContains("InvalidInstanceID.NotFound"))
                    {
                        returnValue = false;
                    }
                    else
                    {
                        throw;
                    }
                }
                return returnValue;
            }


        }

        /// <summary>
        /// timer callback for ASG member without lifecycle hook
        /// </summary>
        /// <param name="state"></param>
        private void SelfTerminateCheckTimerCallBack(object state)
        {
            _Timer.Change(Timeout.Infinite, 0);
            bool shutItAllDown = false;

            lock (_LockProxy)
            {
                //if we have no references and no activity for our window period
                TimeSpan timeSpan = DateTime.Now - _LastActivity;
                if (timeSpan.Minutes >= AutoScaleGroupInstanceIdleTerminationWaitInMinutes)
                {
                    //then shut it down
                    shutItAllDown = true;
                }
            }
            if (shutItAllDown)
            {
                _Timer.Dispose();
                _Timer = null;
                OnBeforeTerminate(new EventArgs());
                if (DetachFromAutoScalingGroup())
                {
                    TerminateInstance(_InstanceId);
                }
            }
            else
            {
                _Timer.Change(_TimerInterval, _TimerInterval);
            }
        }

    }
}
