using System;
using System.Collections.Generic;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.EC2
{
   /// <summary>
    /// interface for AutoScaleLifeCycleMonitor
    /// </summary>
    public interface IAutoScaleLifeCycleMonitor
    {
        event EventHandler BeforeTerminate;

        bool AddRef();

        void Release();
    }
}
