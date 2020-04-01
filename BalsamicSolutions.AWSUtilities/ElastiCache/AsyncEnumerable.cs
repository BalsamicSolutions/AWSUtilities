//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    /// <summary>
    /// simple wrapper class co convert an 
    /// IEnumerable to an IAsyncEnumerable
    /// </summary>
    public class AsyncEnumerable<T> : IAsyncEnumerable<T>, IAsyncEnumerator<T>
    {
        private IEnumerable<T> _Values = null;
        private IEnumerator<T> _Enumerator = null;

        public AsyncEnumerable(IEnumerable<T> values)
        {
            if (null == values) throw new ArgumentNullException(nameof(values));
            _Values = values;
        }

        public T Current
        {
            get
            {
                return _Enumerator.Current;
            }
        }

        public ValueTask DisposeAsync()
        {
            if (null != _Enumerator) _Enumerator.Dispose();
            return new ValueTask(Task.CompletedTask);
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (null != _Enumerator) _Enumerator.Dispose();
            _Enumerator = _Values.GetEnumerator();
            return this;
        }

        public ValueTask<bool> MoveNextAsync()
        {
            bool okDokey = _Enumerator.MoveNext();
            return new ValueTask<bool>(Task.FromResult(okDokey));
        }
    }
}