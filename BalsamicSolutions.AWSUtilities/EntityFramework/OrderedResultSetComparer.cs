//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.EntityFramework
{
    /// <summary>
    /// this a typed IComparer that orders by the array position
    /// of the original list being passed in. This allows us to
    /// visit the sort order after the final results are in
    /// this is not a Generic type because we need to collect
    /// the OrderedResultSetComparer early in the query
    /// process, so doing it this way simplifies the FullTextQuery
    /// </summary>
    public class OrderedResultSetComparer<EntityType> : IComparer<EntityType>
    {
        private Int16[] _Int16s = null;
        private Int32[] _Int32s = null;
        private Int64[] _Int64s = null;
        private string[] _Strings = null;
        private Guid[] _Guids = null;
        private Type _KeyType = typeof(object);
        private PropertyInfo _PropInfo = null;

        private string _PropName = null;

        public string PropertyName
        {
            get
            {
                return _PropName;
            }
        }

        private OrderedResultSetComparer(string propName)
        {
            _PropName = propName;
            _PropInfo = typeof(EntityType).GetProperty(_PropName);
        }

        public OrderedResultSetComparer(List<Int16> queryResults, string propName)
            : this(propName)
        {
            _Int16s = queryResults.ToArray();
            _KeyType = typeof(Int16);
        }

        public OrderedResultSetComparer(List<Int32> queryResults, string propName)
            : this(propName)
        {
            _Int32s = queryResults.ToArray();
            _KeyType = typeof(Int32);
        }

        public OrderedResultSetComparer(List<Int64> queryResults, string propName)
            : this(propName)
        {
            _Int64s = queryResults.ToArray();
            _KeyType = typeof(Int64);
        }

        public OrderedResultSetComparer(List<string> queryResults, string propName)
            : this(propName)
        {
            _Strings = queryResults.ToArray();
            _KeyType = typeof(string);
        }

        public OrderedResultSetComparer(List<Guid> queryResults, string propName)
            : this(propName)
        {
            _Guids = queryResults.ToArray();
            _KeyType = typeof(Guid);
        }

        /// <summary>
        /// find the index of the value
        /// </summary>
        /// <param name="findThis"></param>
        /// <returns></returns>
        private int FindPosition(Int16 findThis)
        {
            for (int idx = 0; idx < _Int16s.Length; idx++)
            {
                if (_Int16s[idx].Equals(findThis)) return idx;
            }
            return -1;
        }

        /// <summary>
        /// find the index of the value
        /// </summary>
        /// <param name="findThis"></param>
        /// <returns></returns>
        private int FindPosition(Int32 findThis)
        {
            for (int idx = 0; idx < _Int32s.Length; idx++)
            {
                if (_Int32s[idx].Equals(findThis)) return idx;
            }
            return -1;
        }

        /// <summary>
        /// find the index of the value
        /// </summary>
        /// <param name="findThis"></param>
        /// <returns></returns>
        private int FindPosition(Int64 findThis)
        {
            for (int idx = 0; idx < _Int64s.Length; idx++)
            {
                if (_Int64s[idx].Equals(findThis)) return idx;
            }
            return -1;
        }

        /// <summary>
        /// find the index of the value
        /// </summary>
        /// <param name="findThis"></param>
        /// <returns></returns>
        private int FindPosition(string findThis)
        {
            for (int idx = 0; idx < _Strings.Length; idx++)
            {
                if (_Strings[idx].Equals(findThis)) return idx;
            }
            return -1;
        }

        /// <summary>
        /// find the index of the value
        /// </summary>
        /// <param name="findThis"></param>
        /// <returns></returns>
        private int FindPosition(Guid findThis)
        {
            for (int idx = 0; idx < _Guids.Length; idx++)
            {
                if (_Guids[idx].Equals(findThis)) return idx;
            }
            return -1;
        }

        ///// <summary>
        ///// A signed integer that indicates the relative values of x and y, as shown in the following table.
        ///// a value less than 0 means x is less than y
        ///// a zero means x==y
        ///// a value greater than 0 means x is greater than y
        ///// </summary>
        ///// <param name="x"></param>
        ///// <param name="y"></param>
        ///// <returns>int</returns>
        private int CompareInternal(Int16 x, Int16 y)
        {
            int posX = FindPosition(x);
            if (posX == -1) throw new ArgumentOutOfRangeException("x", $"Not found in query results {x.ToString()}");
            int posY = FindPosition(y);
            if (posY == -1) throw new ArgumentOutOfRangeException("y", $"Not found in query results {y.ToString()}");
            //If they are in the same position then its the same record
            if (posX == posY) return 0;
            if (posX > posY) return 1;
            return -1;
        }

        ///// <summary>
        ///// A signed integer that indicates the relative values of x and y, as shown in the following table.
        ///// a value less than 0 means x is less than y
        ///// a zero means x==y
        ///// a value greater than 0 means x is greater than y
        ///// </summary>
        ///// <param name="x"></param>
        ///// <param name="y"></param>
        ///// <returns>int</returns>
        private int CompareInternal(Int32 x, Int32 y)
        {
            int posX = FindPosition(x);
            if (posX == -1) throw new ArgumentOutOfRangeException("x", $"Not found in query results {x.ToString()}");
            int posY = FindPosition(y);
            if (posY == -1) throw new ArgumentOutOfRangeException("y", $"Not found in query results {y.ToString()}");
            //If they are in the same position then its the same record
            if (posX == posY) return 0;
            if (posX > posY) return 1;
            return -1;
        }

        ///// <summary>
        ///// A signed integer that indicates the relative values of x and y, as shown in the following table.
        ///// a value less than 0 means x is less than y
        ///// a zero means x==y
        ///// a value greater than 0 means x is greater than y
        ///// </summary>
        ///// <param name="x"></param>
        ///// <param name="y"></param>
        ///// <returns>int</returns>
        private int CompareInternal(Int64 x, Int64 y)
        {
            int posX = FindPosition(x);
            if (posX == -1) throw new ArgumentOutOfRangeException("x", $"Not found in query results {x.ToString()}");
            int posY = FindPosition(y);
            if (posY == -1) throw new ArgumentOutOfRangeException("y", $"Not found in query results {y.ToString()}");
            //If they are in the same position then its the same record
            if (posX == posY) return 0;
            if (posX > posY) return 1;
            return -1;
        }

        ///// <summary>
        ///// A signed integer that indicates the relative values of x and y, as shown in the following table.
        ///// a value less than 0 means x is less than y
        ///// a zero means x==y
        ///// a value greater than 0 means x is greater than y
        ///// </summary>
        ///// <param name="x"></param>
        ///// <param name="y"></param>
        ///// <returns>int</returns>
        private int CompareInternal(string x, string y)
        {
            int posX = FindPosition(x);
            if (posX == -1) throw new ArgumentOutOfRangeException("x", $"Not found in query results {x.ToString()}");
            int posY = FindPosition(y);
            if (posY == -1) throw new ArgumentOutOfRangeException("y", $"Not found in query results {y.ToString()}");
            //If they are in the same position then its the same record
            if (posX == posY) return 0;
            if (posX > posY) return 1;
            return -1;
        }

        ///// <summary>
        ///// A signed integer that indicates the relative values of x and y, as shown in the following table.
        ///// a value less than 0 means x is less than y
        ///// a zero means x==y
        ///// a value greater than 0 means x is greater than y
        ///// </summary>
        ///// <param name="x"></param>
        ///// <param name="y"></param>
        ///// <returns>int</returns>
        private int CompareInternal(Guid x, Guid y)
        {
            int posX = FindPosition(x);
            if (posX == -1) throw new ArgumentOutOfRangeException("x", $"Not found in query results {x.ToString()}");
            int posY = FindPosition(y);
            if (posY == -1) throw new ArgumentOutOfRangeException("y", $"Not found in query results {y.ToString()}");
            //If they are in the same position then its the same record
            if (posX == posY) return 0;
            if (posX > posY) return 1;
            return -1;
        }

        ///// <summary>
        ///// A signed integer that indicates the relative values of x and y, as shown in the following table.
        ///// a value less than 0 means x is less than y
        ///// a zero means x==y
        ///// a value greater than 0 means x is greater than y
        ///// </summary>
        ///// <param name="x"></param>
        ///// <param name="y"></param>
        ///// <returns>int</returns>
        public int Compare(EntityType x, EntityType y)
        {
            //get the prop value for
            object xObj = _PropInfo.GetValue(x);
            object yObj = _PropInfo.GetValue(y);
            if (_KeyType == typeof(Int16))
            {
                return CompareInternal(Convert.ToInt16(xObj), Convert.ToInt16(yObj));
            }
            else if (_KeyType == typeof(Int32))
            {
                return CompareInternal(Convert.ToInt32(xObj), Convert.ToInt32(yObj));
            }
            else if (_KeyType == typeof(Int64))
            {
                return CompareInternal(Convert.ToInt64(xObj), Convert.ToInt64(yObj));
            }
            else if (_KeyType == typeof(string))
            {
                return CompareInternal(Convert.ToString(xObj), Convert.ToString(yObj));
            }
            else if (_KeyType == typeof(Guid))
            {
                return CompareInternal(Guid.Parse(xObj.ToString()), Guid.Parse(yObj.ToString()));
            }
            throw new InvalidCastException("Unsupported key type");
        }
    }
}