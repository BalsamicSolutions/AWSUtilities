//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations
{
    /// <summary>
    /// attribute on a property indicated the property should
    /// participate in a full text index
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Class, AllowMultiple = false)]
    public class FullTextAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets the index name. only applies to Class's
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets or sets a number that determines the column ordering
        /// only applies to Properties
        /// </summary>
        public int Order { get; }

        /// <summary>
        /// Initializes a new IndexAttribute
        /// </summary>
        public FullTextAttribute()
        {
            Order = -1;
            Name = string.Empty;
        }

        /// <summary>
        /// Initializes a new IndexAttribute instance for the Classes IndexName attribute
        /// </summary>
        /// <param name="name">The index name.</param>
        public FullTextAttribute(string name)
        {
            Order = -1;
            Name = name;
        }

        /// <summary>
        /// Initializes a new IndexAttribute instance for a Property with an order to it
        /// </summary>
        /// <param name="order">A number which will be used to determine column ordering for multi-column indexes.</param>
        public FullTextAttribute(int order)
        {
            Order = order;
            Name = string.Empty;
        }
    }
}