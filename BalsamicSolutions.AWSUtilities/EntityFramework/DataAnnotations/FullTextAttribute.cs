using System;
using System.Collections.Generic;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations
{
    /// <summary>
    /// attribute on a property indicated the property should
    /// participate in a full text index
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class FullTextAttribute:Attribute
    {
         /// <summary>
        /// Gets or sets the index name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets or sets a number that determines the column ordering 
        /// </summary>
        public int Order { get; }

         /// <summary>
        /// Initializes a new IndexAttribute instance for an index that will be named by convention and has no column order, uniqueness specified.
        /// </summary>
        public FullTextAttribute() : this(string.Empty, -1)
        {
        }

        /// <summary>
        /// Initializes a new IndexAttribute instance for an index with the given name and has no column order, uniqueness specified.
        /// </summary>
        /// <param name="name">The index name.</param>
        public FullTextAttribute(string name) : this(name, -1)
        {
        }

        /// <summary>
        /// Initializes a new IndexAttribute instance for an index with the given name and column order, but with no uniqueness specified.
        /// </summary>
        /// <param name="name">The index name.</param>
        /// <param name="order">A number which will be used to determine column ordering for multi-column indexes.</param>
        public FullTextAttribute(string name, int order)
        {
            this.Name = name;
            this.Order = order;
        }
    }
}
