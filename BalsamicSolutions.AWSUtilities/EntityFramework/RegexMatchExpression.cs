using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Utilities;
using MySql.Data.EntityFrameworkCore.Query.Internal;

namespace BalsamicSolutions.AWSUtilities.EntityFramework
{
    /// <summary>
    /// handles sql generation for BalsamicSolutions.AWSUtilities.EntityFramework.DbFunctions.Regex(c.ContactName, "^M$"));
    /// </summary>
    public class RegexMatchExpression : SqlFragmentExpression
    {
        /// <summary>
        ///     Creates a new instance of RegexMatchExpression. We are a SqlFragmentExpression 
        ///     implementation so that EF Core lets us go through as a context addition
        ///     and not a global service addition (look at dbcontext base OnModelCreating) 
        /// </summary>
        /// <param name="match"> The expression to match, usually the column name. </param>
        /// <param name="regexPattern"> The regular expression pattern to match. </param>
        public RegexMatchExpression(SqlExpression match, SqlExpression regexPattern)
            : base("")
        {
            Match = match;
            RegexPattern = regexPattern;
        }

        public override bool CanReduce
        {
            get
            {
                return false;
            }
        }

        public virtual SqlExpression Match { get; }

        public virtual SqlExpression RegexPattern { get; }

        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            return ReferenceEquals(this, obj) ? true : obj.GetType() == GetType() && Equals((RegexMatchExpression)obj);
        }

        public override int GetHashCode()
        {
            unchecked // Overflow is fine, just wrap
            {
                int returnValue = Match.GetHashCode();
                returnValue = (returnValue * 23) + RegexPattern.GetHashCode();

                return returnValue;
            }
        }

        public override void Print(ExpressionPrinter expressionPrinter)
        {
            expressionPrinter.Append(this.ToString());
        }

        public override string ToString()
        {
            return $"{Match} REGEXP {RegexPattern}";
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            Type visitorType = visitor.GetType();
            System.Diagnostics.Trace.WriteLine(visitorType.ToString());
            QuerySqlGenerator queryVisitor = visitor as QuerySqlGenerator;
            //if we were not based on SqlFragmentExpression then this would fail
            if (null == queryVisitor) return base.Accept(visitor);
            visitor.Visit(Match);
            visitor.Visit(new SqlFragmentExpression(" REGEXP "));
            visitor.Visit(RegexPattern);
            return this;
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var newMatchExpression = (SqlExpression)visitor.Visit(Match);
            var newPatternExpression = (SqlExpression)visitor.Visit(RegexPattern);

            return newMatchExpression != Match
                   || newPatternExpression != RegexPattern

                ? new RegexMatchExpression(newMatchExpression, newPatternExpression)
                : this;
        }

        private bool Equals(RegexMatchExpression other)
        {
            bool returnValue = Equals(Match, other.Match)
                            && Equals(RegexPattern, other.RegexPattern);
            return returnValue;
        }
    }
}
