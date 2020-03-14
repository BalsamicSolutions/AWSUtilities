//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Web;

namespace BalsamicSolutions.AWSUtilities.Extensions
{
    /// <summary>
    /// extensions to IQuerable to support
    ///  FULLTEXT functionality
    /// </summary>
    public static class FullTextQueryableExtensions
    {
        //https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html

        public static IQueryable<TEntity> NaturalLanguageFullTextSearch<TEntity>(this DbSet<TEntity> thisDbSet, string searchText) where TEntity : class
        {
            throw new NotImplementedException();
        }

        public static IQueryable<TEntity> NaturalLanguageFullTextSearchWithQueryExpansion<TEntity>(this DbSet<TEntity> thisDbSet, string searchText) where TEntity : class
        {
            throw new NotImplementedException();
        }

        public static IQueryable<TEntity> BooleanFullTextContains<TEntity>(this DbSet<TEntity> thisDbSet, string searchText) where TEntity : class
        {
            throw new NotImplementedException();
        }
    }
}

//There are three types of full-text searches:
//A natural language search interprets the search string as a phrase in natural human language
//(a phrase in free text). There are no special operators, with the exception of double quote (") characters.
//Full-text searches are natural language searches if the IN NATURAL LANGUAGE MODE modifier is given or if no modifier is given. For more information, see Section 12.9.1, “Natural Language Full-Text Searches”.
//A boolean search interprets the search string using the rules of a special query language. The string contains the words to search for. It can also contain operators that specify requirements such that a word must be present or absent in matching rows, or that it should be weighted higher or lower than usual. Certain common words (stopwords) are omitted from the search index and do not match if present in the search string. The IN BOOLEAN MODE modifier specifies a boolean search. For more information, see Section 12.9.2, “Boolean Full-Text Searches”.
//A query expansion search is a modification of a natural language search. The search string is used to perform a natural language search. Then words from the most relevant rows returned by the search are added to the search string and the search is done again. The query returns the rows from the second search. The IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION or WITH QUERY EXPANSION modifier specifies a query expansion search. For more information, see Section 12.9.3, “Full-Text Searches with Query Expansion”.