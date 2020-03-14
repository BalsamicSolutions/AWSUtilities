//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
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
    ///  FULLTEXT functionality https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html
    ///There are three types of full-text searches:
    ///A natural language search interprets the search string as a phrase in natural human language
    ///(a phrase in free text). There are no special operators, with the exception of double quote (") characters.
    ///
    ///Full-text searches are natural language searches if the IN NATURAL LANGUAGE MODE modifier is given or if no modifier is given. 
    ///
    ///A query expansion search is a modification of a natural language search. The search string is used to perform a natural language search.
    ///Then words from the most relevant rows returned by the search are added to the search string and the search is done again. The query returns the rows from the second search. 
    ///
    ///A boolean search interprets the search string using the rules of a special query language. The string contains the words to search for. It can also contain operators that specify requirements such that a word must be present or absent in matching rows, or that it should be weighted higher or lower than usual. Certain common words (stopwords) are omitted from the search index and do not match if present in the search string. 
    /// </summary>
    public static class FullTextQueryableExtensions
    {

        /// <summary>
        /// A natural language search interprets the search string as a phrase in natural human language
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="thisDbSet">the DbSet from an enabled DbContext</param>
        /// <param name="searchText"></param>
        /// <returns></returns>
        public static IQueryable<TEntity> NaturalLanguageFullTextSearch<TEntity>(this DbSet<TEntity> thisDbSet, string searchText) where TEntity : class
        {
            Type entityType = typeof(TEntity);
            DbContext dbCtx = thisDbSet.GetDbContext<TEntity>();
            string tableName = dbCtx.GetActualTableName(entityType);
            string indexName = dbCtx.GetFullTextIndexName(entityType);
            string[] entityKeyNames = GetKeyColumnNamesAndTypes(entityType, out Type[] keyColumnTypes);
            if (entityKeyNames.Length == 0 || entityKeyNames.Length > 1)
            {
                throw new FullTextQueryException(typeof(TEntity).Name + " as (" + tableName + ") does not have a single unique Key.");
            }
            if (!HasFullTextAttribute(entityType))
            {
                throw new FullTextQueryException(typeof(TEntity).Name + " as (" + tableName + ") does not have any FullTextAttributes.");
            }
            //Ok its a good query so compose an execute a query to retrive the Key property values
            //that match the query and convert this to an Queryable expression that finds any
            //additional clauses in a List of those key values

            throw new NotImplementedException();
        }

        /// <summary>
        /// A natural language with query expansion search is a modification of a natural language search. The search string is used to perform a natural language search.
        /// Then words from the most relevant rows returned by the search are added to the search string and the search is done again. The query returns the rows from the second search. 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="thisDbSet">the DbSet from an enabled DbContext</param>
        /// <param name="searchText"></param>
        /// <returns></returns>
        public static IQueryable<TEntity> NaturalLanguageFullTextSearchWithQueryExpansion<TEntity>(this DbSet<TEntity> thisDbSet, string searchText) where TEntity : class
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// A boolean search interprets the search string using the rules of a special query language. The string contains the words to search for. 
        /// It can also contain operators that specify requirements such that a word must be present or absent in matching rows, or that it should be weighted higher or lower than usual. 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        / /// <param name="thisDbSet">the DbSet from an enabled DbContext</param>
        /// <param name="searchText"></param>
        /// <returns></returns>
        public static IQueryable<TEntity> BooleanFullTextContains<TEntity>(this DbSet<TEntity> thisDbSet, string searchText) where TEntity : class
        {
            throw new NotImplementedException();
        }



        /// <summary>
		/// Gets a column name suitable for naming constraints
		/// and indices, not actually validated 
		/// </summary>
		/// <param name="columnProp"></param>
		/// <returns></returns>
		public static string GetSimpleColumnName(System.Reflection.PropertyInfo columnProp)
        {
            Type columnAttribute = typeof(ColumnAttribute);
            ColumnAttribute columnNameAttribute = columnProp.GetCustomAttributes(columnAttribute, false).FirstOrDefault() as ColumnAttribute;
            if (null != columnNameAttribute && !columnNameAttribute.Name.IsNullOrEmpty())
            {
                return columnNameAttribute.Name;
            }
            else
            {
                return columnProp.Name;
            }
        }

        /// <summary>
		/// Gets the name a single key of an Entity 
		/// </summary>
		/// <param name="tableType"></param>
		/// <returns></returns>
		static string[] GetKeyColumnNamesAndTypes(Type tableType, out Type[] columnTypes)
        {
            List<string> returnValue = new List<string>();
            List<Type> keyTypes = new List<Type>();
            var keyColumns = tableType.GetProperties().Where(prop => Attribute.IsDefined(prop, typeof(KeyAttribute))).ToArray();
            foreach (PropertyInfo columnInfo in keyColumns)
            {
                returnValue.Add(GetSimpleColumnName(columnInfo));
                keyTypes.Add(columnInfo.PropertyType);
            }
            columnTypes = keyTypes.ToArray();
            return returnValue.ToArray();
        }


        /// <summary>
        /// checks for at least one full text attribute
        /// </summary>
        /// <param name="tableType"></param>
        /// <returns></returns>
        static bool HasFullTextAttribute(Type tableType)
        {
            bool returnValue = false;
            foreach (PropertyInfo pInfo in tableType.GetProperties())
            {
                FullTextAttribute ftAttribute = pInfo.GetCustomAttributes<FullTextAttribute>().FirstOrDefault() as FullTextAttribute;
                if (null != ftAttribute)
                {
                    returnValue = true;
                    break;
                }
            }

            return returnValue;
        }
    }
}

