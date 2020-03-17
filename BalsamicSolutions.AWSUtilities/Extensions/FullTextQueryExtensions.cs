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
        /// <param name="searchText">search command</param>
        ///<param name="orderByScore">order the results by score</param>
        /// <returns></returns>
        public static IQueryable<TEntity> NaturalLanguageFullTextSearch<TEntity>(this DbSet<TEntity> thisDbSet, string searchText, bool orderByScore = false) where TEntity : class
        {
            Type entityType = typeof(TEntity);
            string[] columnNames = GetFullTextColumnNames(entityType);
            return thisDbSet.FullTextSearchInternal(searchText, columnNames, false, false, orderByScore);
        }

        /// <summary>
        /// A natural language with query expansion search is a modification of a natural language search. The search string is used to perform a natural language search.
        /// Then words from the most relevant rows returned by the search are added to the search string and the search is done again. The query returns the rows from the second search.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="thisDbSet">the DbSet from an enabled DbContext</param>
        /// <param name="searchText">search command</param>
        ///<param name="orderByScore">order the results by score</param>
        /// <returns></returns>
        public static IQueryable<TEntity> NaturalLanguageFullTextSearchWithQueryExpansion<TEntity>(this DbSet<TEntity> thisDbSet, string searchText, bool orderByScore = false) where TEntity : class
        {
            Type entityType = typeof(TEntity);
            string[] columnNames = GetFullTextColumnNames(entityType);
            return thisDbSet.FullTextSearchInternal(searchText, columnNames, false, true, orderByScore);
        }

        /// <summary>
        /// A boolean search interprets the search string using the rules of a special query language. The string contains the words to search for.
        /// It can also contain operators that specify requirements such that a word must be present or absent in matching rows, or that it should be weighted higher or lower than usual.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="thisDbSet">the DbSet from an enabled DbContext</param>
        /// <param name="searchText">search command</param>
        ///<param name="orderByScore">order the results by score</param>
        /// <returns></returns>
        public static IQueryable<TEntity> BooleanFullTextContains<TEntity>(this DbSet<TEntity> thisDbSet, string searchText, bool orderByScore = false) where TEntity : class
        {
            Type entityType = typeof(TEntity);
            string[] columnNames = GetFullTextColumnNames(entityType);
            return thisDbSet.FullTextSearchInternal(searchText, columnNames, true, false, orderByScore);
        }

        /// <summary>
        /// A natural language search interprets the search string as a phrase in natural human language
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="thisDbSet">the DbSet from an enabled DbContext</param>
        /// <param name="searchText">search command</param>
        ///<param name="orderByScore">order the results by score</param>
        /// <returns></returns>
        private static IQueryable<TEntity> FullTextSearchInternal<TEntity>(this DbSet<TEntity> thisDbSet, string searchText, string[] columnNames, bool booleanMode, bool queryExpansion, bool orderByScore) where TEntity : class
        {
            Type entityType = typeof(TEntity);
            IQueryable<TEntity> queryableDbSet = thisDbSet as IQueryable<TEntity>;
            DbContext dbCtx = thisDbSet.GetDbContext<TEntity>();
            string tableName = dbCtx.GetActualTableName(entityType);

            string[] entityKeyNames = GetKeyColumnNamesAndTypes(entityType, out Type[] keyColumnTypes);
            if (entityKeyNames.Length == 0 || entityKeyNames.Length > 1)
            {
                throw new FullTextQueryException(typeof(TEntity).Name + " as (" + tableName + ") does not have a single unique Key.");
            }

            if (null == columnNames || columnNames.Length == 0)
            {
                throw new FullTextQueryException(typeof(TEntity).Name + " as (" + tableName + ") does not have any FullTextAttributes.");
            }
            //Ok its a good query so compose an execute a query to retrive the Key property values
            //that match the query and convert this to an Queryable expression that finds any
            //additional clauses in a List of those key values
            //Now get the untyped collection of results from the query
            ArrayList untypedResults = ExecuteFullTextSearch(dbCtx, tableName, entityKeyNames[0], searchText, columnNames, booleanMode, queryExpansion, orderByScore);

            //Ok get the matching TEntity parameter
            ParameterExpression parameterExpression = Expression.Parameter(queryableDbSet.ElementType, "p");
            //And now project the propery name on TEntity
            MemberExpression memberPropertyAccess = MemberExpression.Property(parameterExpression, entityKeyNames[0]);
            // Convert the results to a typed collection so that the comparisonCondition can be correctly constructed , if we dont
            //  we will get errors like this (assuming a key of type guid and an untyped array)
            //  " generic type 'System.Guid' cannot be used for parameter of type 'System.Object' of method 'Boolean Contains(System.Object)'"
            Expression comparisonCondition = null;
            LambdaExpression lambdaExpression = null;
            MethodInfo containsMethod = null;

            if (keyColumnTypes[0] == typeof(Guid))
            {
                HashSet<Guid> containedIn = ConvertUntypedCollectionToTypedHashSet<Guid>(untypedResults);
                containsMethod = typeof(HashSet<Guid>).GetMethod("Contains", new Type[] { typeof(Guid) });
                comparisonCondition = Expression.Call(Expression.Constant(containedIn), containsMethod, memberPropertyAccess);
                lambdaExpression = Expression.Lambda(comparisonCondition, parameterExpression);
            }
            else if (keyColumnTypes[0] == typeof(string))
            {
                HashSet<string> containedIn = ConvertUntypedCollectionToTypedHashSet<string>(untypedResults);
                containsMethod = typeof(HashSet<string>).GetMethod("Contains", new Type[] { typeof(string) });
                comparisonCondition = Expression.Call(Expression.Constant(containedIn), containsMethod, memberPropertyAccess);
                lambdaExpression = Expression.Lambda(comparisonCondition, parameterExpression);
            }
            else if (keyColumnTypes[0] == typeof(int))
            {
                HashSet<int> containedIn = ConvertUntypedCollectionToTypedHashSet<int>(untypedResults);
                containsMethod = typeof(HashSet<int>).GetMethod("Contains", new Type[] { typeof(int) });
                comparisonCondition = Expression.Call(Expression.Constant(containedIn), containsMethod, memberPropertyAccess);
                lambdaExpression = Expression.Lambda(comparisonCondition, parameterExpression);
            }
            else if (keyColumnTypes[0] == typeof(Int64))
            {
                HashSet<Int64> containedIn = ConvertUntypedCollectionToTypedHashSet<Int64>(untypedResults);
                containsMethod = typeof(HashSet<Int64>).GetMethod("Contains", new Type[] { typeof(Int64) });
                comparisonCondition = Expression.Call(Expression.Constant(containedIn), containsMethod, memberPropertyAccess);
                lambdaExpression = Expression.Lambda(comparisonCondition, parameterExpression);
            }
            else if (keyColumnTypes[0] == typeof(Int16))
            {
                HashSet<Int16> containedIn = ConvertUntypedCollectionToTypedHashSet<Int16>(untypedResults);
                containsMethod = typeof(HashSet<Int16>).GetMethod("Contains", new Type[] { typeof(Int16) });
                comparisonCondition = Expression.Call(Expression.Constant(containedIn), containsMethod, memberPropertyAccess);
                lambdaExpression = Expression.Lambda(comparisonCondition, parameterExpression);
            }
            else
            {
                throw new FullTextQueryException("unsupported primary key type " + keyColumnTypes[0].Name + " for " + tableName);
            }
          
            MethodCallExpression conditionResult = Expression.Call(typeof(Queryable), "Where", new[] { queryableDbSet.ElementType }, queryableDbSet.Expression, lambdaExpression);
            //TODO handle the OrderByScore
            return queryableDbSet.Provider.CreateQuery<TEntity>(conditionResult);
        }

        /// <summary>
		/// Gets a column name suitable for naming constraints
		/// and indices, not actually validated
		/// </summary>
		/// <param name="columnProp"></param>
		/// <returns></returns>
		private static string GetSimpleColumnName(PropertyInfo columnProp)
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
		private static string[] GetKeyColumnNamesAndTypes(Type tableType, out Type[] columnTypes)
        {
            List<string> returnValue = new List<string>();
            List<Type> keyTypes = new List<Type>();
            PropertyInfo[] keyColumns = tableType.GetProperties().Where(prop => Attribute.IsDefined(prop, typeof(KeyAttribute))).ToArray();
            foreach (PropertyInfo columnInfo in keyColumns)
            {
                returnValue.Add(GetSimpleColumnName(columnInfo));
                keyTypes.Add(columnInfo.PropertyType);
            }
            columnTypes = keyTypes.ToArray();
            return returnValue.ToArray();
        }

        /// <summary>
        /// Gets the name of any properties with the FullText attribute
        /// </summary>
        /// <param name="tableType"></param>
        /// <returns></returns>
        private static string[] GetFullTextColumnNames(Type tableType)
        {
            List<string> returnValue = new List<string>();
            List<Type> keyTypes = new List<Type>();
            PropertyInfo[] keyColumns = tableType.GetProperties().Where(prop => Attribute.IsDefined(prop, typeof(FullTextAttribute))).ToArray();
            foreach (PropertyInfo columnInfo in keyColumns)
            {
                returnValue.Add(GetSimpleColumnName(columnInfo));
                keyTypes.Add(columnInfo.PropertyType);
            }

            return returnValue.ToArray();
        }

        /// <summary>
		/// convert generic object collection to a typed collection
		/// this only works with primitives or things with explicit
		/// conversion operators
		/// </summary>
		/// <typeparam name="T">type of objects in the untypedList</typeparam>
		/// <param name="untypedList">collection of untyped objects</param>
		/// <returns>Typed collection</returns>
		private static HashSet<T> ConvertUntypedCollectionToTypedHashSet<T>(ArrayList untypedList)
        {
            HashSet<T> returnValue = new HashSet<T>();
            foreach (object untypedObject in untypedList)
            {
                returnValue.Add((T)untypedObject);
            }
            return returnValue;
        }

        /// <summary>
        /// Process the search
        /// </summary>
        /// <param name="dbCtx">EF dbContext</param>
        /// <param name="tableName">the name of the table </param>
        /// <param name="primaryKeyColumnName">the primary key of the table</param>
        /// <param name="searchText">the FULLTEXT formated query</param>
        /// <param name="columnNames"></param>
        ///<param name="booleanMode">if true process as a boolean search, otherwise its a Natural Language searc</param>
        ///<param name="queryExpansion">If its Natural Language, also add the query expansion flag</param>
        ///<param name="orderByScore">order the results by score</param>
        /// <returns></returns>
        private static ArrayList ExecuteFullTextSearch(DbContext dbCtx, string tableName, string primaryKeyColumnName, string searchText, string[] columnNames, bool booleanMode, bool queryExpansion, bool orderByScore)
        {
            ArrayList returnValue = new ArrayList();
            string columnNameText = string.Join(",", columnNames); ;
            string commandMod = "IN NATURAL LANGUAGE MODE";
            if (booleanMode)
            {
                commandMod = "IN BOOLEAN MODE";
            }
            if (queryExpansion)
            {
                commandMod = "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION";
            }
            searchText = searchText.Trim(new char[] { '\'', ' ' });
            string matchText = string.Format("MATCH ({0}) AGAINST ('{1}' {2})", columnNameText, searchText, commandMod);
            string sqlQuery = null;
            if (orderByScore)
            {
                sqlQuery = string.Format("SELECT {0}, {2} AS score FROM {1} WHERE {2} ORDER BY score DESC;", primaryKeyColumnName, tableName, matchText);
            }
            else
            {
                sqlQuery = string.Format("SELECT {0} FROM {1} WHERE {2};", primaryKeyColumnName, tableName, matchText);
            }
            using (var dataReader = dbCtx.Database.ExecuteSqlQuery(sqlQuery))
            {
                System.Data.Common.DbDataReader dbDataReader = dataReader.DbDataReader;
                if (null != dbDataReader)
                {
                    while (dbDataReader.Read())
                    {
                        returnValue.Add(dbDataReader[0]);
                    }
                }
            }
            return returnValue;
        }
    }
}