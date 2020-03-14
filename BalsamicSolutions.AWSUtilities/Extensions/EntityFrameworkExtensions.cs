//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
using BalsamicSolutions.AWSUtilities.RDS;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace BalsamicSolutions.AWSUtilities.Extensions
{
    public static class EntityFrameworkExtensions
    {
        private class IndexParam
        {
            public string IndexName { get; }

            public bool IsUnique { get; }

            public string[] PropertyNames { get; }

            public IndexParam(IndexAttribute indexAttr, params PropertyInfo[] properties)
            {
                this.IndexName = indexAttr.Name;
                this.IsUnique = indexAttr.IsUnique;
                this.PropertyNames = properties.Select(prop => prop.Name).ToArray();
            }
        }

        /// <summary>
        /// converts the "index" tag to fluent additions, excluding owned types
        /// Credit to https://github.com/jsakamoto/EntityFrameworkCore.IndexAttribute/blob/master/EntityFrameworkCore.IndexAttribute/IndexAttribute.cs
        /// </summary>
        /// <param name="modelBuilder"></param>
        public static void BuildIndexesFromAnnotations(this ModelBuilder modelBuilder)
        {
            foreach (var entityType in modelBuilder.Model.GetEntityTypes().Where(ent => ent.ClrType.GetCustomAttribute<OwnedAttribute>() == null).ToList())
            {
                var items = entityType.ClrType
                    .GetProperties()
                    .SelectMany(prop =>
                        Attribute.GetCustomAttributes(prop, typeof(IndexAttribute))
                            .Cast<IndexAttribute>()
                            .Select(index => new { prop, index })
                    )
                    .ToArray();

                var indexParams = items
                    .Where(item => String.IsNullOrEmpty(item.index.Name))
                    .Select(item => new IndexParam(item.index, item.prop));

                var namedIndexParams = items
                    .Where(item => !String.IsNullOrEmpty(item.index.Name))
                    .GroupBy(item => item.index.Name)
                    .Select(g => new IndexParam(
                        g.First().index,
                        g.OrderBy(item => item.index.Order).Select(item => item.prop).ToArray())
                    );

                if (!entityType.IsQueryType)
                {
                    EntityTypeBuilder entity = modelBuilder.Entity(entityType.ClrType);
                    foreach (var indexParam in indexParams.Concat(namedIndexParams))
                    {
                        IndexBuilder indexBuilder = entity
                            .HasIndex(indexParam.PropertyNames)
                            .IsUnique(indexParam.IsUnique);
                        if (!String.IsNullOrEmpty(indexParam.IndexName))
                        {
                            indexBuilder.HasName(indexParam.IndexName);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets the full text index name of a table
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <param name="entityType"></param>
        /// <returns></returns>
        public static string GetFullTextIndexName(this DbContext dbCtx, Type entityType)
        {
            string tableName = dbCtx.GetActualTableName(entityType);
            string returnValue = "FT_" + tableName;
            FullTextAttribute ftAttribute = entityType.GetCustomAttributes<FullTextAttribute>().FirstOrDefault() as FullTextAttribute;
            if (null != ftAttribute && !ftAttribute.Name.IsNullOrWhiteSpace()) returnValue = ftAttribute.Name;
            return returnValue;
        }


        /// <summary>
        /// validates the MySQL Fulltext configuration for the entities in
        /// the provided dbcontext. Intended to be applied during a seed
        /// or migration. See dbContextBase for helper implementation
        /// of migration integration
        /// </summary>
        /// <param name="dbCtx"></param>
        public static void EnsureFullTextIndices(this DbContext dbCtx)
        {
            //collect all the columns that are marked for includsion
            string databaseName = dbCtx.DatabaseName();
            Dictionary<string, string> dbDescription = DescribeAllFullTextIndices(dbCtx, databaseName);
            Dictionary<string, string> modelDescription = DescribeAllFullTextIndices(dbCtx);
            if (!FullTextDescriptionsAreTheSame(dbDescription, modelDescription))
            {
                CreateFullTextIndices(dbCtx);
            }
        }

        /// <summary>
        /// creates full text indices
        /// </summary>
        /// <param name="dbCtx"></param>
        public static void CreateFullTextIndices(this DbContext dbCtx)
        {
            string databaseName = dbCtx.DatabaseName();
            bool lowerCaseTableNames = dbCtx.MySqlLowerCaseTableNames();
            Dictionary<string, Dictionary<string, FullTextAttribute>> fullTextMap = new Dictionary<string, Dictionary<string, FullTextAttribute>>(StringComparer.OrdinalIgnoreCase);
            foreach (IEntityType entityType in dbCtx.Model.GetEntityTypes().Where(ent => ent.ClrType.GetCustomAttribute<OwnedAttribute>() == null).ToList())
            {
                string tableName = lowerCaseTableNames ? entityType.Relational().TableName.ToLowerInvariant() : entityType.Relational().TableName;
                string indexName = dbCtx.GetFullTextIndexName(entityType.ClrType);
                tableName = tableName + ":" + indexName;
                foreach (PropertyInfo pInfo in entityType.ClrType.GetProperties())
                {
                    FullTextAttribute ftAttribute = pInfo.GetCustomAttributes<FullTextAttribute>().FirstOrDefault() as FullTextAttribute;
                    if (null != ftAttribute)
                    {
                        string columnName = pInfo.Name;
                        ColumnAttribute columnNameAttribute = pInfo.GetCustomAttributes<ColumnAttribute>().FirstOrDefault() as ColumnAttribute;
                        if (null != columnNameAttribute && !columnNameAttribute.Name.IsNullOrEmpty()) columnName = columnNameAttribute.Name;
                        Dictionary<string, FullTextAttribute> tableMap = null;
                        if (!fullTextMap.TryGetValue(tableName, out tableMap))
                        {
                            tableMap = new Dictionary<string, FullTextAttribute>(StringComparer.OrdinalIgnoreCase);
                            fullTextMap[tableName] = tableMap;
                        }
                        tableMap[columnName] = ftAttribute;
                    }
                }

            }

            DropFullTextIndices(dbCtx, databaseName);
            if (fullTextMap.Count > 0)
            {
                List<string> sqlCommands = GenerateCreateFullTextIndexCommands(dbCtx, fullTextMap);
                foreach (string sqlCommand in sqlCommands)
                {
                    dbCtx.ExecuteSqlCommand(sqlCommand);
                }
            }
        }

        /// <summary>
        /// drops all full text indices in a specific database
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <param name="datbaseName"></param>
        private static void DropFullTextIndices(DbContext dbCtx, string datbaseName)
        {
            //first collect all full text indices
            List<string> sqlCommands = GenerateDropCommandsForAllFullTextIndices(dbCtx, datbaseName);
            foreach (string sqlCommand in sqlCommands)
            {
                //now kill them
                dbCtx.ExecuteSqlCommand(sqlCommand);
            }
        }

        /// <summary>
        /// compares DB and Model descriptions
        /// NOTE: does not consider column order
        /// </summary>
        /// <param name="desOne"></param>
        /// <param name=""></param>
        /// <param name=""></param>
        /// <returns></returns>
        static bool FullTextDescriptionsAreTheSame(Dictionary<string, string> desOne, Dictionary<string, string> desTwo)
        {
            bool returnValue = false;
            if (desOne.Count == desTwo.Count)
            {
                returnValue = true;
                foreach (string keyName in desOne.Keys.ToArray())
                {
                    string colNamesOne = desOne[keyName];
                    if (!desTwo.TryGetValue(keyName, out string colNamesTwo))
                    {
                        returnValue = false;
                    }
                    if (returnValue)
                    {
                        HashSet<string> namesOne = new HashSet<string>(colNamesOne.Split(','),StringComparer.OrdinalIgnoreCase);
                        HashSet<string> namesTwo = new HashSet<string>(colNamesTwo.Split(','),StringComparer.OrdinalIgnoreCase);
                        returnValue = namesOne.Count == namesTwo.Count;
                        if (returnValue)
                        {
                            foreach (string nameOne in namesOne)
                            {
                                if (!namesTwo.Contains(nameOne))
                                {
                                    returnValue = false;
                                }
                                if (!returnValue) break;
                            }
                        }
                    }
                    if (!returnValue) break;
                }
            }
            return returnValue;
        }

        /// <summary>
        /// creates a description of all table/full text indices
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <param name="datbaseName"></param>
        /// <returns></returns>
        private static Dictionary<string, string> DescribeAllFullTextIndices(DbContext dbCtx)
        {
            bool lowerCaseTableNames = dbCtx.MySqlLowerCaseTableNames();
            Dictionary<string, string> returnValue = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (IEntityType entityType in dbCtx.Model.GetEntityTypes().Where(ent => ent.ClrType.GetCustomAttribute<OwnedAttribute>() == null).ToList())
            {
                string tableName = lowerCaseTableNames ? entityType.Relational().TableName.ToLowerInvariant() : entityType.Relational().TableName;
                string indexName = dbCtx.GetFullTextIndexName(entityType.ClrType);
                string keyName = tableName + ":" + indexName;
                List<string> columnNames = new List<string>();
                foreach (PropertyInfo pInfo in entityType.ClrType.GetProperties())
                {
                    FullTextAttribute ftAttribute = pInfo.GetCustomAttributes<FullTextAttribute>().FirstOrDefault() as FullTextAttribute;
                    if (null != ftAttribute)
                    {
                        string columnName = pInfo.Name;
                        ColumnAttribute columnNameAttribute = pInfo.GetCustomAttributes<ColumnAttribute>().FirstOrDefault() as ColumnAttribute;
                        if (null != columnNameAttribute && !columnNameAttribute.Name.IsNullOrEmpty()) columnName = columnNameAttribute.Name;
                        columnNames.Add(columnName);
                    }
                }
                if (columnNames.Count > 0)
                {
                    returnValue[keyName] = string.Join(",", columnNames);
                }

            }
            return returnValue;
        }


        /// <summary>
        /// creates a description of all table/full text indices from the database
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <param name="datbaseName"></param>
        /// <returns></returns>
        private static Dictionary<string, string> DescribeAllFullTextIndices(DbContext dbCtx, string datbaseName)
        {
            Dictionary<string, string> returnValue = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            string sqlQuery = $"SELECT index_name,table_name, group_concat(distinct column_name) FROM information_Schema.STATISTICS WHERE table_schema = '{datbaseName}' AND index_type = 'FULLTEXT';";
            using (var dataReader = dbCtx.Database.ExecuteSqlQuery(sqlQuery))
            {
                System.Data.Common.DbDataReader dbDataReader = dataReader.DbDataReader;
                if (null != dbDataReader)
                {
                    while (dbDataReader.Read())
                    {
                        object indexName = dbDataReader[0];
                        object tableName = dbDataReader[1];
                        object columnNames = dbDataReader[2];
                        if (null != indexName && null != tableName)
                        {
                            string keyName = $"{tableName}:{indexName}";
                            returnValue[keyName] = columnNames.ToString();
                        }
                    }
                }
            }
            return returnValue;
        }

        /// <summary>
        /// lists all the full text indices in a database
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <param name="datbaseName"></param>
        /// <returns></returns>
        private static List<string> GenerateDropCommandsForAllFullTextIndices(DbContext dbCtx, string datbaseName)
        {
            List<string> returnValue = new List<string>();
            string sqlQuery = $"SELECT index_name,table_name FROM information_Schema.STATISTICS WHERE table_schema = '{datbaseName}' AND index_type = 'FULLTEXT';";
            using (var dataReader = dbCtx.Database.ExecuteSqlQuery(sqlQuery))
            {
                System.Data.Common.DbDataReader dbDataReader = dataReader.DbDataReader;
                if (null != dbDataReader)
                {
                    while (dbDataReader.Read())
                    {
                        object indexName = dbDataReader[0];
                        object tableName = dbDataReader[1];
                        if (null != indexName && null != tableName)
                        {
                            string alterStatement = $"ALTER TABLE {tableName} DROP INDEX {indexName};";
                            returnValue.Add(alterStatement);
                        }
                    }
                }
            }
            return returnValue;
        }

        /// <summary>
        /// creates the full text commands
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <param name="fullTextMap"></param>
        /// <returns></returns>
        private static List<string> GenerateCreateFullTextIndexCommands(DbContext dbCtx, Dictionary<string, Dictionary<string, FullTextAttribute>> fullTextMap)
        {
            List<string> returnValue = new List<string>();
            foreach (string keyName in fullTextMap.Keys.ToArray())
            {
                string[] keyParts = keyName.Split(':');
                string tableName = keyParts[0];
                string indexName = keyParts[1];
                Dictionary<string, FullTextAttribute> ftMap = fullTextMap[keyName];
                //We need to sort the map just in case one or more of the attributes is ordered
                List<string> propertyNames = SortByColumnOrder(ftMap);
                string columnNames = string.Join(",", propertyNames);
                string sqlCommand = $"CREATE FULLTEXT INDEX {indexName} ON {tableName}({columnNames});";
                returnValue.Add(sqlCommand);
            }
            return returnValue;
        }

        /// <summary>
        /// sorts our definition into an array ordered by column id
        /// </summary>
        /// <param name="ftMap"></param>
        /// <returns></returns>
        private static List<string> SortByColumnOrder(Dictionary<string, FullTextAttribute> ftMap)
        {
            List<string> returnValue = new List<string>();
            int maxPos = ftMap.Count();
            for (int colPos = 0; colPos < maxPos; colPos++)
            {
                foreach (string propName in ftMap.Keys.ToArray())
                {
                    if (ftMap[propName].Order == colPos)
                    {
                        returnValue.Add(propName);
                    }
                }
            }
            //now get the unspecified remainder
            foreach (string propName in ftMap.Keys.ToArray())
            {
                if (ftMap[propName].Order == -1)
                {
                    returnValue.Add(propName);
                }
            }
            return returnValue;
        }

        /// <summary>
        /// get the dbcontext from a DbSet
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbSet"></param>
        /// <returns></returns>
        public static DbContext GetDbContext<TEntity>(this DbSet<TEntity> dbSet) where TEntity : class
        {
            IInfrastructure<IServiceProvider> infrastructure = dbSet as IInfrastructure<IServiceProvider>;
            ICurrentDbContext currentDbContext = infrastructure.Instance.GetService(typeof(ICurrentDbContext)) as ICurrentDbContext;
            return currentDbContext.Context;
        }

        /// <summary>
        /// Enable MySQL using the Aurora ExecutionStrategy
        /// Instead of UseMySql in your OnConfiguring call
        /// optionsBuilder.UseAuroraExecutionStrategy(this,"connection string info")
        /// </summary>
        /// <param name="optionsBuilder"></param>
        /// <param name="dbCtx"></param>
        /// <param name="connectionString">Aurora/MySql compatiable connection string</param>
        /// <returns></returns>
        public static DbContextOptionsBuilder UseAuroraExecutionStrategy(this DbContextOptionsBuilder optionsBuilder, DbContext dbCtx, string connectionString)
        {
            optionsBuilder.UseMySQL(connectionString, optAct => optAct.ExecutionStrategy(exStg => new AuroraExecutionStrategy(dbCtx)));
            return optionsBuilder;
        }

        /// <summary>
        /// ExecuteSqlCommand
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static int ExecuteSqlCommand(this DbContext dbCtx, string sqlText)
        {
            return dbCtx.Database.ExecuteSqlCommand(sqlText, Array.Empty<object>());
        }

        /// <summary>
        /// ExecuteSqlCommand
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static int ExecuteSqlCommand(this DbContext dbCtx, string sqlText, params object[] parameters)
        {
            return dbCtx.Database.ExecuteSqlCommand(sqlText, parameters);
        }

        /// <summary>
        /// execute sql and return data reader
        /// </summary>
        /// <param name="databaseFacade"></param>
        /// <param name="sql"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static async Task<RelationalDataReader> ExecuteSqlQueryAsync(this DatabaseFacade databaseFacade,
                                                            string sql,
                                                            CancellationToken cancellationToken = default(CancellationToken),
                                                            params object[] parameters)
        {
            IConcurrencyDetector concurrencyDetector = databaseFacade.GetService<IConcurrencyDetector>();

            using (concurrencyDetector.EnterCriticalSection())
            {
                RawSqlCommand rawSqlCommand = databaseFacade
                    .GetService<IRawSqlCommandBuilder>()
                    .Build(sql, parameters);

                return await rawSqlCommand
                    .RelationalCommand
                    .ExecuteReaderAsync(
                        databaseFacade.GetService<IRelationalConnection>(),
                        parameterValues: rawSqlCommand.ParameterValues,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// execute sql and return data reader
        /// </summary>
        /// <param name="databaseFacade"></param>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static RelationalDataReader ExecuteSqlQuery(this DatabaseFacade databaseFacade, string sql, params object[] parameters)
        {
            IConcurrencyDetector concurrencyDetector = databaseFacade.GetService<IConcurrencyDetector>();

            using (concurrencyDetector.EnterCriticalSection())
            {
                RawSqlCommand rawSqlCommand = databaseFacade
                    .GetService<IRawSqlCommandBuilder>()
                    .Build(sql, parameters);

                return rawSqlCommand
                    .RelationalCommand
                    .ExecuteReader(
                        databaseFacade.GetService<IRelationalConnection>(),
                        parameterValues: rawSqlCommand.ParameterValues);
            }
        }
    }
}