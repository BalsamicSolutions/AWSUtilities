//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using MySql.Data.MySqlClient;

namespace BalsamicSolutions.AWSUtilities.Extensions
{
    /// <summary>
    /// some Aurora/MySQL specific extensions
    /// </summary>
    public static class MySQLExtensions
    {

        /// <summary>
        /// gets the connection string
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static string GetConnectionString(this DbContext dbCtx)
        {
            return dbCtx.Database.GetDbConnection().ConnectionString;
        }

        /// <summary>
        /// gets the default database/MySQL schema name for the connection
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static string DatabaseName(this DbContext dbCtx)
        {
            string connectionString = dbCtx.GetConnectionString();
            MySqlConnectionStringBuilder mySqlCSBuilder = new MySqlConnectionStringBuilder(connectionString);
            return mySqlCSBuilder.Database;
        }

        /// <summary>
        /// gets the UTC time at the server
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static DateTime UtcTimeAtServer(this DbContext dbCtx)
        {
            DateTime returnValue = DateTime.MinValue;
            using (var dataReader = dbCtx.Database.ExecuteSqlQuery("SELECT UTC_TIMESTAMP;"))
            {
                DbDataReader dbDataReader = dataReader.DbDataReader;
                if (null != dbDataReader && dataReader.Read())
                {
                    object queryResponse = dbDataReader[0];
                    if (null != queryResponse)
                    {
                        DateTime unspecifiedTime = (DateTime)queryResponse;
                        if (unspecifiedTime.Kind == DateTimeKind.Unspecified)
                        {
                            //this is what we see coming back
                            returnValue = new DateTime(unspecifiedTime.Ticks, DateTimeKind.Utc);
                        }
                        else if (unspecifiedTime.Kind == DateTimeKind.Utc)
                        {
                            //this is what its supposed to be
                            returnValue = unspecifiedTime;
                        }
                        else
                        {
                            //dont know if we will get this
                            returnValue = unspecifiedTime.ToUniversalTime();
                        }
                    }
                }
            }
            return returnValue;
        }

        /// <summary>
        /// checks to see if MySQL is running in "normalized lower case table names" mode
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static bool MySqlLowerCaseTableNames(this DbContext dbCtx)
        {
            bool returnValue = false;
            //check the MySql settings foro the table names
            using (RelationalDataReader relReader = dbCtx.Database.ExecuteSqlQuery("SHOW VARIABLES LIKE \"lower_case_table_names\";", new object[] { }))
            {
                var dbReader = relReader.DbDataReader;
                if (dbReader.Read())
                {
                    int columnNum = dbReader.GetOrdinal("Value");
                    int configSetting = dbReader.GetInt32(columnNum);
                    if (configSetting > 0)
                    {
                        returnValue = true;
                    }
                }
            }
            return returnValue;
        }

        /// <summary>
        /// gets the server side table name of the entity
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static string GetActualTableName(this DbContext dbCtx, Type modelType)
        {
            bool lowerCaseTableNames = dbCtx.MySqlLowerCaseTableNames();
            IEntityType entityType = dbCtx.Model.FindEntityType(modelType);
            string tableName = entityType.Relational().TableName;
            if (lowerCaseTableNames) tableName = tableName.ToLowerInvariant();
            return tableName;
        }

        /// <summary>
        /// returns a list of table names in the context
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static string[] GetTableNames(this DbContext dbCtx)
        {
            bool lowerCaseTableNames = dbCtx.MySqlLowerCaseTableNames();

            HashSet<string> returnvalue = new HashSet<string>();
            //get all the entities that are not flagged as "owned"
            foreach (IEntityType entityType in dbCtx.Model.GetEntityTypes().Where(ent => ent.ClrType.GetCustomAttribute<OwnedAttribute>() == null).ToList())
            {
                string tableName = entityType.Relational().TableName;
                if (lowerCaseTableNames) tableName = tableName.ToLowerInvariant();
                returnvalue.Add(tableName);
            }
            return returnvalue.ToArray();
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