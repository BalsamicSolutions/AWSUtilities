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
using BalsamicSolutions.AWSUtilities.EntityFramework;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
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
            string returnValue= mySqlCSBuilder.Database;
            if(dbCtx.MySqlLowerCaseTableNames())
            {
                returnValue=returnValue.ToLowerInvariant();
            }
            return returnValue;
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
                if (null != dbDataReader && dbDataReader.Read())
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
        /// cache for expensive lookups
        /// </summary>
        private static Dictionary<string, bool> _LowerCaseCache = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// checks to see if MySQL is running in "normalized lower case table names" mode
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <returns></returns>
        public static bool MySqlLowerCaseTableNames(this DbContext dbCtx)
        {
            string connectionString = dbCtx.GetConnectionString();
            bool returnValue = false;
            lock (_LowerCaseCache)
            {
                if (_LowerCaseCache.TryGetValue(connectionString, out returnValue))
                {
                    return returnValue;
                }
            }
            //check the MySql settings foro the table names
            using (DbDataReaderWrapper relReader = dbCtx.Database.ExecuteSqlQuery("SHOW VARIABLES LIKE \"lower_case_table_names\";", new object[] { }))
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
            lock (_LowerCaseCache)
            {
                _LowerCaseCache[connectionString] = returnValue;
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
            string tableName = entityType.GetTableName();
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
                string tableName = entityType.GetTableName();
                if (lowerCaseTableNames) tableName = tableName.ToLowerInvariant();
                returnvalue.Add(tableName);
            }
            return returnvalue.ToArray();
        }
    }
}