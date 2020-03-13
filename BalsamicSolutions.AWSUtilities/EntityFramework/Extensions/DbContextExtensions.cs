using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
using BalsamicSolutions.AWSUtilities.RDS;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using MySql.Data.EntityFrameworkCore.Infraestructure;

namespace BalsamicSolutions.AWSUtilities.EntityFramework.Extensions
{
    /// <summary>
    /// DbContext Extensions
    /// </summary>
    public static class DbContextExtensions
    {

        /// <summary>
        /// validates the MySQL Fulltext configuration for the entities in
        /// the provided dbcontext. Intended to be applied during a seed
        /// or migration. See dbContextBase for helper implementation
        /// of migration integration
        /// </summary>
        /// <param name="dbCtx"></param>
        public static void ValidateFullTextIndices(this DbContext dbCtx)
        {
            //collect all the columns that are marked for includsion
            List<string> tableNames = new List<string>();
            string databaseName = "";//dbCtx.Database;

            bool lowerCaseTableNames = dbCtx.MySqlLowerCaseTableNames();
            Dictionary<string, Dictionary<string,FullTextAttribute>> fullTextMap = new  Dictionary<string, Dictionary<string,FullTextAttribute>>(StringComparer.OrdinalIgnoreCase);
            foreach (IEntityType entityType in dbCtx.Model.GetEntityTypes().Where(ent => ent.ClrType.GetCustomAttribute<OwnedAttribute>() == null).ToList())
            {
                string tableName = lowerCaseTableNames ? entityType.Relational().TableName.ToLowerInvariant() : entityType.Relational().TableName;
                tableNames.Add(tableName);
                foreach (PropertyInfo pInfo in entityType.ClrType.GetProperties())
                {
                    FullTextAttribute ftAttribute = pInfo.GetCustomAttributes<FullTextAttribute>().FirstOrDefault() as FullTextAttribute;
                    if (null != ftAttribute)
                    {
                        string columnName = pInfo.Name;
                        ColumnAttribute columnNameAttribute = pInfo.GetCustomAttributes<ColumnAttribute>().FirstOrDefault() as ColumnAttribute;
                        if (null != columnNameAttribute && !columnNameAttribute.Name.IsNullOrEmpty()) columnName = columnNameAttribute.Name;
                         Dictionary<string,FullTextAttribute> tableMap = null;
                        if(!fullTextMap.TryGetValue(tableName, out tableMap))
                        {
                            tableMap = new Dictionary<string, FullTextAttribute>(StringComparer.OrdinalIgnoreCase);
                            fullTextMap[tableName] = tableMap;
                        }
                        tableMap[columnName]=ftAttribute;
                    }
                }
                DropFullTextIndices(dbCtx,databaseName);

                //collect the fulltext index names to add
                //query the db for existing fulltext indices to drop
                //itterate over the names fulltext names
            }
        }

        /// <summary>
        /// drops all full text indices in a specific database
        /// </summary>
        /// <param name="dbCtx"></param>
        /// <param name="datbaseName"></param>
        static void DropFullTextIndices(DbContext dbCtx, string datbaseName)
        {
            //TODO
        }

        static List<string> GenerateFullTextCommands(DbContext dbCtx,Dictionary<string, Dictionary<string,FullTextAttribute>> fullTextMap)
        {
            List<string> returnValue = new List<string>();

             //TODO

            return returnValue;
        }
    }
}
