using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
using BalsamicSolutions.AWSUtilities.RDS;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using MySql.Data.EntityFrameworkCore.Infraestructure;

namespace BalsamicSolutions.AWSUtilities.EntityFramework.Extensions
{
    /// <summary>
    /// options builder extensions
    /// </summary>
    public static class DbContextOptionsBuilderExtensions
    {

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
    }
}
