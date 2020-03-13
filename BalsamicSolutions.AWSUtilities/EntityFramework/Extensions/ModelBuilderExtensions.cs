using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace BalsamicSolutions.AWSUtilities.EntityFramework.Extensions
{
    /// <summary>
    /// extensions to the model builder
    /// to activate our properties
    /// </summary>
    public static class ModelBuilderExtensions
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
  

    }
}
