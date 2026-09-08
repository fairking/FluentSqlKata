using System.ComponentModel.DataAnnotations.Schema;

namespace FluentSqlKata.Tests.Entities
{
    [Table("MappedEntities")]
    public class MappedEntity : BaseEntity
    {
        [Column("mapped_name")]
        public virtual string Name { get; set; }

        [SqlKata.Column("sql_kata_name")]
        public virtual string Raw { get; set; }

        [NotMapped]
        public virtual string NotMappedProp { get; set; }

        [SqlKata.Ignore]
        public virtual string IgnoredProp { get; set; }

        public virtual string Plain { get; set; }

        public virtual bool IsActive { get; set; }

        public virtual string this[int index] => "Item" + index;
    }
}
