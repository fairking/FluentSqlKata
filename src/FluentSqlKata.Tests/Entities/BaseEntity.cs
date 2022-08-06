using System.ComponentModel.DataAnnotations;

namespace FluentSqlKata.Tests.Entities
{
    public class BaseEntity
    {
        protected BaseEntity()
        {

        }

        [Key]
        public virtual int Id { get; protected set; }
    }
}
