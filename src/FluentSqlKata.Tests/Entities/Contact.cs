using System.ComponentModel.DataAnnotations.Schema;

namespace FluentSqlKata.Tests.Entities
{
    [Table("Contacts")]
    public class Contact : BaseEntity
    {
        protected Contact() : base()
        {

        }

        public Contact(string firstName, string lastName) : this()
        {
            SetName(firstName, lastName);
        }

        #region Properties

        public virtual string FirstName { get; protected set; }

        public virtual string LastName { get; protected set; }

        [Column("contact_customer_id")]
        public virtual int CustomerId { get; set; }

        #endregion Properties

        #region Public Methods

        public virtual void SetName(string firstName, string lastName)
        {
            if (string.IsNullOrWhiteSpace(firstName))
                throw new ArgumentNullException(nameof(firstName));

            FirstName = firstName;

            if (string.IsNullOrWhiteSpace(firstName))
                throw new ArgumentNullException(nameof(firstName));

            LastName = lastName;
        }

        #endregion Public Methods

    }
}
