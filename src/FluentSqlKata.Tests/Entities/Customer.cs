namespace FluentSqlKata.Tests.Entities
{
    public class Customer : BaseEntity
    {
        protected Customer() : base()
        {

        }

        public Customer(string name) : this()
        {
            SetName(name);
        }

        #region Properties

        public virtual string Name { get; protected set; }

        #endregion Properties

        #region Public Methods

        public virtual void SetName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            Name = name;
        }

        #endregion Public Methods
    }
}
