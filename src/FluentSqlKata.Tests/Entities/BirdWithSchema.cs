using System.ComponentModel.DataAnnotations.Schema;

namespace FluentSqlKata.Tests.Entities
{
	[Table("Birds", Schema = "OtherDatabase.dbo")]
	public class BirdWithSchema : BaseEntity
	{
		protected BirdWithSchema() { }

		public BirdWithSchema(string name)
		{
			SetName(name);
		}

		#region Properties

		public virtual string Name { get; protected set; }

		#endregion Properties

		#region Methods

		public virtual void SetName(string name)
		{
			if (string.IsNullOrWhiteSpace(name))
				throw new ArgumentNullException(nameof(name));

			Name = name;
		}

		#endregion Methods
	}
}
