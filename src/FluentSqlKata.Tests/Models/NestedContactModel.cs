namespace FluentSqlKata.Tests.Models
{
	public class NestedContactModel
	{
		public string FirstName { get; set; }

		public string LastName { get; set; }

		public Initials Initials { get; set; }
	}

	public class Initials
	{
		public string FirstName { get; set; }

		public string LastName { get; set; }
	}
}
