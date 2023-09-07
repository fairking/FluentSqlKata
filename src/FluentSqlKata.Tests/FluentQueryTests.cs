using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Models;
using SqlKata.Compilers;
using System.Linq.Expressions;

namespace FluentSqlKata.Tests
{
    public class FluentQueryTests
    {
        [Fact]
        public void T01_Basic()
        {
            Customer myCust = null;
            (string CustomerId, string CustomerName) result = default;

            var query = FluentQuery.Query()
                .From(() => myCust)
                .Select(() => result.CustomerId, () => myCust.Id)
                .Select(() => result.CustomerName, () => myCust.Name)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT [myCust].[Id] AS [Item1], [myCust].[Name] AS [Item2] FROM [Customer] AS [myCust]", query_str);
        }

        [Fact]
        public void T02_Where()
        {
            Customer myCust = null;
            (string CustomerId, string CustomerName) result = default;

            var query = FluentQuery.Query()
                .From(() => myCust)
                .Select(() => result.CustomerId, () => myCust.Id)
                .Select(() => result.CustomerName, () => myCust.Name)
                .Where((q) => q.Where(() => myCust.Name, "John").OrWhereContains(() => myCust.Name, "oh"))
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT [myCust].[Id] AS [Item1], [myCust].[Name] AS [Item2] FROM [Customer] AS [myCust] WHERE ([myCust].[Name] = 'John' OR LOWER([myCust].[Name]) like '%oh%')", query_str);
        }

        [Fact]
        public void T03_Join()
        {
            Contact myCont = null;
            Customer myCust = null;
            (string FirstName, string LastName, string CustomerId, string CustomerName) result = default;

            var query = FluentQuery.Query()
                .From(() => myCont)
                .Join(() => myCust, () => myCust.Id, () => myCont.CustomerId)
                .Select(() => result.FirstName, () => myCont.FirstName)
                .Select(() => result.LastName, () => myCont.LastName)
                .Select(() => result.CustomerId, () => myCont.CustomerId)
                .Select(() => result.CustomerName, () => myCust.Name)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT [myCont].[FirstName] AS [Item1], [myCont].[LastName] AS [Item2], [myCont].[contact_customer_id] AS [Item3], [myCust].[Name] AS [Item4] FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON [myCust].[Id] = [myCont].[contact_customer_id]", query_str);
        }

        [Fact]
        public void T04_JoinBuilder()
        {
            Contact myCont = null;
            Customer myCust = null;
            (string FirstName, string LastName, string CustomerId) result = default;

            var query = FluentQuery.Query()
                .From(() => myCont)
                .Join(() => myCust, (join) => join.WhereColumns(() => myCust.Id, () => myCont.CustomerId))
                .Select(() => result.FirstName, () => myCont.FirstName)
                .Select(() => result.LastName, () => myCont.LastName)
                .Select(() => result.CustomerId, () => myCust.Id)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT [myCont].[FirstName] AS [Item1], [myCont].[LastName] AS [Item2], [myCust].[Id] AS [Item3] FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON ([myCust].[Id] = [myCont].[contact_customer_id])", query_str);
        }

        [Fact]
        public void T05_GroupBy()
        {
            Customer myCust = null;
            Contact myCont = null;

            (string CustomerName, int ContactCount) result = default;

            var query = FluentQuery.Query()
                .From(() => myCont)
                .Join(() => myCust, (join) => join.WhereColumns(() => myCust.Id, () => myCont.CustomerId))
                .SelectCount(() => result.ContactCount, () => myCont.Id)
                .Select(() => result.CustomerName, () => myCust.Name)
                .GroupBy(() => myCust.Name)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT COUNT(myCont.Id) AS Item2, [myCust].[Name] AS [Item1] FROM [Contacts] AS [myCont] \nINNER JOIN [Customer] AS [myCust] ON ([myCust].[Id] = [myCont].[contact_customer_id]) GROUP BY [myCust].[Name]", query_str);
        }

        [Fact]
        public void T06_OrderBy()
        {
            Customer myCust = null;

            var query = FluentQuery.Query()
                .SelectAll(() => myCust)
                .OrderByColumn(() => myCust.Name)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT [myCust].[Name] AS [Name], [myCust].[Id] AS [Id] FROM [Customer] AS [myCust] ORDER BY [myCust].[Name]", query_str);
        }

        [Fact]
        public void T07_OrderByAlias()
        {
            Customer myCust = null;
            CustomerModel model = null;

            var query = FluentQuery.Query()
                .From(() => myCust)
                .SelectRawFormat(() => model.Name, "ISNULL({0}, 'Uknown')", () => myCust.Name)
                .OrderByAlias(() => model.Name)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT ISNULL(myCust.Name, 'Uknown') AS Name FROM [Customer] AS [myCust] ORDER BY ISNULL(myCust.Name, 'Uknown')", query_str);
        }

        [Fact]
        public void T08_SkipTake()
        {
            Customer myCust = null;

            var query = FluentQuery.Query()
                .Distinct()
                .SelectAll(() => myCust)
                .Skip(10)
                .Take(20)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            // TODO: https://github.com/sqlkata/querybuilder/issues/643#issuecomment-1709879159
            Assert.Equal("SELECT DISTINCT [myCust].[Name] AS [Name], [myCust].[Id] AS [Id] FROM [Customer] AS [myCust] ORDER BY (SELECT 0) OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY", query_str);
        }

		[Fact]
		public void T09_PerPage()
		{
			Customer myCust = null;

			var query = FluentQuery.Query()
                .Distinct()
				.SelectAll(() => myCust)
				.ForPage(2, 10)
				;

			var query_str = new SqlServerCompiler().Compile(query).ToString();

			Assert.NotNull(query_str);
            // TODO: https://github.com/sqlkata/querybuilder/issues/643#issuecomment-1709879159
            Assert.Equal("SELECT DISTINCT [myCust].[Name] AS [Name], [myCust].[Id] AS [Id] FROM [Customer] AS [myCust] ORDER BY (SELECT 0) OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY", query_str);
		}

		[Fact]
		public void T10_Schema()
		{
			BirdWithSchema bird = null;

			var query = FluentQuery.Query()
				.SelectAll(() => bird)
				;

			var query_str = new SqlServerCompiler().Compile(query).ToString();

			Assert.NotNull(query_str);
			Assert.Equal("SELECT [bird].[Name] AS [Name], [bird].[Id] AS [Id] FROM [OtherDatabase].[dbo].[Birds] AS [bird]", query_str);
		}

		[Fact]
		public void T11_NestedFields()
		{
			Contact cnt = null;

			NestedContactModel model = null;

			var query = FluentQuery.Query(() => cnt)
				.Select(() => model.FirstName, () => cnt.FirstName)
				.Select(() => model.LastName, () => cnt.LastName)
				.Select(() => model.Initials.FirstName, () => cnt.FirstName)
				.Select(() => model.Initials.LastName, () => cnt.LastName)
				;

			var query_str = new SqlServerCompiler().Compile(query).ToString();

			Assert.NotNull(query_str);
			Assert.Equal("SELECT [cnt].[FirstName] AS [FirstName], [cnt].[LastName] AS [LastName], [cnt].[FirstName] AS [Initials_FirstName], [cnt].[LastName] AS [Initials_LastName] FROM [Contacts] AS [cnt]", query_str);
		}

		[Fact]
		public void T12_FormattedRawQuery()
		{
			Contact cnt = null;

			var query = FluentQuery.Query(() => cnt)
				.SelectRawFormat("FullName", queryFormat: "ISNULL({0}, ?) + ' ' + ISNULL({1}, ?)",
                    new[] { FluentQuery.Expression(() => cnt.FirstName), FluentQuery.Expression(() => cnt.LastName) },
                    new[] { "John", "Smith" })
				.SelectRawFormat("Age", queryFormat: "{0} + 'y'", () => cnt.Age)
				.WhereRawFormat("{0} LIKE ?", columns: new[] { FluentQuery.Expression(() => cnt.FirstName) }, bindings: new[] { "John" })
				.If(true,
					q => q.Where(() => cnt.Age, 12, ">"),
					q => q.Where(() => cnt.Age, 16, ">"))
				;

			var query_str = new SqlServerCompiler().Compile(query).ToString();

			Assert.NotNull(query_str);
			Assert.Equal("SELECT ISNULL(cnt.FirstName, 'John') + ' ' + ISNULL(cnt.LastName, 'Smith') AS FullName, cnt.Age + 'y' AS Age FROM [Contacts] AS [cnt] WHERE cnt.FirstName LIKE 'John' AND [cnt].[Age] > 12", query_str);
		}

		[Fact]
		public void T13_ConditionalQuery()
		{
			Contact cnt = null;

			var queryBuilder = (bool ageCheck) =>
			{
				return FluentQuery.Query(() => cnt)
					.Select(() => cnt.FirstName)
					.Select(() => cnt.LastName)
					.If(ageCheck,
						q => q.Where(() => cnt.Age, 18, ">"),
						q => q.Where(() => cnt.Age, 16, ">"))
					.If(ageCheck, q => q.Where(() => cnt.Age, 60, "<="))
					;
			};

			// True
			{
				var query = queryBuilder(true);

				var query_str = new SqlServerCompiler().Compile(query).ToString();

				Assert.NotNull(query_str);
				Assert.Equal("SELECT cnt.FirstName, cnt.LastName FROM [Contacts] AS [cnt] WHERE [cnt].[Age] > 18 AND [cnt].[Age] <= 60", query_str);
			}

			// False
			{
				var query = queryBuilder(false);

				var query_str = new SqlServerCompiler().Compile(query).ToString();

				Assert.NotNull(query_str);
				Assert.Equal("SELECT cnt.FirstName, cnt.LastName FROM [Contacts] AS [cnt] WHERE [cnt].[Age] > 16", query_str);
			}
		}

		[Fact]
		public void T14_HelperFunctions()
		{
			Contact cnt = null;
			NestedContactModel model = null;

			// Column name
			{
				var result = FluentQuery.Column(() => cnt.CustomerId);

				Assert.Equal("contact_customer_id", result);
			}

			// Column name
			{
				var result = FluentQuery.ColumnWithAlias(() => cnt.CustomerId);

				Assert.Equal("cnt.contact_customer_id", result);
			}

			// Column with alias name
			{
				var result = FluentQuery.AliasFromColumn(() => cnt.CustomerId);

				Assert.Equal("cnt", result);
			}

			// Alias name
			{
				var result = FluentQuery.Alias(() => model.FirstName);

				Assert.Equal("FirstName", result);
			}

			// Table name
			{
				var result = FluentQuery.Table<Contact>();

				Assert.Equal("Contacts", result);
			}

			// Table name
			{
				var result = FluentQuery.Table(() => cnt);

				Assert.Equal("Contacts", result);
			}
		}
    }
}