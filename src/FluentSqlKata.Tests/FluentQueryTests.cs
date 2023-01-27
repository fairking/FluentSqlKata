using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Models;
using SqlKata.Compilers;

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
                .SelectRaw(() => model.Name, "ISNULL({0}, 'Uknown')", () => myCust.Name)
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
                .SelectAll(() => myCust)
                .Skip(10)
                .Take(20)
                ;

            var query_str = new SqlServerCompiler().Compile(query).ToString();

            Assert.NotNull(query_str);
            Assert.Equal("SELECT * FROM (SELECT [myCust].[Name] AS [Name], [myCust].[Id] AS [Id], ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS [row_num] FROM [Customer] AS [myCust]) AS [results_wrapper] WHERE [row_num] BETWEEN 11 AND 30", query_str);
        }

		[Fact]
		public void T09_PerPage()
		{
			Customer myCust = null;

			var query = FluentQuery.Query()
				.SelectAll(() => myCust)
				.ForPage(2, 10)
				;

			var query_str = new SqlServerCompiler().Compile(query).ToString();

			Assert.NotNull(query_str);
			Assert.Equal("SELECT * FROM (SELECT [myCust].[Name] AS [Name], [myCust].[Id] AS [Id], ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS [row_num] FROM [Customer] AS [myCust]) AS [results_wrapper] WHERE [row_num] BETWEEN 11 AND 20", query_str);
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
	}
}