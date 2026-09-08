using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;
using FluentSqlKata.Tests.Models;

namespace FluentSqlKata.Tests
{
    public class SelectTests
    {
        [Fact]
        public void T47_SelectSubquery()
        {
            Contact cnt = null;
            ContactReport result = null;

            var sub = FluentQuery.Query<Contact>().Select("Id");

            FluentQuery.Query(() => cnt)
                .Select(() => result.FirstName, () => cnt.FirstName)
                .Select(() => result.ContactCount, sub)
                .AssertSql("SELECT [cnt].[FirstName] AS [FirstName], (SELECT [Id] FROM [Contacts]) AS [ContactCount] FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T48_SelectRawWithBindings()
        {
            Contact cnt = null;
            ContactReport result = null;

            FluentQuery.Query(() => cnt)
                .SelectRaw(() => result.Name, "? + ' ' + ?", "John", "Smith")
                .AssertSql("SELECT 'John' + ' ' + 'Smith' AS Name FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .SelectRaw(() => result.Age, "DATEDIFF(year, ?, GETDATE())", "2000-01-01")
                .AssertSql("SELECT DATEDIFF(year, '2000-01-01', GETDATE()) AS Age FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T49_SelectRawFormatVariants()
        {
            Contact cnt = null;
            ContactReport result = null;

            // Typed alias + columns
            FluentQuery.Query(() => cnt)
                .SelectRawFormat(() => result.FullName, "{0} + ' ' + {1}", () => cnt.FirstName, () => cnt.LastName)
                .AssertSql("SELECT cnt.FirstName + ' ' + cnt.LastName AS FullName FROM [Contacts] AS [cnt]");

            // String alias + columns
            FluentQuery.Query(() => cnt)
                .SelectRawFormat("FullName", "{0} + ' ' + {1}", () => cnt.FirstName, () => cnt.LastName)
                .AssertSql("SELECT cnt.FirstName + ' ' + cnt.LastName AS FullName FROM [Contacts] AS [cnt]");

            // Typed alias + columns + bindings
            FluentQuery.Query(() => cnt)
                .SelectRawFormat(() => result.FullName, "ISNULL({0}, ?) + ' ' + ISNULL({1}, ?)",
                    new[] { FluentQuery.Expression(() => cnt.FirstName), FluentQuery.Expression(() => cnt.LastName) },
                    new[] { "John", "Smith" })
                .AssertSql("SELECT ISNULL(cnt.FirstName, 'John') + ' ' + ISNULL(cnt.LastName, 'Smith') AS FullName FROM [Contacts] AS [cnt]");

            // String alias + columns + bindings
            FluentQuery.Query(() => cnt)
                .SelectRawFormat("FullName", "ISNULL({0}, ?)",
                    new[] { FluentQuery.Expression(() => cnt.FirstName) },
                    new[] { "John" })
                .AssertSql("SELECT ISNULL(cnt.FirstName, 'John') AS FullName FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T50_SelectAllHonoursMappingAttributes()
        {
            MappedEntity e = null;

            FluentQuery.Query()
                .SelectAll(() => e)
                .AssertSql("SELECT [e].[mapped_name] AS [Name], [e].[sql_kata_name] AS [Raw], [e].[Plain] AS [Plain], [e].[IsActive] AS [IsActive], [e].[Id] AS [Id] FROM [MappedEntities] AS [e]");
        }

        [Fact]
        public void T51_SelectAllWithoutAliasSetsFrom()
        {
            FluentQuery.Query()
                .SelectAll<Contact>()
                .AssertSql("SELECT [FirstName] AS [FirstName], [LastName] AS [LastName], [Age] AS [Age], [contact_customer_id] AS [CustomerId], [Id] AS [Id] FROM [Contacts]");

            // SelectAll overrides any previously set FROM clause (documented behavior)
            Contact cnt = null;
            FluentQuery.Query()
                .From(() => cnt)
                .SelectAll<Contact>()
                .AssertSql("SELECT [FirstName] AS [FirstName], [LastName] AS [LastName], [Age] AS [Age], [contact_customer_id] AS [CustomerId], [Id] AS [Id] FROM [Contacts]");
        }

        [Fact]
        public void T52_DuplicateAliasThrowsWithClearMessage()
        {
            Contact cnt = null;

            var ex = Assert.Throws<ArgumentException>(() =>
                FluentQuery.Query(() => cnt)
                    .Select(() => cnt.FirstName)
                    .Select(() => cnt.FirstName));

            Assert.Contains("The alias 'FirstName' is already registered in the query", ex.Message);

            // SelectAll + Select of the same property is also a duplicate
            var ex2 = Assert.Throws<ArgumentException>(() =>
                FluentQuery.Query(() => cnt)
                    .SelectAll(() => cnt)
                    .Select(() => cnt.FirstName));

            Assert.Contains("The alias 'FirstName' is already registered in the query", ex2.Message);

            // Aggregates are covered too
            var ex3 = Assert.Throws<ArgumentException>(() =>
                FluentQuery.Query(() => cnt)
                    .SelectCount("cnt", () => cnt.Id)
                    .SelectCount("cnt", () => cnt.Age));

            Assert.Contains("The alias 'cnt' is already registered in the query", ex3.Message);
        }

        [Fact]
        public void T53_TooLargeTupleThrows()
        {
            LargeEntity e = default;

            // Note: maximum of 7 items of Tuple are supported
            (int P1, int P2, int P3, int P4, int P5, int P6, int P7, int P8, int P9) m = default;

            Assert.Throws<ArgumentException>(() =>
                FluentQuery.Query(() => e)
                    .Select(() => m.P9, () => e.Prop9));
        }
    }
}
