using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;
using FluentSqlKata.Tests.Models;

namespace FluentSqlKata.Tests
{
    public class OrderTests
    {
        [Fact]
        public void T38_OrderByAliasOverSelects()
        {
            Contact cnt = null;
            ContactReport result = null;

            // Order by a plain selected column (resolves to the underlying column expression)
            FluentQuery.Query(() => cnt)
                .Select(() => result.FirstName, () => cnt.FirstName)
                .OrderByAlias(() => result.FirstName)
                .AssertSql("SELECT [cnt].[FirstName] AS [FirstName] FROM [Contacts] AS [cnt] ORDER BY [cnt].[FirstName]");

            // Alias matching a nested (dotted) alias name
            FluentQuery.Query(() => cnt)
                .Select(() => result.FirstName, () => cnt.FirstName)
                .OrderByAliasDesc(() => result.FirstName)
                .AssertSql("SELECT [cnt].[FirstName] AS [FirstName] FROM [Contacts] AS [cnt] ORDER BY [cnt].[FirstName] DESC");
        }

        [Fact]
        public void T39_OrderByAliasOverRawAndAggregates()
        {
            Contact cnt = null;
            ContactReport result = null;

            FluentQuery.Query(() => cnt)
                .SelectRawFormat(() => result.FullName, "ISNULL({0}, 'Uknown')", () => cnt.FirstName)
                .OrderByAlias(() => result.FullName)
                .AssertSql("SELECT ISNULL(cnt.FirstName, 'Uknown') AS FullName FROM [Contacts] AS [cnt] ORDER BY ISNULL(cnt.FirstName, 'Uknown')");

            FluentQuery.Query(() => cnt)
                .SelectCount(() => result.ContactCount, () => cnt.Id)
                .OrderByAlias(() => result.ContactCount)
                .AssertSql("SELECT COUNT(cnt.Id) AS ContactCount FROM [Contacts] AS [cnt] ORDER BY COUNT(cnt.Id)");

            FluentQuery.Query(() => cnt)
                .SelectRawFormat(() => result.FullName, "ISNULL({0}, 'Uknown')", () => cnt.FirstName)
                .OrderByAliasDesc(() => result.FullName)
                .AssertSql("SELECT ISNULL(cnt.FirstName, 'Uknown') AS FullName FROM [Contacts] AS [cnt] ORDER BY ISNULL(cnt.FirstName, 'Uknown') desc");

            FluentQuery.Query(() => cnt)
                .SelectCount(() => result.ContactCount, () => cnt.Id)
                .OrderByAliasDesc(() => result.ContactCount)
                .AssertSql("SELECT COUNT(cnt.Id) AS ContactCount FROM [Contacts] AS [cnt] ORDER BY COUNT(cnt.Id) desc");
        }

        [Fact]
        public void T41_OrderByUnknownAliasThrows()
        {
            Contact cnt = null;
            ContactReport result = null;

            var ex = Assert.Throws<ArgumentException>(() =>
                FluentQuery.Query(() => cnt)
                    .Select(() => cnt.FirstName)
                    .OrderByAlias(() => result.LastName));

            Assert.Contains("The alias name 'LastName' not found or not supported", ex.Message);

            Assert.Throws<ArgumentException>(() =>
                FluentQuery.Query(() => cnt)
                    .OrderByAliasDesc(() => result.FirstName));
        }

        [Fact]
        public void T42_OrderByRawFormat()
        {
            Contact cnt = null;

            FluentQuery.Query(() => cnt)
                .OrderByRawFormat("LEN({0})", () => cnt.FirstName)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] ORDER BY LEN(cnt.FirstName)");

            FluentQuery.Query(() => cnt)
                .OrderByRawFormat("LEN({0}) > ?",
                    new[] { FluentQuery.Expression(() => cnt.FirstName) },
                    new[] { 5 })
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] ORDER BY LEN(cnt.FirstName) > 5");
        }
    }
}
