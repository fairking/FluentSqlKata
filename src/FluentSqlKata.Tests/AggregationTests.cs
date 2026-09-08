using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;
using FluentSqlKata.Tests.Models;

namespace FluentSqlKata.Tests
{
    public class AggregationTests
    {
        [Fact]
        public void T33_SelectAggregatesTypedAlias()
        {
            Contact cnt = null;
            ContactReport result = null;

            FluentQuery.Query(() => cnt)
                .SelectCount(() => result.ContactCount, () => cnt.Id)
                .SelectMin(() => result.Youngest, () => cnt.Age)
                .SelectMax(() => result.Oldest, () => cnt.Age)
                .SelectAvg(() => result.AverageAge, () => cnt.Age)
                .SelectSum(() => result.TotalAge, () => cnt.Age)
                .AssertSql("SELECT COUNT(cnt.Id) AS ContactCount, MIN(cnt.Age) AS Youngest, MAX(cnt.Age) AS Oldest, AVG(cnt.Age) AS AverageAge, SUM(cnt.Age) AS TotalAge FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T34_SelectAggregatesStringAliasAndColumnOnly()
        {
            Contact cnt = null;

            // String alias
            FluentQuery.Query(() => cnt)
                .SelectCount("ContactCount", () => cnt.Id)
                .SelectMin("Youngest", () => cnt.Age)
                .SelectMax("Oldest", () => cnt.Age)
                .SelectAvg("AverageAge", () => cnt.Age)
                .SelectSum("TotalAge", () => cnt.Age)
                .AssertSql("SELECT COUNT(cnt.Id) AS ContactCount, MIN(cnt.Age) AS Youngest, MAX(cnt.Age) AS Oldest, AVG(cnt.Age) AS AverageAge, SUM(cnt.Age) AS TotalAge FROM [Contacts] AS [cnt]");

            // Column only (alias = column name)
            FluentQuery.Query(() => cnt)
                .SelectCount(() => cnt.Id)
                .AssertSql("SELECT COUNT(cnt.Id) AS Id FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .SelectMax(() => cnt.Age)
                .AssertSql("SELECT MAX(cnt.Age) AS Age FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T35_AsAggregatesTypedAlias()
        {
            Contact cnt = null;
            ContactReport result = null;

            FluentQuery.Query(() => cnt)
                .AsCount(() => result.ContactCount, () => cnt.Id)
                .AssertSql("SELECT COUNT(cnt.Id) AS ContactCount FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsAvg(() => result.AverageAge, () => cnt.Age)
                .AssertSql("SELECT AVG(cnt.Age) AS AverageAge FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsSum(() => result.TotalAge, () => cnt.Age)
                .AssertSql("SELECT SUM(cnt.Age) AS TotalAge FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsMax(() => result.Oldest, () => cnt.Age)
                .AssertSql("SELECT MAX(cnt.Age) AS Oldest FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsMin(() => result.Youngest, () => cnt.Age)
                .AssertSql("SELECT MIN(cnt.Age) AS Youngest FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsAverage(() => result.AverageAge2, () => cnt.Age)
                .AssertSql("SELECT AVG(cnt.Age) AS AverageAge2 FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T36_AsAggregatesStringAlias()
        {
            Contact cnt = null;

            FluentQuery.Query(() => cnt)
                .AsCount("ContactCount", () => cnt.Id)
                .AssertSql("SELECT COUNT(cnt.Id) AS ContactCount FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsAvg("AverageAge", () => cnt.Age)
                .AssertSql("SELECT AVG(cnt.Age) AS AverageAge FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsSum("TotalAge", () => cnt.Age)
                .AssertSql("SELECT SUM(cnt.Age) AS TotalAge FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsMax("Oldest", () => cnt.Age)
                .AssertSql("SELECT MAX(cnt.Age) AS Oldest FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsMin("Youngest", () => cnt.Age)
                .AssertSql("SELECT MIN(cnt.Age) AS Youngest FROM [Contacts] AS [cnt]");

            FluentQuery.Query(() => cnt)
                .AsAverage("AverageAge", () => cnt.Age)
                .AssertSql("SELECT AVG(cnt.Age) AS AverageAge FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T37_GroupBy()
        {
            Contact cnt = null;
            LargeEntity e = null;

            FluentQuery.Query(() => cnt)
                .SelectCount("ContactCount", () => cnt.Id)
                .GroupBy(() => cnt.Age)
                .AssertSql("SELECT COUNT(cnt.Id) AS ContactCount FROM [Contacts] AS [cnt] GROUP BY [cnt].[Age]");

            // Note: FluentQuery.GroupByRaw<T> is shadowed by SqlKata's own instance method
        // GroupByRaw(string, params object[]) (instance methods always win over extensions),
        // so raw group-by expressions must be supplied fully composed
            FluentQuery.Query(() => e)
                .GroupByRaw("DATEPART(year, [e].[Prop3])")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] GROUP BY DATEPART(year, [e].[Prop3])");
        }
    }
}
