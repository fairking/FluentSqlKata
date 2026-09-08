using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;

namespace FluentSqlKata.Tests
{
    public class HavingTests
    {
        [Fact]
        public void T29_HavingOperators()
        {
            Contact cnt = null;

            FluentQuery.Query(() => cnt)
                .GroupBy(() => cnt.Age)
                .Having(() => cnt.Age, 18, ">")
                .OrHavingNot(() => cnt.Age, 5)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] GROUP BY [cnt].[Age] HAVING [cnt].[Age] > 18 OR NOT ([cnt].[Age] = 5)");

            FluentQuery.Query(() => cnt)
                .HavingNot(() => cnt.Age, 5)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING NOT ([cnt].[Age] = 5)");

            FluentQuery.Query(() => cnt)
                .HavingColumns(() => cnt.CustomerId, () => cnt.Age, ">")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING [cnt].[contact_customer_id] > [cnt].[Age]");
        }

        [Fact]
        public void T30_HavingNullTrueInBetween()
        {
            MappedEntity e = null;

            FluentQuery.Query(() => e)
                .GroupBy(() => e.Plain)
                .HavingNull(() => e.Plain)
                .OrHavingNotNull(() => e.Plain)
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] GROUP BY [e].[Plain] HAVING [e].[Plain] IS NULL OR [e].[Plain] IS NOT NULL");

            FluentQuery.Query(() => e)
                .GroupBy(() => e.Plain)
                .HavingTrue(() => e.IsActive)
                .OrHavingFalse(() => e.IsActive)
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] GROUP BY [e].[Plain] HAVING [e].[IsActive] = cast(1 as bit) AND [e].[IsActive] = cast(0 as bit)");

            FluentQuery.Query(() => e)
                .GroupBy(() => e.Plain)
                .HavingIn(() => e.Plain, new[] { "a", "b" })
                .OrHavingNotIn(() => e.Plain, new[] { "c" })
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] GROUP BY [e].[Plain] HAVING [e].[Plain] IN ('a', 'b') OR [e].[Plain] NOT IN ('c')");

            FluentQuery.Query(() => e)
                .GroupBy(() => e.Plain)
                .HavingBetween(() => e.Plain, "a", "m")
                .OrHavingNotBetween(() => e.Plain, "n", "z")
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] GROUP BY [e].[Plain] HAVING [e].[Plain] BETWEEN 'a' AND 'm' OR [e].[Plain] NOT BETWEEN 'n' AND 'z'");

            // Subquery
            var sub = FluentQuery.Query<Contact>().Select("FirstName");
            FluentQuery.Query(() => e)
                .GroupBy(() => e.Plain)
                .HavingIn(() => e.Plain, sub)
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] GROUP BY [e].[Plain] HAVING [e].[Plain] IN (SELECT [FirstName] FROM [Contacts])");
        }

        [Fact]
        public void T31_HavingLikeFamily()
        {
            Contact cnt = null;

            // Plain LIKE (no % pattern is added; value lowered when not case sensitive)
            FluentQuery.Query(() => cnt)
                .HavingLike(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING LOWER([cnt].[FirstName]) like 'jo'");

            // Case sensitive
            FluentQuery.Query(() => cnt)
                .HavingLike(() => cnt.FirstName, "Jo", caseSensitive: true)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING [cnt].[FirstName] like 'Jo'");

            FluentQuery.Query(() => cnt)
                .HavingStarts(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING LOWER([cnt].[FirstName]) like 'jo%'");

            FluentQuery.Query(() => cnt)
                .HavingEnds(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING LOWER([cnt].[FirstName]) like '%jo'");

            FluentQuery.Query(() => cnt)
                .HavingContains(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING LOWER([cnt].[FirstName]) like '%jo%'");

            FluentQuery.Query(() => cnt)
                .HavingNotContains(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING NOT (LOWER([cnt].[FirstName]) like '%jo%')");

            FluentQuery.Query(() => cnt)
                .HavingLike(() => cnt.FirstName, "Jo")
                .OrHavingNotStarts(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] HAVING LOWER([cnt].[FirstName]) like 'jo' OR NOT (LOWER([cnt].[FirstName]) like 'jo%')");
        }

        [Fact]
        public void T32_HavingDateFamily()
        {
            LargeEntity e = null;

            FluentQuery.Query(() => e)
                .HavingDate(() => e.Prop3, "2020-01-01")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] HAVING CAST([e].[Prop3] AS DATE) = '2020-01-01'");

            FluentQuery.Query(() => e)
                .HavingTime(() => e.Prop3, "08:00")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] HAVING CAST([e].[Prop3] AS TIME) = '08:00'");

            FluentQuery.Query(() => e)
                .HavingDatePart(() => e.Prop3, 2020, "year")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] HAVING DATEPART(YEAR, [e].[Prop3]) = 2020");

            FluentQuery.Query(() => e)
                .HavingDate(() => e.Prop3, "2020-01-01")
                .OrHavingNotDate(() => e.Prop3, "2021-01-01")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] HAVING CAST([e].[Prop3] AS DATE) = '2020-01-01' OR NOT (CAST([e].[Prop3] AS DATE) = '2021-01-01')");

            FluentQuery.Query(() => e)
                .HavingRawFormat<LargeEntity>("DATEPART(year, {0}) > 2000", () => e.Prop3)
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] HAVING DATEPART(year, e.Prop3) > 2000");
        }
    }
}
