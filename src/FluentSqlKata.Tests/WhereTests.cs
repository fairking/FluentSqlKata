using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;

namespace FluentSqlKata.Tests
{
    public class WhereTests
    {
        [Fact]
        public void T22_WhereOperators()
        {
            Contact cnt = null;

            FluentQuery.Query(() => cnt)
                .Where(() => cnt.Age, 18, ">")
                .OrWhereNot(() => cnt.Age, 5)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[Age] > 18 OR NOT ([cnt].[Age] = 5)");

            FluentQuery.Query(() => cnt)
                .WhereNot(() => cnt.Age, 5)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE NOT ([cnt].[Age] = 5)");

            FluentQuery.Query(() => cnt)
                .Where(() => cnt.Age, 18, ">")
                .OrWhere(() => cnt.Age, 30, "<")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[Age] > 18 OR [cnt].[Age] < 30");
        }

        [Fact]
        public void T23_WhereColumns()
        {
            Contact cnt = null;
            Customer cust = null;

            FluentQuery.Query(() => cnt)
                .WhereColumns(() => cust.Id, () => cnt.CustomerId, ">")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cust].[Id] > [cnt].[contact_customer_id]");

            FluentQuery.Query(() => cnt)
                .WhereColumns(() => cust.Id, "cnt.contact_customer_id")
                .OrWhereColumns(() => cust.Id, () => cnt.CustomerId)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cust].[Id] = [cnt].[contact_customer_id] OR [cust].[Id] = [cnt].[contact_customer_id]");
        }

        [Fact]
        public void T24_WhereNullNotNull()
        {
            Contact cnt = null;

            FluentQuery.Query(() => cnt)
                .WhereNull(() => cnt.FirstName)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[FirstName] IS NULL");

            FluentQuery.Query(() => cnt)
                .WhereNotNull(() => cnt.FirstName)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[FirstName] IS NOT NULL");

            FluentQuery.Query(() => cnt)
                .WhereNull(() => cnt.FirstName)
                .OrWhereNotNull(() => cnt.LastName)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[FirstName] IS NULL OR [cnt].[LastName] IS NOT NULL");
        }

        [Fact]
        public void T25_WhereTrueFalse()
        {
            MappedEntity e = null;

            FluentQuery.Query(() => e)
                .WhereTrue(() => e.IsActive)
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] WHERE [e].[IsActive] = cast(1 as bit)");

            FluentQuery.Query(() => e)
                .WhereFalse(() => e.IsActive)
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] WHERE [e].[IsActive] = cast(0 as bit)");

            FluentQuery.Query(() => e)
                .WhereTrue(() => e.IsActive)
                .OrWhereFalse(() => e.IsActive)
                .AssertSql("SELECT * FROM [MappedEntities] AS [e] WHERE [e].[IsActive] = cast(1 as bit) OR [e].[IsActive] = cast(0 as bit)");
        }

        [Fact]
        public void T26_WhereLikeFamily()
        {
            Contact cnt = null;

            // Plain LIKE (no % pattern is added; value lowered when not case sensitive)
            FluentQuery.Query(() => cnt)
                .WhereLike(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE LOWER([cnt].[FirstName]) like 'jo'");

            // Case sensitive
            FluentQuery.Query(() => cnt)
                .WhereLike(() => cnt.FirstName, "Jo", caseSensitive: true)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[FirstName] like 'Jo'");

            FluentQuery.Query(() => cnt)
                .WhereStarts(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE LOWER([cnt].[FirstName]) like 'jo%'");

            FluentQuery.Query(() => cnt)
                .WhereEnds(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE LOWER([cnt].[FirstName]) like '%jo'");

            FluentQuery.Query(() => cnt)
                .WhereContains(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE LOWER([cnt].[FirstName]) like '%jo%'");

            FluentQuery.Query(() => cnt)
                .WhereNotLike(() => cnt.FirstName, "Jo")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE NOT (LOWER([cnt].[FirstName]) like 'jo')");

            FluentQuery.Query(() => cnt)
                .WhereLike(() => cnt.FirstName, "Jo")
                .OrWhereNotContains(() => cnt.FirstName, "hn")
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE LOWER([cnt].[FirstName]) like 'jo' OR NOT (LOWER([cnt].[FirstName]) like '%hn%')");
        }

        [Fact]
        public void T27_WhereBetweenAndIn()
        {
            Contact cnt = null;

            FluentQuery.Query(() => cnt)
                .WhereBetween(() => cnt.Age, 5, 10)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[Age] BETWEEN 5 AND 10");

            FluentQuery.Query(() => cnt)
                .WhereNotBetween(() => cnt.Age, 5, 10)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[Age] NOT BETWEEN 5 AND 10");

            FluentQuery.Query(() => cnt)
                .WhereIn(() => cnt.Age, new[] { 5, 7 })
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[Age] IN (5, 7)");

            FluentQuery.Query(() => cnt)
                .WhereIn(() => cnt.Age, new[] { 5, 7 })
                .OrWhereNotIn(() => cnt.Age, new[] { 11 })
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[Age] IN (5, 7) OR [cnt].[Age] NOT IN (11)");

            // Subquery
            var sub = FluentQuery.Query<Customer>().Select("Id");
            FluentQuery.Query(() => cnt)
                .WhereIn(() => cnt.CustomerId, sub)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[contact_customer_id] IN (SELECT [Id] FROM [Customer])");
        }

        [Fact]
        public void T28_WhereDateFamily()
        {
            LargeEntity e = null;

            FluentQuery.Query(() => e)
                .WhereDate(() => e.Prop3, "2020-01-01")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] WHERE CAST([e].[Prop3] AS DATE) = '2020-01-01'");

            FluentQuery.Query(() => e)
                .WhereTime(() => e.Prop3, "08:00")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] WHERE CAST([e].[Prop3] AS TIME) = '08:00'");

            FluentQuery.Query(() => e)
                .WhereDatePart("year", () => e.Prop3, 2020)
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] WHERE DATEPART(YEAR, [e].[Prop3]) = 2020");

            FluentQuery.Query(() => e)
                .WhereDate(() => e.Prop3, "2020-01-01")
                .OrWhereNotDate(() => e.Prop3, "2021-01-01")
                .AssertSql("SELECT * FROM [LargeEntity] AS [e] WHERE CAST([e].[Prop3] AS DATE) = '2020-01-01' OR NOT (CAST([e].[Prop3] AS DATE) = '2021-01-01')");
        }
    }
}
