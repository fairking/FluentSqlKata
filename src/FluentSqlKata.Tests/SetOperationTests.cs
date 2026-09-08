using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;

namespace FluentSqlKata.Tests
{
    public class SetOperationTests
    {
        [Fact]
        public void T43_WithRawFormat()
        {
            Contact cnt = null;
            Contact cte = null;

            FluentQuery.Query()
                .WithRawFormat(() => cte, "SELECT {0} FROM Contacts WHERE {0} IS NOT NULL", () => cnt.FirstName)
                .From(() => cte)
                .AssertSql("WITH [cte] AS (SELECT cnt.FirstName FROM Contacts WHERE cnt.FirstName IS NOT NULL)\nSELECT * FROM [Contacts] AS [cte]");
        }

        [Fact]
        public void T44_CombineRawFormat()
        {
            Contact cnt = null;

            // Note: the raw statement is appended verbatim; the set-operator keyword must be part of the raw SQL
            FluentQuery.Query(() => cnt)
                .Select(() => cnt.FirstName)
                .CombineRawFormat("UNION SELECT {0} FROM Contacts", () => cnt.LastName)
                .AssertSql("SELECT cnt.FirstName FROM [Contacts] AS [cnt] UNION SELECT cnt.LastName FROM Contacts");

            FluentQuery.Query(() => cnt)
                .CombineRawFormat("UNION SELECT {0} FROM Contacts WHERE {0} > ?",
                    new[] { FluentQuery.Expression(() => cnt.LastName) },
                    new[] { "x" })
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] UNION SELECT cnt.LastName FROM Contacts WHERE cnt.LastName > 'x'");
        }

        [Fact]
        public void T45_UnionExceptIntersectRawFormat()
        {
            Contact cnt = null;

            FluentQuery.Query(() => cnt)
                .Select(() => cnt.FirstName)
                .UnionRawFormat("UNION SELECT {0} FROM Contacts", () => cnt.LastName)
                .AssertSql("SELECT cnt.FirstName FROM [Contacts] AS [cnt] UNION SELECT cnt.LastName FROM Contacts");

            FluentQuery.Query(() => cnt)
                .Select(() => cnt.FirstName)
                .ExceptRawFormat("EXCEPT SELECT {0} FROM Contacts", () => cnt.LastName)
                .AssertSql("SELECT cnt.FirstName FROM [Contacts] AS [cnt] EXCEPT SELECT cnt.LastName FROM Contacts");

            FluentQuery.Query(() => cnt)
                .Select(() => cnt.FirstName)
                .IntersectRawFormat("INTERSECT SELECT {0} FROM Contacts WHERE {0} > ?",
                    new[] { FluentQuery.Expression(() => cnt.LastName) },
                    new[] { "x" })
                .AssertSql("SELECT cnt.FirstName FROM [Contacts] AS [cnt] INTERSECT SELECT cnt.LastName FROM Contacts WHERE cnt.LastName > 'x'");
        }

        [Fact]
        public void T46_FromRawFormat()
        {
            Contact cnt = null;

            // Note: FromRaw does not wrap the expression in parentheses; include them in the raw SQL when required
            FluentQuery.Query()
                .FromRawFormat("OPENJSON(?)", System.Array.Empty<System.Linq.Expressions.Expression<Func<object>>>(), new[] { "[]" })
                .AssertSql("SELECT * FROM OPENJSON('[]')");

            FluentQuery.Query()
                .FromRawFormat("(SELECT {0} FROM Contacts)", () => cnt.FirstName)
                .AssertSql("SELECT * FROM (SELECT cnt.FirstName FROM Contacts)");

            FluentQuery.Query()
                .FromRawFormat("(SELECT {0} FROM Contacts WHERE {0} > ?)",
                    new[] { FluentQuery.Expression(() => cnt.FirstName) },
                    new[] { "x" })
                .AssertSql("SELECT * FROM (SELECT cnt.FirstName FROM Contacts WHERE cnt.FirstName > 'x')");
        }
    }
}
