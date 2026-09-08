using FluentSqlKata.Tests.Entities;
using FluentSqlKata.Tests.Helpers;
using FluentSqlKata.Tests.Models;
using SqlKata.Compilers;

namespace FluentSqlKata.Tests
{
    public class LifecycleTests
    {
        [Fact]
        public void T54_QueryCreationOverloads()
        {
            Contact cnt = null;

            FluentQuery.Query<Contact>()
                .AssertSql("SELECT * FROM [Contacts]");

            FluentQuery.Query(() => cnt)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt]");

            FluentQuery.Query<Contact>("MyContacts", () => cnt)
                .AssertSql("SELECT * FROM [MyContacts] AS [cnt]");
        }

        [Fact]
        public void T55_FromOverloads()
        {
            Contact cnt = null;

            FluentQuery.Query()
                .From<Contact>()
                .AssertSql("SELECT * FROM [Contacts]");

            FluentQuery.Query()
                .From("MyContacts", () => cnt)
                .AssertSql("SELECT * FROM [MyContacts] AS [cnt]");

            FluentQuery.Query()
                .From(() => cnt)
                .AssertSql("SELECT * FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T56_AsSubqueryAlias()
        {
            Contact cnt = null;
            CustomerModel model = null;

            var sub = FluentQuery.Query(() => cnt)
                .Select(() => cnt.FirstName)
                .As(() => model.Name);

            FluentQuery.Query()
                .From(sub)
                .AssertSql("SELECT * FROM (SELECT cnt.FirstName FROM [Contacts] AS [cnt]) AS [Name]");
        }

        [Fact]
        public void T57_ClonePreservesAliasState()
        {
            Contact cnt = null;
            ContactReport result = null;

            var query = FluentQuery.Query(() => cnt)
                .SelectRawFormat(() => result.FullName, "ISNULL({0}, 'Uknown')", () => cnt.FirstName)
                .Select(() => result.FirstName, () => cnt.FirstName);

            var clone = query.Clone();

            // Alias registrations survive the clone: OrderByAlias resolves on the clone
            clone.OrderByAlias(() => result.FullName)
                .AssertSql("SELECT ISNULL(cnt.FirstName, 'Uknown') AS FullName, [cnt].[FirstName] AS [FirstName] FROM [Contacts] AS [cnt] ORDER BY ISNULL(cnt.FirstName, 'Uknown')");

            // The clone is still a fluent wrapper for newly added selects as well
            clone.Select(() => cnt.LastName)
                .OrderByAlias(() => cnt.LastName)
                .AssertSql("SELECT ISNULL(cnt.FirstName, 'Uknown') AS FullName, [cnt].[FirstName] AS [FirstName], cnt.LastName FROM [Contacts] AS [cnt] ORDER BY ISNULL(cnt.FirstName, 'Uknown'), [cnt].[LastName]");

            // The original query is unaffected
            query.AssertSql("SELECT ISNULL(cnt.FirstName, 'Uknown') AS FullName, [cnt].[FirstName] AS [FirstName] FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T58_PlainQueryThrowsInvalidOperationException()
        {
            Contact cnt = null;

            var query = new SqlKata.Query().From("Contacts");

            var ex = Assert.Throws<InvalidOperationException>(() => query.Select(() => cnt.FirstName));
            Assert.Contains("Use 'FluentQuery.Query()' instead of 'new Query()'", ex.Message);
        }

        [Fact]
        public void T59_IfEdgeCases()
        {
            Contact cnt = null;

            // ifTrue null throws
            Assert.Throws<ArgumentNullException>(() =>
                FluentQuery.Query(() => cnt).If(true, null));

            // ifFalse applied when condition is false
            FluentQuery.Query(() => cnt)
                .If(false, q => q.Where(() => cnt.Age, 18, ">"), q => q.Where(() => cnt.Age, 16, ">"))
                .AssertSql("SELECT * FROM [Contacts] AS [cnt] WHERE [cnt].[Age] > 16");

            // No ifFalse: query unchanged
            FluentQuery.Query(() => cnt)
                .If(false, q => q.Where(() => cnt.Age, 18, ">"))
                .AssertSql("SELECT * FROM [Contacts] AS [cnt]");
        }

        [Fact]
        public void T60_WithVariableObsoleteStillWorks()
        {
            Contact cnt = null;

#pragma warning disable CS0618
            var query = FluentQuery.Query(() => cnt)
                .WithVariable("@Today", "2020-01-01");
#pragma warning restore CS0618

            Assert.Equal("2020-01-01", query.Variables["@Today"]);
        }

        [Fact]
        public void T61_HelperFunctions()
        {
            Contact cnt = null;
            BirdWithSchema bird = null;

            // Expression helper returns the same instance
            var column = FluentQuery.Expression(() => cnt.FirstName);
            Assert.Equal("FirstName", FluentQuery.Alias(column));
            Assert.Equal("cnt", FluentQuery.AliasFromColumn(column));
            Assert.Equal("cnt.FirstName", FluentQuery.ColumnWithAlias(column));

            // Table with schema
            Assert.Equal("OtherDatabase.dbo.Birds", FluentQuery.Table<BirdWithSchema>());
            Assert.Equal("OtherDatabase.dbo.Birds", FluentQuery.Table(() => bird));
        }

        [Fact]
        public void T62_CrossCompilerSmoke()
        {
            Contact cnt = null;
            Customer cust = null;

            var query = FluentQuery.Query(() => cnt)
                .Select(() => cnt.FirstName)
                .Select("CustomerId", () => cnt.CustomerId)
                .Join(() => cust, () => cust.Id, () => cnt.CustomerId)
                .Where(() => cnt.Age, 21, ">");

            query.AssertSql(
                "SELECT cnt.FirstName, [cnt].[contact_customer_id] AS [CustomerId] FROM [Contacts] AS [cnt] \nINNER JOIN [Customer] AS [cust] ON [cust].[Id] = [cnt].[contact_customer_id] WHERE [cnt].[Age] > 21");

            query.AssertSql(
                "SELECT cnt.FirstName, `cnt`.`contact_customer_id` AS `CustomerId` FROM `Contacts` AS `cnt` \nINNER JOIN `Customer` AS `cust` ON `cust`.`Id` = `cnt`.`contact_customer_id` WHERE `cnt`.`Age` > 21",
                new MySqlCompiler());

            query.AssertSql(
                "SELECT cnt.FirstName, \"cnt\".\"contact_customer_id\" AS \"CustomerId\" FROM \"Contacts\" AS \"cnt\" \nINNER JOIN \"Customer\" AS \"cust\" ON \"cust\".\"Id\" = \"cnt\".\"contact_customer_id\" WHERE \"cnt\".\"Age\" > 21",
                new PostgresCompiler());
        }
    }
}
