using Newtonsoft.Json;
using System.Collections;
using System.Linq.Expressions;
using Xunit.Sdk;

namespace FluentSqlKata.Tests.Helpers
{
    public static class AssertHelper
    {
        /// <summary>
        /// Check whether all elements are included into the collection. Order does not matter.
        /// </summary>
        public static void CollectionContainsAll<T>(IEnumerable<T> collectionToTest, params Expression<Func<T, bool>>[] inspectors)
        {
            var expectedLength = inspectors.Count();
            var actualLength = collectionToTest.Count();

            if (expectedLength != actualLength)
                throw CollectionException.ForMismatchedItemCount(expectedLength, actualLength, JsonConvert.SerializeObject(collectionToTest));

            var all = new List<T>(collectionToTest);

            foreach (var inspector in inspectors)
            {
                var foundElement = all.Where(inspector.Compile()).ToArray();

                if (foundElement.Length == 0)
                    throw ContainsException.ForCollectionItemNotFound(inspector.ToStringExpression(), JsonConvert.SerializeObject(collectionToTest));

                all.Remove(foundElement[0]);
            }

            if (all.Any())
                throw ContainsException.ForCollectionFilterNotMatched(JsonConvert.SerializeObject(all));
        }

        /// <summary>
        /// Check whether all elements are included into the collection. Order does matter.
        /// </summary>
        public static void CollectionContainsOrdered<T>(IEnumerable<T> collectionToTest, params Expression<Func<T, bool>>[] inspectors)
        {
            var expectedLength = inspectors.Count();
            var actualLength = collectionToTest.Count();

            if (expectedLength != actualLength)
                throw CollectionException.ForMismatchedItemCount(expectedLength, actualLength, JsonConvert.SerializeObject(collectionToTest));

            for (int i = 0; i < expectedLength; i++)
            {
                if (!collectionToTest.Skip(i).Take(1).All(inspectors[i].Compile()))
                    throw ContainsException.ForCollectionItemNotFound(inspectors[i].ToStringExpression(), JsonConvert.SerializeObject(collectionToTest));
            }
        }

        /// <summary>
        /// Check whether any elements are contained into the collection. Order does not matter.
        /// </summary>
        public static void CollectionContainsAny<T>(IEnumerable<T> collectionToTest, params Expression<Func<T, bool>>[] inspectors)
        {
            foreach (var inspector in inspectors)
            {
                var foundElement = collectionToTest.Where(inspector.Compile()).ToArray();

                if (foundElement.Length == 0)
                    throw ContainsException.ForCollectionItemNotFound(inspector.ToStringExpression(), JsonConvert.SerializeObject(collectionToTest));
            }
        }

        /// <summary>
        /// Check whether any elements are not contained into the collection.
        /// </summary>
        public static void CollectionDoesNotContain<T>(IEnumerable<T> collectionToTest, params Expression<Func<T, bool>>[] inspectors)
        {
            foreach (var (inspector, index) in inspectors.WithIndex())
            {
                var foundElement = collectionToTest.Where(inspector.Compile()).ToArray();
                if (foundElement.Length > 0)
                    throw DoesNotContainException.ForCollectionItemFound(inspector.ToStringExpression(), index, null, JsonConvert.SerializeObject(collectionToTest));
            }
        }

        public static string ToStringExpression<T>(this Expression<Func<T, bool>> exp)
        {
            string expBody = ((LambdaExpression)exp).Body.ToString();
            // Gives: ((x.Id > 5) AndAlso (x.Warranty != False))

            var paramName = exp.Parameters[0].Name;
            var paramTypeName = exp.Parameters[0].Type.Name;

            // You could easily add "OrElse" and others...
            expBody = expBody.Replace(paramName + ".", paramTypeName + ".")
                             .Replace("AndAlso", "&&");

            return expBody;
        }

        public static void NullOrEmpty(IEnumerable array)
        {
            if (array == null)
                Assert.Null(array);
            else
                Assert.Empty(array);
        }

        public static void NotNullAndEmpty(IEnumerable array)
        {
            Assert.NotNull(array);
            Assert.NotEmpty(array);
        }

        public static IEnumerable<(T item, int index)> WithIndex<T>(this IEnumerable<T> source)
        {
            return source.Select((item, index) => (item, index));
        }
    }
}
