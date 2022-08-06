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
                throw new CollectionException(collectionToTest, expectedLength, actualLength);

            var all = new List<T>(collectionToTest);

            foreach (var inspector in inspectors)
            {
                var foundElement = all.Where(inspector.Compile()).ToArray();

                if (foundElement.Length == 0)
                    throw new Exception("Element not found: " + inspector.ToStringExpression());

                if (foundElement.Length > 1)
                    throw new Exception("More than one element found: " + inspector.ToStringExpression());

                all.Remove(foundElement[0]);
            }

            if (all.Any())
                throw new Exception($"Not all elements are matched. {all.Count} elements remains.");
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
                    throw new Exception("Element not found: " + inspector.ToStringExpression());

                if (foundElement.Length > 1)
                    throw new Exception("More than one element found: " + inspector.ToStringExpression());
            }
        }

        /// <summary>
        /// Check whether any elements are not contained into the collection.
        /// </summary>
        public static void CollectionDoesNotContain<T>(IEnumerable<T> collectionToTest, Expression<Func<T, bool>> inspector)
        {
            var foundElement = collectionToTest.Where(inspector.Compile()).ToArray();
            if (foundElement.Length > 0)
                throw new DoesNotContainException(inspector.ToStringExpression(), foundElement[0]);
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

    }
}
