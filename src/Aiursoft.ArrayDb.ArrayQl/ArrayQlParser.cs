using System.Dynamic;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic.Simplify;
using Microsoft.CSharp.RuntimeBinder;

namespace Aiursoft.ArrayDb.ArrayQl;

public class ArrayQlParser
{
    // Cache compiled queries for better performance
    private readonly Dictionary<string, Func<IDynamicObjectBucket, object>> _compiledQueries = new();

    /// <summary>
    /// Executes an ArrayQL query against the provided bucket
    /// </summary>
    /// <param name="query">The ArrayQL query string</param>
    /// <param name="bucket">The data source</param>
    /// <returns>Query results as an enumerable collection</returns>
    public IEnumerable<dynamic> Run(string query, IDynamicObjectBucket bucket)
    {
        // Get or compile the query
        if (!_compiledQueries.TryGetValue(query, out var compiledQuery))
        {
            compiledQuery = CompileQuery(query);
            _compiledQueries[query] = compiledQuery;
        }

        // Execute the query and process the result
        var result = compiledQuery(bucket);

        // Handle different return types
        if (result is IEnumerable<dynamic> dynamicEnumerable)
        {
            return dynamicEnumerable;
        }
        if (result is IEnumerable<object> objectEnumerable)
        {
            return objectEnumerable;
        }

        // Convert single result to collection
        return [result];
    }

    private Func<IDynamicObjectBucket, object> CompileQuery(string query)
    {
        // Validate the query for safety
        ValidateQuery(query);

        // Create code that will execute the query
        var code = $@"
using System;
using System.Collections.Generic;
using System.Linq;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;

namespace ArrayQl.Dynamic
{{
    public static class QueryExecutor
    {{
        public static object Execute(IDynamicObjectBucket bucket)
        {{
            var source = bucket.AsSimplified();
            return {query};
        }}
    }}
}}";

        // Compile the code
        var syntaxTree = CSharpSyntaxTree.ParseText(code);
        MetadataReference[] references =
        [
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(SimplifiedBucketItem).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IEnumerable<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IDynamicObjectBucket).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(DynamicObjectBucket).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(DynamicObject).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(CSharpArgumentInfo).Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.GetExecutingAssembly().Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Collections").Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Linq.Expressions").Location),
        ];

        var compilation = CSharpCompilation.Create("ArrayQl.Dynamic")
            .WithOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        using var ms = new MemoryStream();
        var result = compilation.Emit(ms);

        if (!result.Success)
        {
            var errors = result.Diagnostics
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .Select(d => d.GetMessage());
            throw new InvalidOperationException(
                $"Query compilation failed: {string.Join(Environment.NewLine, errors)}");
        }

        // Load the assembly and create a delegate
        ms.Seek(0, SeekOrigin.Begin);
        var assembly = Assembly.Load(ms.ToArray());
        var type = assembly.GetType("ArrayQl.Dynamic.QueryExecutor");
        var method = type!.GetMethod("Execute");

        return bucket => method!.Invoke(null, [ bucket ])!;
    }

    private void ValidateQuery(string query)
    {
        // Parse the query into a syntax tree
        var syntaxTree = CSharpSyntaxTree.ParseText(query);
        var root = syntaxTree.GetCompilationUnitRoot();

        // Use a syntax visitor to check for unsafe operations
        var visitor = new UnsafeOperationVisitor();
        visitor.Visit(root);

        if (visitor.HasUnsafeOperations)
        {
            throw new InvalidOperationException("The query contains unsafe operations");
        }
    }

    /// <summary>
    /// Syntax walker that checks for unsafe operations in the query
    /// </summary>
    private class UnsafeOperationVisitor : CSharpSyntaxWalker
    {
        private static readonly HashSet<string> AllowedMethods =
        [
            "Where", "Select", "GroupBy", "OrderBy", "OrderByDescending",
            "ThenBy", "ThenByDescending", "Skip", "Take", "Count", "Sum",
            "Min", "Max", "Average", "Any", "All", "First", "FirstOrDefault",
            "Last", "LastOrDefault", "Single", "SingleOrDefault", "ToArray",
            "ToList", "Distinct"
        ];

        public bool HasUnsafeOperations { get; private set; }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            // Check if method is allowed
            if (node.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;
                if (!AllowedMethods.Contains(methodName))
                {
                    HasUnsafeOperations = true;
                }
            }

            base.VisitInvocationExpression(node);
        }

        public override void VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            // Only allow anonymous objects
            if (!node.Type.ToString().Equals("var") &&
                !node.Type.ToString().Equals("anonymous") &&
                !node.Type.ToString().StartsWith("AnonymousType"))
            {
                HasUnsafeOperations = true;
            }

            base.VisitObjectCreationExpression(node);
        }

        // Block other unsafe operations
        public override void VisitMethodDeclaration(MethodDeclarationSyntax node)
        {
            HasUnsafeOperations = true;
            base.VisitMethodDeclaration(node);
        }

        public override void VisitClassDeclaration(ClassDeclarationSyntax node)
        {
            HasUnsafeOperations = true;
            base.VisitClassDeclaration(node);
        }

        public override void VisitAssignmentExpression(AssignmentExpressionSyntax node)
        {
            // Disallow assignments except in lambda expressions
            if (!(node.Parent is LambdaExpressionSyntax))
            {
                HasUnsafeOperations = true;
            }

            base.VisitAssignmentExpression(node);
        }
    }
}
