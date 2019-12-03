using COFRS.Rql;
using System.Collections.Generic;

namespace COFRS.SqlServer
{
	/// <summary>
	/// Utilties for RQL
	/// </summary>
	internal static class RqlUtilities
	{

		/// <summary>
		/// Extract a Clause from the compiled RQL Query
		/// </summary>
		/// <param name="type"></param>
		/// <param name="node"></param>
		/// <returns></returns>
		public static RqlNode ExtractClause(RqlNodeType type, RqlNode node)
		{
			if (node == null)
				return null;

			if (node.NodeType == RqlNodeType.AND)
			{
				foreach (var child in node.Value<List<RqlNode>>())
				{
					var clause = ExtractClause(type, child);

					if (clause != null)
						return clause;
				}
			}
			else if (node.NodeType == RqlNodeType.OR)
			{
				foreach (var child in node.Value<List<RqlNode>>())
				{
					var clause = ExtractClause(type, child);

					if (clause != null)
						return clause;
				}
			}
			else if (node.NodeType == type)
			{
				return node;
			}

			return null;
		}

		/// <summary>
		/// Replaces a clause with a new one in place
		/// </summary>
		/// <param name="type"></param>
		/// <param name="node"></param>
		/// <param name="replacement"></param>
		/// <returns></returns>
		public static RqlNode ReplaceClause(RqlNodeType type, RqlNode node, RqlNode replacement)
		{
			if (node.NodeType == RqlNodeType.AND)
			{
				for (int i = 0; i < node.Value<List<RqlNode>>().Count; i++)
				{
					node.SetValue<RqlNode>(i, ReplaceClause(type, node.Value<RqlNode>(i), replacement));
				}
			}
			else if (node.NodeType == RqlNodeType.OR)
			{
				for (int i = 0; i < node.Value<List<RqlNode>>().Count; i++)
				{
					node.SetValue<RqlNode>(i, ReplaceClause(type, node.Value<RqlNode>(i), replacement));
				}
			}
			else if (node.NodeType == type)
			{
				return replacement;
			}

			return node;
		}

		public static List<RqlNode> ExtractAggregates(RqlNode node)
		{
			if (node == null)
				return null;

			var result = new List<RqlNode>();

			if (node.NodeType == RqlNodeType.AND)
			{
				foreach (var child in node.Value<List<RqlNode>>())
				{
					result.AddRange(ExtractAggregates(child));
				}
			}
			else if (node.NodeType == RqlNodeType.OR)
			{
				foreach (var child in node.Value<List<RqlNode>>())
				{
					result.AddRange(ExtractAggregates(child));
				}
			}
			else if (node.NodeType == RqlNodeType.SUM)
			{
				return new List<RqlNode>() { node };
			}
			else if (node.NodeType == RqlNodeType.MIN)
			{
				return new List<RqlNode>() { node };
			}
			else if (node.NodeType == RqlNodeType.MAX)
			{
				return new List<RqlNode>() { node };
			}
			else if (node.NodeType == RqlNodeType.MEAN)
			{
				return new List<RqlNode>() { node };
			}

			return result;
		}
	}
}
