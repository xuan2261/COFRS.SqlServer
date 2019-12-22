using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;
using System.Net;
using System.Data.SqlClient;
using System.Data;
using COFRS.Rql;

namespace COFRS.SqlServer
{
	/// <summary>
	/// Emitter class - returns the generated SQL statement
	/// </summary>
	public static class Emitter
	{
		/// <summary>
		/// Updates an item in the datastore
		/// </summary>
		/// <typeparam name="T">The type of item to update</typeparam>
		/// <param name="item">The item to update</param>
		/// <param name="parameters">The list of SQL Parameters needed to execute the SQL statement</param>
		/// <returns></returns>
		public static string BuildUpdateQuery<T>(T item, List<SqlParameter> parameters)
		{
			var properties = typeof(T).GetProperties();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>();
			var sql = new StringBuilder();

			List<RqlNode> keyNodes = new List<RqlNode>();

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					if (string.Equals(tableName, tableAttribute.Name, StringComparison.InvariantCulture))
					{
						if (memberAttribute.IsPrimaryKey)
						{
							keyNodes.Add(new RqlNode(RqlNodeType.EQ, new List<object>() { columnName, property.GetValue(item) }));
						}
					}
				}
			}

			var node = new RqlNode(RqlNodeType.AND, keyNodes);
			string whereClause = ParseWhereClause<T>(node, null, parameters);

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.AppendLine($"UPDATE [{tableAttribute.Name}]");
			else
				sql.AppendLine($"UPDATE [{tableAttribute.Schema}].[{tableAttribute.Name}]");

			bool first = true;

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					if (string.Equals(tableName, tableAttribute.Name, StringComparison.InvariantCulture))
					{
						if (!memberAttribute.IsPrimaryKey)
						{
							var parameterName = $"@P{parameters.Count}";
							parameters.Add(BuildSqlParameter(parameterName, property, property.GetValue(item) ?? DBNull.Value));

							if (first)
							{
								sql.Append($" SET [{columnName}] = {parameterName}");
								first = false;
							}
							else
							{
								sql.AppendLine(",");
								sql.Append($"     [{columnName}] = {parameterName}");
							}
						}
					}
				}
			}

			sql.AppendLine();
			sql.Append("WHERE ");
			sql.Append(whereClause);

			return sql.ToString();
		}

		/// <summary>
		/// Builds a query to delete an object from the datastore using the specfied keys
		/// </summary>
		/// <typeparam name="T">The type of item to delete</typeparam>
		/// <param name="keys">The list of keys used to identify the items to be deleted</param>
		/// <param name="parameters">The list of SQL parameters that must be bound to execute the SQL statement</param>
		/// <returns></returns>
		public static string BuildDeleteQuery<T>(IEnumerable<KeyValuePair<string, object>> keys, List<SqlParameter> parameters)
		{
			var properties = typeof(T).GetProperties();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>();
			var sql = new StringBuilder();
			string whereClause = string.Empty;

			if (keys.ToList().Count > 0)
			{
				List<RqlNode> keyNodes = new List<RqlNode>();

				foreach (var key in keys)
				{
					var property = typeof(T).GetProperties().FirstOrDefault(p => string.Equals(p.Name, key.Key, StringComparison.OrdinalIgnoreCase));

					if (property != null)
					{
						var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

						if (memberAttribute != null)
						{
							var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
							var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

							if (string.Equals(tableName, tableAttribute.Name, StringComparison.InvariantCulture))
							{
								keyNodes.Add(new RqlNode(RqlNodeType.EQ, new List<object>() { columnName, key.Value }));
							}
							else
								throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("bad_request", $"key field {key.Key} is not a member of {tableAttribute.Name}"));
						}
						else
							throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("bad_request", $"key field {key.Key} is not a member of {tableAttribute.Name}"));
					}
					else
						throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("bad_request", $"key field {key.Key} is not a member of {tableAttribute.Name}"));
				}

				var node = new RqlNode(RqlNodeType.AND, keyNodes);
				
				whereClause = ParseWhereClause<T>(node, null, parameters);
			}

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.AppendLine($"DELETE FROM [{tableAttribute.Name}]");
			else
				sql.AppendLine($"DELETE FROM [{tableAttribute.Schema}].[{tableAttribute.Name}]");

			if (!string.IsNullOrWhiteSpace(whereClause))
				sql.AppendLine($"WHERE {whereClause}");

			return sql.ToString();
		}

		/// <summary>
		/// Builds the SQL query to add an item to the datastore
		/// </summary>
		/// <typeparam name="T">The type of item to be added</typeparam>
		/// <param name="item">The item to be added</param>
		/// <param name="parameters">The list of SQL parameters that must be bound to execute the SQL statement</param>
		/// <param name="identityProperty"></param>
		/// <returns></returns>
		public static string BuildAddQuery<T>(T item, List<SqlParameter> parameters, out PropertyInfo identityProperty)
		{
			var sql = new StringBuilder();
			var properties = typeof(T).GetProperties();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>();
			var containsIdentity = false;

			identityProperty = null;

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.Append($"INSERT INTO [{tableAttribute.Name}]\r\n            (");
			else
				sql.Append($"INSERT INTO [{tableAttribute.Schema}].[{tableAttribute.Name}]\r\n            (");

			bool firstField = true;

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					bool includeField = true;

					//	If the member is an identity column, then SQL will assign the value,
					//	do not include it in the list of columns to insert
					if (memberAttribute.IsIdentity)
					{
						containsIdentity = true;
						includeField = false;
						identityProperty = property;
					}

					//	Only columns that belong to the main table are inserted
					if ( !string.Equals(tableName, tableAttribute.Name, StringComparison.InvariantCulture))
						includeField = false;

					if (includeField)
					{
						if (firstField)
						{
							sql.Append($"[{columnName}]");
							firstField = false;
						}
						else
						{
							sql.Append($", [{columnName}]");
						}
					}
				}
			}

			sql.AppendLine(")");
			if (containsIdentity)
			{
				foreach (var property in properties)
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
						var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

						if (string.Compare(tableName, tableAttribute.Name, true) == 0)
							if (memberAttribute.IsIdentity)
								sql.AppendLine($" OUTPUT inserted.[{columnName}]");
					}
				}
			}

			sql.Append(" VALUES (");
			firstField = true;

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

					bool includeField = true;

					//	Don't include identity columns
					if (memberAttribute.IsIdentity)
						includeField = false;

					//	Don't include foreign columns
					if (string.Compare(tableName, tableAttribute.Name, true) != 0)
						includeField = false;

					if (includeField)
					{
						var parameterName = $"@P{parameters.Count}";
						parameters.Add(BuildSqlParameter(parameterName, property, property.GetValue(item)));

						if (firstField)
						{
							sql.Append($"{parameterName}");
							firstField = false;
						}
						else
						{
							sql.Append($", {parameterName}");
						}
					}
				}
			}

			sql.Append(")");

			return sql.ToString();
		}

		/// <summary>
		/// Builds the SQL query for a single result
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="keyList"></param>
		/// <param name="node"></param>
		/// <param name="parameters">The list of SQL parameters that must be bound to execute the SQL statement</param>
		/// <returns></returns>
		public static string BuildSingleQuery<T>(IEnumerable<KeyValuePair<string, object>> keyList, RqlNode node, List<SqlParameter> parameters)
		{
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause<T>(node, null, parameters);
			var orderByClause = ParseOrderByClause<T>(node);
			var selectFields = RqlUtilities.ExtractClause(RqlNodeType.SELECT, node);
			var tableAttribute = typeof(T).GetCustomAttribute<Table>();
			var joinAttributes = typeof(T).GetCustomAttributes<Join>(false);
			var joinConditions = typeof(T).GetCustomAttributes<JoinCondition>(false);
			var properties = typeof(T).GetProperties();
			bool firstField = true;

			sql.Append("SELECT ");

			var aggregates = RqlUtilities.ExtractAggregates(node);

			if (aggregates != null && aggregates.Count > 0)
			{
				bool first = true;
				foreach (var aggregate in aggregates)
				{
					if (first)
					{
						first = false;

						if (aggregate.NodeType == RqlNodeType.SUM)
						{
							sql.Append($"SUM({aggregate.Value<string>()})");
						}
						else if (aggregate.NodeType == RqlNodeType.MAX)
						{
							sql.Append($"MAX({aggregate.Value<string>()})");
						}
						else if (aggregate.NodeType == RqlNodeType.MIN)
						{
							sql.Append($"MIN({aggregate.Value<string>()})");
						}
						else if (aggregate.NodeType == RqlNodeType.MEAN)
						{
							sql.Append($"AVG({aggregate.Value<string>()})");
						}
					}
					else
					{
						sql.AppendLine(",");

						if (aggregate.NodeType == RqlNodeType.SUM)
						{
							sql.Append($"       SUM({aggregate.Value<string>()})");
						}
						else if (aggregate.NodeType == RqlNodeType.MAX)
						{
							sql.Append($"       MAX({aggregate.Value<string>()})");
						}
						else if (aggregate.NodeType == RqlNodeType.MIN)
						{
							sql.Append($"       MIN({aggregate.Value<string>()})");
						}
						else if (aggregate.NodeType == RqlNodeType.MEAN)
						{
							sql.Append($"       AVG({aggregate.Value<string>()})");
						}
					}
				}

				sql.AppendLine();
			}
			else
			{
				if (RqlUtilities.ExtractClause(RqlNodeType.DISTINCT, node) != null)
				{
					sql.Append("DISTINCT ");
				}

				if (RqlUtilities.ExtractClause(RqlNodeType.FIRST, node) != null)
				{
					sql.Append("TOP 1 ");
				}

				foreach (var property in properties)
				{
					bool includeField = true;
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						if (selectFields != null &&     //	we have a list 
							selectFields.Value<List<string>>().Count > 0 && //	and it has at least one entries in it.
																			//  and this field is not in the list.
							selectFields.Value<List<string>>().FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.OrdinalIgnoreCase)) == null)
						{
							//	Since the field is not in the list, by default we don't include it in the result set.
							includeField = false;

							//	Or, if it is the primary key, we do
							if (memberAttribute.IsPrimaryKey)
								includeField = true;
						}

						if (includeField)
						{
							var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
							var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
							var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

							if (firstField)
							{
								firstField = false;

								if (string.IsNullOrWhiteSpace(schema))
								{
									if (string.Compare(columnName, property.Name, true) == 0)
										sql.Append($"[{tableName}].[{property.Name}]");
									else
										sql.Append($"[{tableName}].[{columnName}] as [{property.Name}]");
								}
								else
								{
									if (string.Compare(columnName, property.Name, true) == 0)
										sql.Append($"[{schema}].[{tableName}].[{property.Name}]");
									else
										sql.Append($"[{schema}].[{tableName}].[{columnName}] as [{property.Name}]");
								}
							}
							else
							{
								if (string.IsNullOrWhiteSpace(schema))
								{
									if (string.Compare(columnName, property.Name, true) == 0)
										sql.Append($",\r\n       [{tableName}].[{property.Name}]");
									else
										sql.Append($",\r\n       [{tableName}].[{columnName}] as [{property.Name}]");
								}
								else
								{
									if (string.Compare(columnName, property.Name, true) == 0)
										sql.Append($",\r\n       [{schema}].[{tableName}].[{property.Name}]");
									else
										sql.Append($",\r\n       [{schema}].[{tableName}].[{columnName}] as [{property.Name}]");
								}
							}
						}
					}
				}
			}

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.Append($"\r\n  FROM [{tableAttribute.Name}] WITH(NOLOCK)");
			else
				sql.Append($"\r\n  FROM [{tableAttribute.Schema}].[{tableAttribute.Name}] WITH(NOLOCK)");

			foreach (var joinAttribute in joinAttributes)
			{
				var currentJoinConditons = joinConditions.Where(x => (string.Compare(x.Schema, joinAttribute.Schema) == 0) && (string.Compare(x.TableName, joinAttribute.TableName) == 0));

				switch (joinAttribute.JoinType)
				{
					case JoinType.Inner:
						if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
							sql.Append($"\r\n INNER JOIN [{joinAttribute.TableName}] on ");
						else
							sql.Append($"\r\n INNER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
						AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
						break;

					case JoinType.LeftOuter:
						if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
							sql.Append($"\r\n  LEFT OUTER JOIN [{joinAttribute.TableName}] on ");
						else
							sql.Append($"\r\n  LEFT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
						AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
						break;

					case JoinType.RightOuter:
						if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
							sql.Append($"\r\n RIGHT OUTER JOIN [{joinAttribute.TableName}] on ");
						else
							sql.Append($"\r\n RIGHT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
						AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
						break;
				}
			}

			sql.Append("\r\n WHERE ");
			bool firstClause = true;

			foreach (var pair in keyList)
			{
				var property = properties.FirstOrDefault(x => x.Name.ToLower() == pair.Key.ToLower());
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);
				var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
				var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
				var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

				if (firstClause)
				{
					sql.Append("(");
					firstClause = false;
				}
				else
				{
					sql.Append(" AND (");
				}

				var parameterName = $"@P{parameters.Count}";
				parameters.Add(BuildSqlParameter(parameterName, property, pair.Value ?? DBNull.Value));

				if (string.IsNullOrWhiteSpace(schema))
					sql.Append($"\r\n [{tableName}].[{columnName}] = {parameterName}");
				else
					sql.Append($"\r\n [{schema}].[{tableName}].[{columnName}] = {parameterName}");

				sql.Append(")");
			}

			if (!string.IsNullOrWhiteSpace(whereClause))
			{
				sql.Append("\r\n  AND (");
				sql.Append(whereClause);
				sql.Append(")");
			}

			if (!string.IsNullOrWhiteSpace(orderByClause))
			{
				sql.Append("\r\n ORDER BY ");
				sql.Append(orderByClause);
			}

			return sql.ToString();
		}

		#region Helper Functions
		/// <summary>
		/// Builds the count query for the collection
		/// </summary>
		/// <param name="keyList"></param>
		/// <param name="node"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		internal static string BuildCollectionCountQuery<T>(IEnumerable<KeyValuePair<string, object>> keyList, RqlNode node, List<SqlParameter> parameters)
		{
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause<T>(node, null, parameters);

			var properties = typeof(T).GetProperties();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>(false);
			var joinAttributes = typeof(T).GetCustomAttributes<Join>(false);
			var joinConditions = typeof(T).GetCustomAttributes<JoinCondition>(false);

			sql.Append("SELECT ");

			if (RqlUtilities.ExtractClause(RqlNodeType.DISTINCT, node) != null)
			{
				sql.Append("DISTINCT ");
			}

			if (RqlUtilities.ExtractClause(RqlNodeType.FIRST, node) != null)
			{
				sql.Append("TOP 1 ");
			}

			sql.AppendLine("COUNT(*) as [RecordCount]");

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.AppendLine($"  FROM [{tableAttribute.Name}]");
			else
				sql.AppendLine($"  FROM [{tableAttribute.Schema}].[{tableAttribute.Name}]");

			foreach (var joinAttribute in joinAttributes)
			{
				var currentJoinConditons = joinConditions.Where(x => (string.Compare(x.Schema, joinAttribute.Schema) == 0) && (string.Compare(x.TableName, joinAttribute.TableName) == 0));

				switch (joinAttribute.JoinType)
				{
					case JoinType.Inner:
						if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
							sql.Append($" INNER JOIN [{joinAttribute.TableName}] on ");
						else
							sql.Append($" INNER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
						AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
						break;

					case JoinType.LeftOuter:
						if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
							sql.Append($"  LEFT OUTER JOIN [{joinAttribute.TableName}] on ");
						else
							sql.Append($"  LEFT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
						AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
						break;

					case JoinType.RightOuter:
						if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
							sql.Append($" RIGHT OUTER JOIN [{joinAttribute.TableName}] on ");
						else
							sql.Append($" RIGHT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
						AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
						break;
				}
			}

			bool firstClause = true;
			bool clausesAdded = false;

			foreach (var pair in keyList)
			{
				sql.Append(" WHERE ");
				clausesAdded = true;
				var property = properties.FirstOrDefault(x => x.Name.ToLower() == pair.Key.ToLower());
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);
				var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
				var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
				var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

				if (firstClause)
				{
					sql.Append("(");
					firstClause = false;
				}
				else
				{
					sql.Append(" AND (");
				}

				var parameterName = $"@P{parameters.Count}";
				parameters.Add(BuildSqlParameter(parameterName, property, pair.Value ?? DBNull.Value));

				if (string.IsNullOrWhiteSpace(schema))
					sql.Append($"\r\n [{tableName}].[{columnName}] = {parameterName}");
				else
					sql.Append($"\r\n [{schema}].[{tableName}].[{columnName}] = {parameterName}");

				sql.Append(")");
			}

			if (!string.IsNullOrWhiteSpace(whereClause))
			{
				if (clausesAdded)
					sql.Append("\r\n  AND (");
				else if (!string.IsNullOrWhiteSpace(whereClause))
					sql.Append(" WHERE ");

				sql.Append(whereClause);

				if (clausesAdded)
					sql.Append(")");
			}

			return sql.ToString();
		}

		/// <summary>
		/// Builds the SQL query for the collection
		/// </summary>
		/// <param name="keyList">The list of key value pairs that limit the scope of the collection</param>
		/// <param name="node">The compiled RQL query</param>
		/// <param name="count">The number of records in the collection</param>
		/// <param name="batchLimit">The maximum number of items that can be included in a collection batch</param>
		/// <param name="pageFilter">The page filter that applies to the collection</param>
		/// <param name="parameters">The parameters for the SQL query</param>
		/// <param name="NoPaging">Do not page results even if the result set exceeds the system defined limit. Default value = false.</param>
		/// <returns></returns>
		internal static string BuildCollectionListQuery<T>(IEnumerable<KeyValuePair<string, object>> keyList, RqlNode node, int count, int batchLimit, RqlNode pageFilter, List<SqlParameter> parameters, bool NoPaging)
		{
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause<T>(node, null, parameters);
			var orderByClause = ParseOrderByClause<T>(node);
			var selectFields = RqlUtilities.ExtractClause(RqlNodeType.SELECT, node);
			var options = ServiceFactory.Get<IRepositoryOptions>();

			var properties = typeof(T).GetProperties();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>(false);
			var joinAttributes = typeof(T).GetCustomAttributes<Join>(false);
			var joinConditions = typeof(T).GetCustomAttributes<JoinCondition>(false);

			if (NoPaging || (count < batchLimit && pageFilter == null))
			{
				bool firstField = true;

				sql.Append("SELECT ");

				if (RqlUtilities.ExtractClause(RqlNodeType.DISTINCT, node) != null)
				{
					sql.Append("DISTINCT ");
				}

				if (RqlUtilities.ExtractClause(RqlNodeType.FIRST, node) != null)
				{
					sql.Append("TOP 1 ");
				}

				foreach (var property in properties)
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
					var includeField = (memberAttribute == null) ? false : true;

					if (selectFields != null &&     //	we have a list 
						selectFields.Value<List<string>>().Count > 0 && //	and it has at least one entries in it.
																		//  and this field is not in the list.
						selectFields.Value<List<string>>().FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.OrdinalIgnoreCase)) == null)
					{
						//	Since the field is not in the list, by default we don't include it in the result set.
						includeField = false;
					}

					if (includeField)
					{
						var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
						var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
						var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

						if (firstField)
						{
							firstField = false;

							if (string.IsNullOrWhiteSpace(schema))
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($" [{tableName}].[{columnName}]");
								else
									sql.Append($" [{tableName}].[{columnName}] as [{property.Name}]");
							}
							else
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($" [{schema}].[{tableName}].[{columnName}]");
								else
									sql.Append($" [{schema}].[{tableName}].[{columnName}] as [{property.Name}]");
							}
						}
						else
						{
							sql.AppendLine(",");
							if (string.IsNullOrWhiteSpace(schema))
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($"       [{tableName}].[{columnName}]");
								else
									sql.Append($"       [{tableName}].[{columnName}] as [{property.Name}]");
							}
							else
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($"       [{schema}].[{tableName}].[{columnName}]");
								else
									sql.Append($"       [{schema}].[{tableName}].[{columnName}] as [{property.Name}]");
							}
						}
					}
				}

				sql.AppendLine();
				if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
					sql.AppendLine($"  FROM [{tableAttribute.Name}] WITH(NOLOCK)");
				else
					sql.AppendLine($"  FROM [{tableAttribute.Schema}].[{tableAttribute.Name}] WITH(NOLOCK)");

				foreach (var joinAttribute in joinAttributes)
				{
					var currentJoinConditons = joinConditions.Where(x => (string.Compare(x.Schema, joinAttribute.Schema) == 0) && (string.Compare(x.TableName, joinAttribute.TableName) == 0));

					switch (joinAttribute.JoinType)
					{
						case JoinType.Inner:
							if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
								sql.Append($"\r\n INNER JOIN [{joinAttribute.TableName}] on ");
							else
								sql.Append($"\r\n INNER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
							AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
							break;

						case JoinType.LeftOuter:
							if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
								sql.Append($"\r\n LEFT OUTER JOIN [{joinAttribute.TableName}] on ");
							else
								sql.Append($"\r\n LEFT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
							AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
							break;

						case JoinType.RightOuter:
							if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
								sql.Append($"\r\n RIGHT OUTER JOIN [{joinAttribute.TableName}] on ");
							else
								sql.Append($"\r\n RIGHT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
							AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
							break;
					}
				}

				bool firstClause = true;
				bool clausesAdded = false;

				foreach (var pair in keyList)
				{
					sql.Append(" WHERE ");
					clausesAdded = true;
					var property = properties.FirstOrDefault(x => x.Name.ToLower() == pair.Key.ToLower());
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);
					var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					if (firstClause)
					{
						sql.Append("(");
						firstClause = false;
					}
					else
					{
						sql.Append(" AND (");
					}

					var parameterName = $"@P{parameters.Count}";
					parameters.Add(BuildSqlParameter(parameterName, property, pair.Value ?? DBNull.Value));

					if (string.IsNullOrWhiteSpace(schema))
						sql.Append($"\r\n [{tableName}].[{columnName}] = {parameterName}");
					else
						sql.Append($"\r\n [{schema}].[{tableName}].[{columnName}] = {parameterName}");

					sql.Append(")");
				}

				if (!string.IsNullOrWhiteSpace(whereClause))
				{
					if (clausesAdded)
						sql.Append("\r\n  AND (");
					else if (!string.IsNullOrWhiteSpace(whereClause))
						sql.Append(" WHERE ");

					sql.Append(whereClause);

					if (clausesAdded)
						sql.Append(")");
				}

				if (!string.IsNullOrWhiteSpace(orderByClause))
				{
					sql.Append("\r\n ORDER BY ");
					sql.Append(orderByClause);
				}
			}
			else
			{
				bool firstField = true;

				sql.Append("SELECT ");

				if (RqlUtilities.ExtractClause(RqlNodeType.DISTINCT, node) != null)
				{
					sql.Append("DISTINCT ");
				}

				if (RqlUtilities.ExtractClause(RqlNodeType.FIRST, node) != null)
				{
					sql.Append("TOP 1 ");
				}

				foreach (var property in properties)
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						bool includeField = true;

						if (selectFields != null &&     //	we have a list 
							selectFields.Value<List<string>>().Count > 0 && //	and it has at least one entries in it.
																			//  and this field is not in the list.
							selectFields.Value<List<string>>().FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.OrdinalIgnoreCase)) == null)
						{
							//	Since the field is not in the list, by default we don't include it in the result set.
							includeField = false;

							//	Or, if it is the primary key, we do
							if (memberAttribute.IsPrimaryKey)
								includeField = true;
						}

						if (includeField)
						{
							var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

							if (firstField)
							{
								firstField = false;
								sql.Append($" [t0].[{columnName}]");
							}
							else
							{
								sql.AppendLine(",");
								sql.Append($"       [t0].[{columnName}]");
							}
						}
					}
				}

				sql.AppendLine();
				sql.AppendLine("  FROM (");
				sql.AppendLine("         SELECT ROW_NUMBER() OVER (");

				if (string.IsNullOrWhiteSpace(orderByClause))
				{
					var primaryKeyProperties = properties.Where(x => x.GetCustomAttribute<MemberAttribute>() != null && x.GetCustomAttribute<MemberAttribute>().IsPrimaryKey);

					if (primaryKeyProperties.Count() > 1)
					{
						sql.Append(" ORDER BY ");

						bool firstComponent = true;
						foreach (var composite in primaryKeyProperties)
						{
							var tableName = tableAttribute.Name;
							var schema = tableAttribute.Schema;
							var memberAttribute = composite.GetCustomAttribute<MemberAttribute>();
							var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? composite.Name : memberAttribute.ColumnName;

							if (firstComponent)
							{
								if (string.IsNullOrWhiteSpace(schema))
									sql.Append($"[{tableName}].[{columnName}]");
								else
									sql.Append($"[{schema}].[{tableName}].[{columnName}]");
								firstComponent = false;
							}
							else
							{
								if (string.IsNullOrWhiteSpace(schema))
									sql.Append($", [{tableName}].[{columnName}]");
								else
									sql.Append($", [{schema}].[{tableName}].[{columnName}]");
							}
						}
					}
					else if (primaryKeyProperties.Count() > 0)
					{
						var primaryKeyProperty = properties.FirstOrDefault(x => x.GetCustomAttribute<MemberAttribute>() != null && x.GetCustomAttribute<MemberAttribute>().IsPrimaryKey);

						var tableName = primaryKeyProperty != null ? (string.IsNullOrWhiteSpace(primaryKeyProperty.GetCustomAttribute<MemberAttribute>().TableName) ? tableAttribute.Name : primaryKeyProperty.GetCustomAttribute<MemberAttribute>().TableName) : tableAttribute.Name;
						var schema = primaryKeyProperty != null ? (string.IsNullOrWhiteSpace(primaryKeyProperty.GetCustomAttribute<MemberAttribute>().Schema) ? tableAttribute.Schema : primaryKeyProperty.GetCustomAttribute<MemberAttribute>().Schema) : tableAttribute.Schema;
						var columnName = string.IsNullOrWhiteSpace(primaryKeyProperty.GetCustomAttribute<MemberAttribute>().ColumnName) ? primaryKeyProperty.Name : primaryKeyProperty.GetCustomAttribute<MemberAttribute>().ColumnName;

						if (string.IsNullOrWhiteSpace(schema))
							sql.Append($" ORDER BY [{tableName}].[{columnName}] asc ");
						else
							sql.Append($" ORDER BY [{schema}].[{tableName}].[{columnName}] asc ");
					}
				}
				else
				{
					sql.Append(" ORDER BY ");
					sql.Append(orderByClause);
				}

				sql.AppendLine(") as [ROW_NUMBER],");
				firstField = true;

				foreach (var property in properties)
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
					var includeField = (memberAttribute == null) ? false : true;

					if (selectFields != null &&     //	we have a list 
						selectFields.Value<List<string>>().Count > 0 && //	and it has at least one entries in it.
																		//  and this field is not in the list.
						selectFields.Value<List<string>>().FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.OrdinalIgnoreCase)) == null)
					{
						//	Since the field is not in the list, by default we don't include it in the result set.
						includeField = false;

						//	Or, if it is the primary key, we do
						if (memberAttribute.IsPrimaryKey)
							includeField = true;
					}

					if (includeField)
					{
						var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
						var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
						var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

						if (firstField)
						{
							firstField = false;

							if (string.IsNullOrWhiteSpace(schema))
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($"                [{tableName}].[{columnName}]");
								else
									sql.Append($"                [{tableName}].[{columnName}] as [{property.Name}]");
							}
							else
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($"                [{schema}].[{tableName}].[{columnName}]");
								else
									sql.Append($"                [{schema}].[{tableName}].[{columnName}] as [{property.Name}]");
							}
						}
						else
						{
							if (string.IsNullOrWhiteSpace(schema))
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($",\r\n                [{tableName}].[{columnName}]");
								else
									sql.Append($",\r\n                [{tableName}].[{columnName}] as [{property.Name}]");
							}
							else
							{
								if (string.Compare(columnName, property.Name, true) == 0)
									sql.Append($",\r\n                [{schema}].[{tableName}].[{columnName}]");
								else
									sql.Append($",\r\n                [{schema}].[{tableName}].[{columnName}] as [{property.Name}]");
							}
						}
					}
				}

				if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
					sql.Append($"\r\n           FROM [{tableAttribute.Name}] WITH(NOLOCK)");
				else
					sql.Append($"\r\n           FROM [{tableAttribute.Schema}].[{tableAttribute.Name}] WITH(NOLOCK) ");

				foreach (var joinAttribute in joinAttributes)
				{
					var currentJoinConditons = joinConditions.Where(x => (string.Compare(x.Schema, joinAttribute.Schema) == 0) && (string.Compare(x.TableName, joinAttribute.TableName) == 0));

					switch (joinAttribute.JoinType)
					{
						case JoinType.Inner:
							if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
								sql.Append($"\r\n INNER JOIN [{joinAttribute.TableName}] on ");
							else
								sql.Append($"\r\n INNER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
							AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
							break;

						case JoinType.LeftOuter:
							if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
								sql.Append($"\r\n LEFT OUTER JOIN [{joinAttribute.TableName}] on ");
							else
								sql.Append($"\r\n LEFT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
							AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
							break;

						case JoinType.RightOuter:
							if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
								sql.Append($"\r\n RIGHT OUTER JOIN [{joinAttribute.TableName}] on ");
							else
								sql.Append($"\r\n RIGHT OUTER JOIN [{joinAttribute.Schema}].[{joinAttribute.TableName}] on ");
							AddJoinConditions<T>(sql, tableAttribute, joinAttribute, currentJoinConditons, parameters);
							break;
					}
				}

				bool firstClause = true;
				bool clausesAdded = false;

				foreach (var pair in keyList)
				{
					sql.Append(" WHERE ");
					clausesAdded = true;
					var property = properties.FirstOrDefault(x => x.Name.ToLower() == pair.Key.ToLower());
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);
					var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					if (firstClause)
					{
						sql.Append("(");
						firstClause = false;
					}
					else
					{
						sql.Append(" AND (");
					}

					var parameterName = $"@P{parameters.Count}";
					parameters.Add(BuildSqlParameter(parameterName, property, pair.Value ?? DBNull.Value));

					if (string.IsNullOrWhiteSpace(schema))
						sql.Append($"\r\n [{tableName}].[{columnName}] = {parameterName}");
					else
						sql.Append($"\r\n [{schema}].[{tableName}].[{columnName}] = {parameterName}");

					sql.Append(")");
				}

				if (!string.IsNullOrWhiteSpace(whereClause))
				{
					if (clausesAdded)
						sql.Append("\r\n  AND (");
					else if (!string.IsNullOrWhiteSpace(whereClause))
						sql.Append(" WHERE ");

					sql.Append(whereClause);

					if (clausesAdded)
						sql.Append(")");
				}

				int start = 1;

				if (pageFilter != null)
				{
					start = pageFilter.Value<int>(0);
					count = pageFilter.Value<int>(1);
				}

				if (count > options.BatchLimit)
					count = options.BatchLimit;

				sql.AppendLine($") as [t0]");
				sql.AppendLine($" WHERE [t0].[ROW_NUMBER] BETWEEN {start} AND {start + count - 1}");
				sql.AppendLine($" ORDER BY [t0].[ROW_NUMBER]");
			}

			return sql.ToString();
		}

		/// <summary>
		/// Add join conditions
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="tableAttribute"></param>
		/// <param name="joinAttribute"></param>
		/// <param name="currentJoinConditons"></param>
		/// <param name="parameters"></param>
		private static void AddJoinConditions<T>(StringBuilder sql, Table tableAttribute, Join joinAttribute, IEnumerable<JoinCondition> currentJoinConditons, List<SqlParameter> parameters)
		{
			bool first = true;

			foreach (var joinCondition in currentJoinConditons)
			{
				if (first)
				{
					first = false;

					switch (joinCondition.JoinOperation)
					{
						case JoinOperator.AND:
						case JoinOperator.OR:
							break;

						case JoinOperator.BEGINGROUP:
							sql.Append("(");
							first = true;
							break;

						case JoinOperator.ENDGROUP:
							sql.Append(")");
							break;
					}
				}
				else
				{
					switch (joinCondition.JoinOperation)
					{
						case JoinOperator.AND:
							sql.Append(" AND ");
							break;

						case JoinOperator.OR:
							sql.Append(" OR ");
							break;

						case JoinOperator.BEGINGROUP:
							sql.Append("(");
							first = true;
							break;

						case JoinOperator.ENDGROUP:
							sql.Append(")");
							break;
					}
				}

				if (joinCondition.JoinOperation != JoinOperator.BEGINGROUP && joinCondition.JoinOperation != JoinOperator.ENDGROUP)
				{
					if (!string.IsNullOrWhiteSpace(joinCondition.SourceField))
					{
						if (string.IsNullOrWhiteSpace(joinAttribute.Schema))
							sql.Append($"[{joinAttribute.TableName}].[{joinCondition.SourceField}]");
						else
							sql.Append($"[{joinAttribute.Schema}].[{joinAttribute.TableName}].[{joinCondition.SourceField}]");

						switch (joinCondition.Operation)
						{
							case JoinComparrisonOperator.eq:
								sql.Append(" = ");
								break;

							case JoinComparrisonOperator.ge:
								sql.Append(" >= ");
								break;

							case JoinComparrisonOperator.gt:
								sql.Append(" > ");
								break;

							case JoinComparrisonOperator.le:
								sql.Append(" <= ");
								break;

							case JoinComparrisonOperator.lt:
								sql.Append(" < ");
								break;

							case JoinComparrisonOperator.ne:
								sql.Append(" <> ");
								break;
						}

						if (joinCondition.ReferenceLiteral == null)
						{
							if (string.IsNullOrWhiteSpace(joinCondition.ReferenceTable))
							{
								if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
									sql.Append($"[{tableAttribute.Name}].[{joinCondition.ReferenceField}]");
								else
									sql.Append($"[{tableAttribute.Schema}].[{tableAttribute.Name}].[{joinCondition.ReferenceField}]");
							}
							else
							{
								if (string.IsNullOrWhiteSpace(joinCondition.ReferenceSchema))
									sql.Append($"[{joinCondition.ReferenceTable}].[{joinCondition.ReferenceField}]");
								else
									sql.Append($"[{joinCondition.ReferenceSchema}].[{joinCondition.ReferenceTable}].[{joinCondition.ReferenceField}]");
							}
						}
						else
						{
							var property = GetProperty<T>(joinCondition.ReferenceField);

							if (joinCondition.ReferenceLiteral.GetType() == typeof(string))
							{
								if (string.Equals(joinCondition.ReferenceLiteral.ToString(), "null", StringComparison.OrdinalIgnoreCase))
									sql.Append($"null");
								else
								{
									var parameterName = $"@P{parameters.Count}";
									parameters.Add(BuildSqlParameter(parameterName, property, joinCondition.ReferenceLiteral));
									sql.Append(parameterName);
								}
							}
							else
							{
								var parameterName = $"@P{parameters.Count}";
								parameters.Add(BuildSqlParameter(parameterName, property, joinCondition.ReferenceLiteral));
								sql.Append(parameterName);
							}
						}
					}
				}
			}

			sql.AppendLine();
		}

		/// <summary>
		/// Returns a formatted WHERE clause representation of the filters in the query string
		/// </summary>
		/// <typeparam name="T">The type of object from which the fields are being selected</typeparam>
		/// <param name="node">The RQL node representation of the query string</param>
		/// <param name="op"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		private static string ParseWhereClause<T>(RqlNode node, string op, List<SqlParameter> parameters)
		{
			var whereClause = new StringBuilder();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>(false);
			var properties = typeof(T).GetProperties();

			if (node == null)
				return string.Empty;

			switch (node.NodeType)
			{
				case RqlNodeType.AND:
					{
						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append($"(");

						bool first = true;

						foreach (RqlNode argument in node.Value<List<RqlNode>>())
						{
							var subClause = ParseWhereClause<T>(argument, "AND", parameters);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (first)
									first = false;
								else
									whereClause.Append(" AND ");

								whereClause.Append(subClause);
							}
						}

						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append(")");
					}
					break;

				case RqlNodeType.OR:
					{
						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append($"(");

						bool first = true;

						foreach (RqlNode argument in node.Value<List<RqlNode>>())
						{
							var subClause = ParseWhereClause<T>(argument, "OR", parameters);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (first)
									first = false;
								else
									whereClause.Append(" OR ");

								whereClause.Append(subClause);
							}
						}

						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append(")");
					}
					break;

				case RqlNodeType.GE:
					whereClause.Append(ConstructComparrisonOperator<T>(">=", node.Value<string>(0), node.Value<object>(1), parameters));
					break;

				case RqlNodeType.GT:
					whereClause.Append(ConstructComparrisonOperator<T>(">", node.Value<string>(0), node.Value<object>(1), parameters));
					break;

				case RqlNodeType.LE:
					whereClause.Append(ConstructComparrisonOperator<T>("<=", node.Value<string>(0), node.Value<object>(1), parameters));
					break;

				case RqlNodeType.LT:
					whereClause.Append(ConstructComparrisonOperator<T>("<", node.Value<string>(0), node.Value<object>(1), parameters));
					break;

				case RqlNodeType.EQ:
					whereClause.Append(ConstructComparrisonOperator<T>("=", node.Value<string>(0), node.Value<object>(1), parameters));
					break;

				case RqlNodeType.NE:
					whereClause.Append(ConstructComparrisonOperator<T>("<>", node.Value<string>(0), node.Value<object>(1), parameters));
					break;

				case RqlNodeType.IN:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.Value<string>(0), StringComparison.OrdinalIgnoreCase));
						
						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"([{tableName}].[{property.Name}] IN(");
								else
									whereClause.Append($"([{schema}].[{tableName}].[{property.Name}] IN(");

								for (int i = 1; i < node.Value<List<object>>().Count; i++)
								{
									if (i > 1)
										whereClause.Append(",");

									var parameterName = $"@P{parameters.Count}";
									parameters.Add(BuildSqlParameter(parameterName, property, node.Value<object>(i)));
									whereClause.Append(parameterName);
								}

								whereClause.Append("))");

							}
							else
							{
								throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
							}
						}
						else
						{
							throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
						}
					}
					break;

				case RqlNodeType.OUT:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.Value<string>(0), StringComparison.OrdinalIgnoreCase));

						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"([{tableName}].[{property.Name}] NOT IN(");
								else
									whereClause.Append($"([{schema}].[{tableName}].[{property.Name}] NOT IN(");

								for (int i = 1; i < node.Value<List<object>>().Count; i++)
								{
									if (i > 1)
										whereClause.Append(",");

									var parameterName = $"@P{parameters.Count}";
									parameters.Add(BuildSqlParameter(parameterName, property, node.Value<object>(i)));
									whereClause.Append(parameterName);
								}

								whereClause.Append("))");

							}
							else
							{
								throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
							}
						}
						else
						{
							throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
						}
					}
					break;

				case RqlNodeType.CONTAINS:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.Value<string>(0), StringComparison.OrdinalIgnoreCase));

						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"([{tableName}].[{property.Name}] LIKE (");
								else
									whereClause.Append($"([{schema}].[{tableName}].[{property.Name}] LIKE (");

								var filter = node.Value<string>(1);
								filter = filter.Replace("*", "%").Replace("?", "_");

								var parameterName = $"@P{parameters.Count}";
								parameters.Add(BuildSqlParameter(parameterName, property, filter));
								whereClause.Append(parameterName);

								whereClause.Append(")");
							}
							else
							{
								throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
							}
						}
						else
						{
							throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
						}
					}
					break;

				case RqlNodeType.EXCLUDES:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.Value<string>(0), StringComparison.OrdinalIgnoreCase));

						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"([{tableName}].[{property.Name}] NOT LIKE (");
								else
									whereClause.Append($"([{schema}].[{tableName}].[{property.Name}] NOT LIKE (");

								var filter = node.Value<string>(1);
								filter = filter.Replace("*", "%").Replace("?", "_");

								var parameterName = $"@P{parameters.Count}";
								parameters.Add(BuildSqlParameter(parameterName, property, filter));
								whereClause.Append(parameterName);

								whereClause.Append(")");
							}
							else
							{
								throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
							}
						}
						else
						{
							throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
						}
					}
					break;
			}

			return whereClause.ToString();
		}

		private static string ConstructComparrisonOperator<T>(string operation, string attributeName, object attributeValue, List<SqlParameter> parameters)
		{
			var tableAttribute = typeof(T).GetCustomAttribute<Table>(false);
			var property = typeof(T).GetProperties().FirstOrDefault(x => x.Name.ToLower() == attributeName.ToLower());

			if (property != null)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

				if (memberAttribute != null)
				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					try
					{
						if (attributeValue == null)
						{
							if (string.Compare(operation, "<>", true) == 0)
							{
								if (string.IsNullOrWhiteSpace(schema))
									return $"[{tableName}].[{columnName}] is not null";
								else
									return $"[{schema}].[{tableName}].[{columnName}] is not null";
							}
							else
							{
								if (string.IsNullOrWhiteSpace(schema))
									return $"[{tableName}].[{property.Name}] is null";
								else
									return $"[{schema}].[{tableName}].[{property.Name}] is null";
							}
						}
						else
						{
							var parameterName = $"@P{parameters.Count}";
							parameters.Add(BuildSqlParameter(parameterName, property, attributeValue));

							if (string.IsNullOrWhiteSpace(schema))
								return $"[{tableName}].[{property.Name}] {operation} {parameterName}";
							else
								return $"[{schema}].[{tableName}].[{property.Name}] {operation} {parameterName}";
						}
					}
					catch (FormatException error)
					{
						throw new ApiException(HttpStatusCode.BadRequest, new ApiError("exception", "Unknown exception"), error);
					}
				}
				else
				{
					throw new ApiException(HttpStatusCode.BadRequest, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
				}
			}
			else
			{
				throw new ApiException(HttpStatusCode.BadRequest, new ApiError("invalid_operation", $"Malformed RQL query: {attributeName} is not a member of {typeof(T).Name}."));
			}
		}

		/// <summary>
		/// Returns a formatted ORDER BY clause representation of the sort operation in the query string
		/// </summary>
		/// <typeparam name="T">The type of object from which the fields are being selected</typeparam>
		/// <param name="node">The RQL node representation of the query string</param>
		/// <returns></returns>
		private static string ParseOrderByClause<T>(RqlNode node)
		{
			var orderByClause = new StringBuilder();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>(false);

			if (node == null)
				return string.Empty;

			switch (node.NodeType)
			{
				case RqlNodeType.AND:
					{
						foreach (RqlNode argument in node.Value<List<RqlNode>>())
						{
							var subClause = ParseOrderByClause<T>(argument);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (orderByClause.Length > 0)
									orderByClause.Append(", ");

								orderByClause.Append(subClause);
							}
						}
					}
					break;
				case RqlNodeType.OR:
					{
						foreach (RqlNode argument in node.Value<List<RqlNode>>())
						{
							var subClause = ParseOrderByClause<T>(argument);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (orderByClause.Length > 0)
									orderByClause.Append(", ");

								orderByClause.Append(subClause);
							}
						}
					}
					break;

				case RqlNodeType.SORT:
					{
						foreach (var argument in node.Value<List<string>>())
						{
							var field = argument.ToString();
							var fieldName = field.StartsWith("+") ? field.Substring(1) : field.StartsWith("-") ? field.Substring(1) : field;
							var property = GetProperty<T>(fieldName);

							if (property != null)
							{
								var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

								if (memberAttribute != null)
								{
									var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
									var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
									var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

									if (orderByClause.Length > 0)
										orderByClause.Append(", ");

									if (field.StartsWith("-"))
									{
										if (string.IsNullOrWhiteSpace(schema))
											orderByClause.Append($"[{tableName}].[{property.Name}] desc");
										else
											orderByClause.Append($"[{schema}].[{tableName}].[{property.Name}] desc");
									}
									else
									{
										if (string.IsNullOrWhiteSpace(schema))
											orderByClause.Append($"[{tableName}].[{property.Name}]");
										else
											orderByClause.Append($"[{schema}].[{tableName}].[{property.Name}]");
									}
								}
								else
									throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
							}
							else
								throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("invalid_operation", $"{property.Name} is not a member of {typeof(T).Name}"));
						}
					}
					break;
			}

			return orderByClause.ToString();
		}

		private static PropertyInfo GetProperty<T>(string name)
		{
			var properties = typeof(T).GetProperties();
			return properties.FirstOrDefault(x => string.Compare(x.Name, name, true) == 0);
		}

		internal static SqlParameter BuildSqlParameter(string parameterName, PropertyInfo property, object value)
		{
			//	Is the property a string property?
			if (property.PropertyType == typeof(string))
			{
				if (value == DBNull.Value)
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute.StringType == StringType.UNICODE)
					{
						if (memberAttribute.Length > 0)
							return new SqlParameter(parameterName, SqlDbType.NVarChar, memberAttribute.Length) { Value = value };
						else
							return new SqlParameter(parameterName, SqlDbType.NVarChar, ((string)value).Length) { Value = value };
					}
					else
					{
						if (memberAttribute.Length > 0)
							return new SqlParameter(parameterName, SqlDbType.VarChar, memberAttribute.Length) { Value = value };
						else
							return new SqlParameter(parameterName, SqlDbType.VarChar, ((string)value).Length) { Value = value };
					}
				}
				else
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute.StringType == StringType.UNICODE)
					{
						if (memberAttribute.Length > 0)
							return new SqlParameter(parameterName, SqlDbType.NVarChar, memberAttribute.Length) { Value = value.ToString() };
						else
							return new SqlParameter(parameterName, SqlDbType.NVarChar, ((string)value).Length) { Value = value.ToString() };
					}
					else
					{
						if (memberAttribute.Length > 0)
							return new SqlParameter(parameterName, SqlDbType.VarChar, memberAttribute.Length) { Value = value.ToString() };
						else
							return new SqlParameter(parameterName, SqlDbType.VarChar, ((string)value).Length) { Value = value.ToString() };
					}
				}
			}
			else
				return new SqlParameter(parameterName, value);
		}
		#endregion
	}
}
