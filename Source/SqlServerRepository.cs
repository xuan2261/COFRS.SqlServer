using COFRS.Rql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace COFRS.SqlServer
{
	/// <summary>
	/// Standard Api Repository
	/// </summary>
	public class SqlServerRepository : IRepository
	{
		/// <summary>
		/// The repository options used to connect to and interact with the database
		/// </summary>
		protected readonly IRepositoryOptions _options;

		/// <summary>
		/// The SQL Connection used to interact with the database
		/// </summary>
		protected SqlConnection _connection;

		/// <summary>
		/// The Service Provider
		/// </summary>
		protected IServiceProvider ServiceProvider;

		/// <summary>
		/// The Service Provider
		/// </summary>
		protected ILogger<SqlServerRepository> Logger;

		/// <summary>
		/// Initializes a repository with the specified options
		/// </summary>
		///	<param name="logger">A generic interface for logging where the category name is derrived from the specified TCategoryName type name.</param>
		///	<param name="provider">Defines a mechanism for retrieving a service object; that is, an object that provides custom support to other objects.</param>
		///	<param name="options">The runtime options for this repository</param>
		public SqlServerRepository(ILogger<SqlServerRepository> logger, IServiceProvider provider, IRepositoryOptions options)
		{
			try
			{
				Logger = logger;
				ServiceProvider = provider;
				_options = options;
				_connection = new SqlConnection(_options.ConnectionString);
				_connection.Open();
			}
			catch (Exception error)
			{
				throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("connection_error", $"Cannot open database connection to {_options.ConnectionString}."), error);
			}
		}

		#region General Functions
		/// <summary>
		/// Returns the Repository options used by the repository
		/// </summary>
		/// <returns></returns>
		public IRepositoryOptions GetOptions()
		{
			return _options;
		}
		#endregion

		#region Asynchronous Operations
		/// <summary>
		/// Asynchronously adds an item to the datastore
		/// </summary>
		/// <param name="item">The item to add</param>
		/// <returns></returns>
		public async Task<object> AddAsync(object item)
		{
			using (var ctc = new CancellationTokenSource())
			{
				var task = Task.Run(async () =>
				{
					var parameters = new List<SqlParameter>();
					var emitter = new Emitter(ServiceProvider);

					var sql = emitter.BuildAddQuery(item, parameters, out PropertyInfo identityProperty);

					Logger.BeginScope<string>(sql.ToString());
					Logger.LogDebug($"[REPOSITORY] Add<{item.GetType().Name}>");

					using (var command = new SqlCommand(sql, _connection))
					{
						foreach (var parameter in parameters)
						{
							command.Parameters.Add(parameter);
						}

						if (identityProperty != null)
						{
							using (var reader = await command.ExecuteReaderAsync(ctc.Token))
							{
								if (await reader.ReadAsync(ctc.Token))
								{
									identityProperty.SetValue(item, await reader.GetFieldValueAsync<object>(0, ctc.Token));
									return item;
								}
							}
						}
						else
						{
							await command.ExecuteNonQueryAsync(ctc.Token);
							return item;
						}

						throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("unexcpected failure", "insert failed"));
					}
				});

				if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) == task)
					return task.Result;

				ctc.Cancel();
				throw new InvalidOperationException("Task exceeded time limit.");
			}
		}

		/// <summary>
		/// Deletes an item(s) from the datastore that match the specified keys. If no keys are specified
		/// all the items of type T will be deleted.
		/// </summary>
		/// <param name="T">The type of item to delete</param>
		/// <param name="keys">The keys that defined the item to be deleted</param>
		public async Task DeleteAsync(Type T, IEnumerable<KeyValuePair<string, object>> keys)
		{
			using (var ctc = new CancellationTokenSource())
			{
				var task = Task.Run(async () =>
				{
					var parameters = new List<SqlParameter>();
					var emitter = new Emitter(ServiceProvider);

					var sql = emitter.BuildDeleteQuery(keys, parameters, T);

					using (var command = new SqlCommand(sql, _connection))
					{
						foreach (var parameter in parameters)
						{
							command.Parameters.Add(parameter);
						}

						await command.ExecuteNonQueryAsync(ctc.Token);
					}
				});

				if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
				{
					ctc.Cancel();
					throw new InvalidOperationException("Task exceeded time limit.");
				}
			}
		}


		/// <summary>
		/// Gets a collection
		/// </summary>
		/// <param name="T">The type of items to retrieve</param>
		/// <param name="node">The compiled RQL Query</param>
		/// <param name="NoPaging">Do not page results even if the result set exceeds the system defined limit. Default value = false.</param>
		/// <returns></returns>
		public async Task<object> GetCollectionAsync(Type T, RqlNode node, bool NoPaging)
		{
			return await GetCollectionAsync(T, new List<KeyValuePair<string, object>>(), node, NoPaging);
		}

		/// <summary>
		/// Gets a collection
		/// </summary>
		/// <param name="T">The type of items to retrieve</param>
		/// <param name="keys">The list of keys to further limit the results of the query.</param>
		/// <param name="node">The compiled RQL Query</param>
		/// <param name="NoPaging">Do not page results even if the result set exceeds the system defined limit. Default value = false.</param>
		/// <returns></returns>
		public async Task<object> GetCollectionAsync(Type T, IEnumerable<KeyValuePair<string, object>> keys, RqlNode node, bool NoPaging)
		{
			using (var ctc = new CancellationTokenSource())
			{
				var task = Task.Run(async () =>
				{
					var parameters = new List<SqlParameter>();
					var pageFilter = RqlUtilities.ExtractClause(RqlNodeType.LIMIT, node);
					var options = ServiceProvider.GetService<IRepositoryOptions>();
					var translationOptions = ServiceProvider.GetService<ITranslationOptions>();
					var emitter = new Emitter(ServiceProvider);

					var rdgeneric = typeof(List<>);
					var rd = rdgeneric.MakeGenericType(T);
					var resultList = Activator.CreateInstance(rd);

					var rxgeneric = typeof(RqlCollection<>);
					var rx = rxgeneric.MakeGenericType(T);
					var results = Activator.CreateInstance(rx);
					var countProperty = results.GetType().GetProperty("count");
					var itemsProperty = results.GetType().GetProperty("items");
					var limitProperty = results.GetType().GetProperty("limit");
					var hrefProperty = results.GetType().GetProperty("href");
					var firstProperty = results.GetType().GetProperty("first");
					var previousProperty = results.GetType().GetProperty("previous");
					var nextProperty = results.GetType().GetProperty("next");

					if (!NoPaging)
					{
						if (pageFilter != null && pageFilter.Value<int>(0) < 1)
						{
							pageFilter = new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, options.BatchLimit });
						}
						else
						{
							var sqlCount = emitter.BuildCollectionCountQuery(keys, node, parameters, T);
							Logger.BeginScope<string>(sqlCount.ToString());
							Logger.LogTrace($"[REPOSITORY] ReadCollection<{T.Name}>");

							//	We now have an SQL query that needs to be executed in order to get our object.
							using (var command = new SqlCommand(sqlCount, _connection))
							{
								foreach (var parameter in parameters)
								{
									command.Parameters.Add(parameter);
								}

								using (var reader = await command.ExecuteReaderAsync(ctc.Token))
								{
									if (await reader.ReadAsync())
									{
										countProperty.SetValue(results, await reader.ReadInt32Async("RecordCount", ctc.Token));
									}
								}
							}
						}

						if (ctc.Token.IsCancellationRequested)
							return default;
					}

					parameters.Clear();
					var sql = emitter.BuildCollectionListQuery(keys, node, Convert.ToInt32(countProperty.GetValue(results)), _options.BatchLimit, pageFilter, parameters, T, NoPaging);

					int theCount = 0;
					Logger.BeginScope<string>(sql.ToString());
					Logger.LogTrace($"[REPOSITORY] ReadCollection<{T.Name}>");

					if (ctc.Token.IsCancellationRequested)
						return default;

					//	We now have an SQL query that needs to be executed in order to get our object.
					using (var command = new SqlCommand(sql, _connection))
					{
						foreach (var parameter in parameters)
						{
							command.Parameters.Add(parameter);
						}

						using (var reader = await command.ExecuteReaderAsync(ctc.Token))
						{
							while (await reader.ReadAsync(ctc.Token))
							{
								var entity = await reader.ReadAsync(node, T, ctc.Token);
								var method = resultList.GetType().GetMethod("Add");
								method.Invoke(resultList, new object[] { entity });
								theCount++;

								if (ctc.Token.IsCancellationRequested)
									return default;
							}

							var toArrayMethod = resultList.GetType().GetMethod("ToArray");
							itemsProperty.SetValue(results, toArrayMethod.Invoke(resultList, null));

							if (Convert.ToInt32(countProperty.GetValue(results)) == 0)
							{
								countProperty.SetValue(results, theCount);
							}
						}
					}

					if (ctc.Token.IsCancellationRequested)
						return default;

					if (pageFilter == null)
					{
						pageFilter = new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, options.BatchLimit });
					}
					else if (pageFilter.Value<int>(1) > options.BatchLimit)
					{
						pageFilter.SetValue<int>(1, options.BatchLimit);
					}

					var start = pageFilter.Value<int>(0);
					var batchSize = pageFilter.Value<int>(1);

					if (Convert.ToInt32(countProperty.GetValue(results)) <= batchSize)
						limitProperty.SetValue(results, null);
					else
						limitProperty.SetValue(results, theCount);

					var refNode = node;

					if (refNode == null)
						refNode = new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, options.BatchLimit });

					if (RqlUtilities.ExtractClause(RqlNodeType.LIMIT, refNode) == null)
						refNode = new RqlNode(RqlNodeType.AND, new List<RqlNode> { new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, options.BatchLimit }), refNode });

					var hrefNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { start, batchSize }));
					hrefProperty.SetValue(results, new Uri(translationOptions.RootUrl, $"collection?{hrefNode.ToString()}"));

					if (start > 1)
					{
						var newStart = start - batchSize;
						if (newStart < 1)
							newStart = 1;

						var firstNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, batchSize }));
						firstProperty.SetValue(results, new Uri(translationOptions.RootUrl, $"collection?{firstNode.ToString()}"));

						var prevNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { newStart, batchSize }));
						previousProperty.SetValue(results, new Uri(translationOptions.RootUrl, $"collection?{prevNode.ToString()}"));
					}

					if (Convert.ToInt32(countProperty.GetValue(results)) >= pageFilter.Value<int>(0) + pageFilter.Value<int>(1))
					{
						var nextNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { start + batchSize, batchSize }));
						nextProperty.SetValue(results, new Uri(translationOptions.RootUrl, $"collection?{nextNode.ToString()}"));
					}

					return results;
				});

				if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
				{
					ctc.Cancel();
					throw new InvalidOperationException("Task exceeded time limit.");
				}

				return task.Result;
			}
		}

		/// <summary>
		/// Returns a single item based on a set of keys
		/// </summary>
		/// <param name="T">The type of object to retrieve</param>
		/// <param name="keys">The primary key value</param>
		/// <param name="node">The compiled RQL query</param>
		/// <returns></returns>
		public async Task<object> GetSingleAsync(Type T, IEnumerable<KeyValuePair<string, object>> keys, RqlNode node)
		{
			using (var ctc = new CancellationTokenSource())
			{
				var task = Task.Run(async () =>
				{
					var parameters = new List<SqlParameter>();
					var emitter = new Emitter(ServiceProvider);
					var sql = emitter.BuildSingleQuery(keys, node, parameters, T);

					Logger.BeginScope<string>(sql.ToString());
					Logger.LogDebug($"[REPOSITORY] ReadSingle<{T.Name}>");

					//	We now have an SQL query that needs to be executed in order to get our object.
					using (var command = new SqlCommand(sql, _connection))
					{
						foreach (var parameter in parameters)
						{
							command.Parameters.Add(parameter);
						}

						using (var reader = await command.ExecuteReaderAsync(ctc.Token))
						{
							if (await reader.ReadAsync(ctc.Token))
							{
								//	Read the object (of type T) from the database.
								//	This will create a new object of type T populated with data from the database.
								return await reader.ReadAsync(node, T, ctc.Token);
							}
						}
					}

					return default;
				});

				if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
				{
					ctc.Cancel();
					throw new InvalidOperationException("Task exceeded time limit.");
				}

				return task.Result;
			}
		}

		/// <summary>
		/// Update an item in the repository
		/// </summary>
		/// <param name="item"></param>
		/// <returns></returns>
		public async Task UpdateAsync(object item)
		{
			using (var ctc = new CancellationTokenSource())
			{
				var task = Task.Run(async () =>
				{
					var parameters = new List<SqlParameter>();
					var emitter = new Emitter(ServiceProvider);
					var sql = emitter.BuildUpdateQuery(item, parameters);

					Logger.BeginScope<string>(sql.ToString());
					Logger.LogTrace($"[REPOSITORY] Update<{item.GetType().Name}>");

					using (var command = new SqlCommand(sql, _connection))
					{
						foreach (var parameter in parameters)
						{
							command.Parameters.Add(parameter);
						}

						await command.ExecuteNonQueryAsync(ctc.Token);
					}
				});

				if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
				{
					ctc.Cancel();
					throw new InvalidOperationException("Task exceeded time limit.");
				}
			}
		}

		/// <summary>
		/// Patch Async
		/// </summary>
		/// <param name="T">The type of item to patch</param>
		/// <param name="keys"></param>
		/// <param name="patchCommands"></param>
		/// <returns></returns>
		public async Task PatchAsync(Type T, IEnumerable<KeyValuePair<string, object>> keys, IEnumerable<PatchCommand> patchCommands)
		{
			using (var ctc = new CancellationTokenSource())
			{
				var task = Task.Run(async () =>
				{
					var parameters = new List<SqlParameter>();
					var emitter = new Emitter(ServiceProvider);
					var sql = emitter.BuildPatchQuery(keys, patchCommands, parameters, T);

					Logger.BeginScope<string>(sql.ToString());
					Logger.LogTrace($"[REPOSITORY] Patch<{T.Name}>");

					using (var command = new SqlCommand(sql, _connection))
					{
						foreach (var parameter in parameters)
						{
							command.Parameters.Add(parameter);
						}

						await command.ExecuteNonQueryAsync(ctc.Token);
					}
				});

				if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
				{
					ctc.Cancel();
					throw new InvalidOperationException("Task exceeded time limit.");
				}
			}
		}
		#endregion

		#region Helper Functions
		internal static bool CanCast(Type type, object obj)
		{
			if (obj.GetType() == type)
			{
				return true;
			}

			// If it's null, we can't get the type.
			if (obj != null)
			{
				var converter = TypeDescriptor.GetConverter(type);
				if (!converter.CanConvertFrom(obj.GetType()))
					return false;

				return true;
			}

			//Be permissive if the object was null and the target is a ref-type
			return !type.IsValueType;
		}

		internal static bool CanCast<T>(object obj)
		{
			if (obj is T)
			{
				return true;
			}

			// If it's null, we can't get the type.
			if (obj != null)
			{
				var converter = TypeDescriptor.GetConverter(typeof(T));
				if (!converter.CanConvertFrom(obj.GetType()))
					return false;

				return true;
			}

			//Be permissive if the object was null and the target is a ref-type
			return !typeof(T).IsValueType;
		}


		#endregion

		#region IDisposable Support
		private bool disposedValue = false; // To detect redundant calls

		/// <summary>
		/// Disposes the respository
		/// </summary>
		/// <param name="disposing"></param>
		protected virtual void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					_connection.Close();
					_connection.Dispose();
				}

				disposedValue = true;
			}
		}

		/// <summary>
		/// Destructor
		/// </summary>
		~SqlServerRepository()
		{
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(false);
		}

		/// <summary>
		/// This code added to correctly implement the disposable pattern.
		/// </summary>
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		#endregion
	}
}
