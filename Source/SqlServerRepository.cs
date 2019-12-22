using COFRS.Rql;
using Microsoft.Extensions.Logging;
using Serilog.Context;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace COFRS.SqlServer
{
	/// <summary>
	/// Standard Api Repository
	/// </summary>
	public class SqlServerRepository : IRepository
	{
		/// <summary>
		/// The logger
		/// </summary>
		protected ILogger logger;

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
		/// Initializes a repository with the specified options
		/// </summary>
		/// <param name="serviceProvider"></param>
		public SqlServerRepository(IServiceProvider serviceProvider)
		{
			try
			{
				using (var factory = new LoggerFactory())
				{
					logger = factory.CreateLogger("ApiRepository");
				}

				ServiceProvider = serviceProvider;
				_options = (IRepositoryOptions) ServiceProvider.GetService(typeof(IRepositoryOptions));
				_connection = new SqlConnection(_options.ConnectionString);
				_connection.Open();
			}
			catch (Exception error)
			{
				throw new ApiException(HttpStatusCode.InternalServerError, new ApiError("connection_error", $"Cannot open database connection to {_options.ConnectionString}."), error);
			}
		}

		#region Asynchronous Operations
		/// <summary>
		/// Asynchronously adds an item to the datastore
		/// </summary>
		/// <typeparam name="T">The type of object to add</typeparam>
		/// <param name="item">The item to add</param>
		/// <returns></returns>
		public async Task<T> AddAsync<T>(T item)
		{
			using var ctc = new CancellationTokenSource();
			var task = Task.Run(async () =>
			{
				var parameters = new List<SqlParameter>();
				var emitter = new Emitter(ServiceProvider);

				var sql = emitter.BuildAddQuery(item,
				parameters,
				out PropertyInfo identityProperty);

				using (LogContext.PushProperty("SQL", sql.ToString()))
				{
					logger.LogDebug($"[REPOSITORY] Add<{typeof(T).Name}>");

					using var command = new SqlCommand(sql, _connection);
					foreach (var parameter in parameters)
					{
						command.Parameters.Add(parameter);
					}

					if (identityProperty != null)
					{
						using var reader = await command.ExecuteReaderAsync(ctc.Token);
						if (await reader.ReadAsync(ctc.Token))
						{
							identityProperty.SetValue(item, await reader.GetFieldValueAsync<object>(0, ctc.Token));
							return item;
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

		/// <summary>
		/// Deletes an item(s) from the datastore that match the specified keys. If no keys are specified
		/// all the items of type T will be deleted.
		/// </summary>
		/// <typeparam name="T">The type of item to delete</typeparam>
		/// <param name="keys">The keys that defined the item to be deleted</param>
		public async Task DeleteAsync<T>(IEnumerable<KeyValuePair<string, object>> keys)
		{
			using var ctc = new CancellationTokenSource();
			var task = Task.Run(async () =>
			{
				var parameters = new List<SqlParameter>();
				var emitter = new Emitter(ServiceProvider);

				var sql = emitter.BuildDeleteQuery<T>(keys, parameters);

				using var command = new SqlCommand(sql, _connection);
				foreach (var parameter in parameters)
				{
					command.Parameters.Add(parameter);
				}

				await command.ExecuteNonQueryAsync(ctc.Token);
			});

			if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
			{
				ctc.Cancel();
				throw new InvalidOperationException("Task exceeded time limit.");
			}
		}

		/// <summary>
		/// Gets a collection
		/// </summary>
		/// <typeparam name="T">The type of items to retrieve</typeparam>
		/// <param name="node">The compiled RQL Query</param>
		/// <param name="NoPaging">Do not page results even if the result set exceeds the system defined limit. Default value = false.</param>
		/// <returns></returns>
		public async Task<RqlCollection<T>> GetCollectionAsync<T>(RqlNode node, bool NoPaging)
		{
			return await GetCollectionAsync<T>(new List<KeyValuePair<string, object>>(), node, NoPaging);
		}

		/// <summary>
		/// Gets a collection
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="keys">The list of keys to further limit the results of the query.</param>
		/// <param name="node">The compiled RQL Query</param>
		/// <param name="NoPaging">Do not page results even if the result set exceeds the system defined limit. Default value = false.</param>
		/// <returns></returns>
		public async Task<RqlCollection<T>> GetCollectionAsync<T>(IEnumerable<KeyValuePair<string, object>> keys, RqlNode node, bool NoPaging)
		{
			using var ctc = new CancellationTokenSource();
			var task = Task.Run(async () =>
			{
				var parameters = new List<SqlParameter>();
				var results = new RqlCollection<T>();
				var pageFilter = RqlUtilities.ExtractClause(RqlNodeType.LIMIT, node);
				var options = (IRepositoryOptions) ServiceProvider.GetService(typeof(IRepositoryOptions));
				var translationOptions = (ITranslationOptions)ServiceProvider.GetService(typeof(ITranslationOptions));
				var resultList = new List<T>();
				var emitter = new Emitter(ServiceProvider);

				if (!NoPaging)
				{
					if (pageFilter != null && pageFilter.Value<int>(0) < 1)
					{
						pageFilter = new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, options.BatchLimit });
					}
					else
					{
						var sqlCount = emitter.BuildCollectionCountQuery<T>(keys, node, parameters);

						using (LogContext.PushProperty("SQL", sqlCount.ToString()))
						{
							logger.LogTrace($"[REPOSITORY] ReadCollection<{typeof(T).Name}>");

							//	We now have an SQL query that needs to be executed in order to get our object.
							using var command = new SqlCommand(sqlCount, _connection);
							foreach (var parameter in parameters)
							{
								command.Parameters.Add(parameter);
							}

							using var reader = await command.ExecuteReaderAsync(ctc.Token);
							if (await reader.ReadAsync())
							{
								results.count = await reader.ReadInt32Async("RecordCount", ctc.Token);
							}
						}
					}

					if (ctc.Token.IsCancellationRequested)
						return default;
				}

				parameters.Clear();
				var sql = emitter.BuildCollectionListQuery<T>(keys, node, results.count, _options.BatchLimit, pageFilter, parameters, NoPaging);

				using (LogContext.PushProperty("SQL", sql.ToString()))
				{
					logger.LogTrace($"[REPOSITORY] ReadCollection<{typeof(T).Name}>");

					if (ctc.Token.IsCancellationRequested)
						return default;

					//	We now have an SQL query that needs to be executed in order to get our object.
					using (var command = new SqlCommand(sql, _connection))
					{
						foreach (var parameter in parameters)
						{
							command.Parameters.Add(parameter);
						}

						using var reader = await command.ExecuteReaderAsync(ctc.Token);
						while (await reader.ReadAsync(ctc.Token))
						{
							resultList.Add(await reader.ReadAsync<T>(node, ctc.Token));

							if (ctc.Token.IsCancellationRequested)
								return default;
						}

						results.items = resultList.ToArray();

						if (results.count == 0)
							results.count = resultList.Count;
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

					if (results.count <= batchSize)
						results.limit = null;
					else
						results.limit = resultList.Count;

					var refNode = node;

					if (refNode == null)
						refNode = new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, options.BatchLimit });

					if (RqlUtilities.ExtractClause(RqlNodeType.LIMIT, refNode) == null)
						refNode = new RqlNode(RqlNodeType.AND, new List<RqlNode> { new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, options.BatchLimit }), refNode });

					var hrefNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { start, batchSize }));
					results.href = new Uri(translationOptions.RootUrl, $"collection?{hrefNode.ToString()}");

					if (start > 1)
					{
						var newStart = start - batchSize;
						if (newStart < 1)
							newStart = 1;

						var firstNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { 1, batchSize }));
						results.first = new Uri(translationOptions.RootUrl, $"collection?{firstNode.ToString()}");

						var prevNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { newStart, batchSize }));
						results.previous = new Uri(translationOptions.RootUrl, $"collection?{prevNode.ToString()}");
					}

					if (results.count >= pageFilter.Value<int>(0) + pageFilter.Value<int>(1))
					{
						var nextNode = RqlUtilities.ReplaceClause(RqlNodeType.LIMIT, refNode, new RqlNode(RqlNodeType.LIMIT, new List<int> { start + batchSize, batchSize }));
						results.next = new Uri(translationOptions.RootUrl, $"collection?{nextNode.ToString()}");
					}

					return results;
				}
			});

			if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
			{
				ctc.Cancel();
				throw new InvalidOperationException("Task exceeded time limit.");
			}

			return task.Result;
		}

		/// <summary>
		/// Returns a single item based on a set of keys
		/// </summary>
		/// <typeparam name="T">The type of object to retrieve</typeparam>
		/// <param name="keys">The primary key value</param>
		/// <param name="node">The compiled RQL query</param>
		/// <returns></returns>
		public async Task<T> GetSingleAsync<T>(IEnumerable<KeyValuePair<string, object>> keys, RqlNode node)
		{
			using var ctc = new CancellationTokenSource();
			var task = Task.Run(async () =>
			{
				var parameters = new List<SqlParameter>();
				var emitter = new Emitter(ServiceProvider);
				var sql = emitter.BuildSingleQuery<T>(keys, node, parameters);

				using (LogContext.PushProperty("SQL", sql.ToString()))
					logger.LogDebug($"[REPOSITORY] ReadSingle<{typeof(T).Name}>");

				//	We now have an SQL query that needs to be executed in order to get our object.
				using (var command = new SqlCommand(sql, _connection))
				{
					foreach (var parameter in parameters)
					{
						command.Parameters.Add(parameter);
					}

					using var reader = await command.ExecuteReaderAsync(ctc.Token);
					if (await reader.ReadAsync(ctc.Token))
					{
						//	Read the object (of type T) from the database.
						//	This will create a new object of type T populated with data from the database.
						return await reader.ReadAsync<T>(node, ctc.Token);
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

		/// <summary>
		/// Update an item in the repository
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="item"></param>
		/// <returns></returns>
		public async Task UpdateAsync<T>(T item)
		{
			using var ctc = new CancellationTokenSource();
			var task = Task.Run(async () =>
			{
				var parameters = new List<SqlParameter>();
				var emitter = new Emitter(ServiceProvider);
				var sql = emitter.BuildUpdateQuery(item, parameters);

				using (LogContext.PushProperty("SQL", sql.ToString()))
					logger.LogTrace($"[REPOSITORY] Update<{typeof(T).Name}>");

				using var command = new SqlCommand(sql, _connection);
				foreach (var parameter in parameters)
				{
					command.Parameters.Add(parameter);
				}

				await command.ExecuteNonQueryAsync(ctc.Token);
			});

			if (await Task.WhenAny(task, Task.Delay(_options.Timeout)) != task)
			{
				ctc.Cancel();
				throw new InvalidOperationException("Task exceeded time limit.");
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

				// TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
				// TODO: set large fields to null.

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
