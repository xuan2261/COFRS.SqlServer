using COFRS.Rql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace COFRS.SqlServer
{
	/// <summary>
	/// SQL Extensions
	/// </summary>
	internal static class SqlExtensions
	{
		#region Read Boolean Values
		/// <summary>
		/// Reads a boolean value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static bool ReadBoolean(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to bool");

			return reader.GetFieldValue<bool>(columnIndex);
		}

		/// <summary>
		/// Reads a boolean value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<bool> ReadBooleanAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to bool");

			return await reader.GetFieldValueAsync<bool>(columnIndex);
		}

		/// <summary>
		/// Reads a boolean value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<bool> ReadBooleanAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to bool");

			return await reader.GetFieldValueAsync<bool>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable boolean value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static bool? ReadNullableBoolean(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<bool>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable boolean value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<bool?> ReadNullableBooleanAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<bool?>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable boolean value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<bool?> ReadNullableBooleanAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<bool?>(columnIndex, token);
		}
		#endregion

		#region Read byte values
		/// <summary>
		/// Reads a byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static byte ReadByte(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to byte");

			return reader.GetFieldValue<byte>(columnIndex);
		}

		/// <summary>
		/// Reads a byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<byte> ReadByteAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to byte");

			return await reader.GetFieldValueAsync<byte>(columnIndex);
		}
		/// <summary>
		/// Reads a byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<byte> ReadByteAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to byte");

			return await reader.GetFieldValueAsync<byte>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static byte? ReadNullableByte(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<byte>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<byte?> ReadNullableByteAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<byte>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<byte?> ReadNullableByteAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<byte>(columnIndex, token);
		}
		#endregion

		#region Read SByte Values
		/// <summary>
		/// Reads a signed byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static sbyte ReadSByte(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to SByte");

			return reader.GetFieldValue<sbyte>(columnIndex);
		}

		/// <summary>
		/// Reads a signed byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<sbyte> ReadSByteAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to SByte");

			return await reader.GetFieldValueAsync<sbyte>(columnIndex);
		}

		/// <summary>
		/// Reads a signed byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<sbyte> ReadSByteAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to SByte");

			return await reader.GetFieldValueAsync<sbyte>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable signed byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static sbyte? ReadNullableSByte(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<sbyte>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable signed byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<sbyte?> ReadNullableSByteAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<sbyte>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable signed byte value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<sbyte?> ReadNullableSByteAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<sbyte>(columnIndex, token);
		}
		#endregion

		#region Read short Values
		/// <summary>
		/// Reads a short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static short ReadInt16(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int16");

			return reader.GetFieldValue<short>(columnIndex);
		}

		/// <summary>
		/// Reads a short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<short> ReadInt16Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int16");

			return await reader.GetFieldValueAsync<short>(columnIndex);
		}

		/// <summary>
		/// Reads a short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<short> ReadInt16Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int16");

			return await reader.GetFieldValueAsync<short>(columnIndex, token);
		}

		/// <summary>
		/// Reads a short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static short? ReadNullableInt16(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<short>(columnIndex);
		}

		/// <summary>
		/// Reads a short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<short?> ReadNullableInt16Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<short>(columnIndex);
		}

		/// <summary>
		/// Reads a short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<short?> ReadNullableInt16Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<short>(columnIndex, token);
		}
		#endregion

		#region Read ushort Values
		/// <summary>
		/// Reads an unsigned short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static ushort ReadUInt16(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt16");

			return reader.GetFieldValue<ushort>(columnIndex);
		}

		/// <summary>
		/// Reads an unsigned short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<ushort> ReadUInt16Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt16");

			return await reader.GetFieldValueAsync<ushort>(columnIndex);
		}

		/// <summary>
		/// Reads an unsigned short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<ushort> ReadUInt16Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt16");

			return await reader.GetFieldValueAsync<ushort>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable unsigned short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static ushort? ReadNullableUInt16(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<ushort>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable unsigned short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<ushort?> ReadNullableUInt16Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<ushort>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable unsigned short interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<ushort?> ReadNullableUInt16Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<ushort>(columnIndex, token);
		}
		#endregion

		#region Read int Values
		/// <summary>
		/// Reads an interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static int ReadInt32(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int32");

			return reader.GetFieldValue<int>(columnIndex);
		}

		/// <summary>
		/// Reads an interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public async static Task<int> ReadInt32Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int32");

			return await reader.GetFieldValueAsync<int>(columnIndex);
		}

		/// <summary>
		/// Reads an interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<int> ReadInt32Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int32");

			return await reader.GetFieldValueAsync<int>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static int? ReadNullableInt32(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<int>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<int?> ReadNullableInt32Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<int>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<int?> ReadNullableInt32Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<int>(columnIndex, token);
		}
		#endregion

		#region Read uint Values
		/// <summary>
		/// Reads an unsigned interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static uint ReadUInt32(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt32");

			return reader.GetFieldValue<uint>(columnIndex);
		}

		/// <summary>
		/// Reads an unsigned interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<uint> ReadUInt32Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt32");

			return await reader.GetFieldValueAsync<uint>(columnIndex);
		}

		/// <summary>
		/// Reads an unsigned interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation Token</param>
		/// <returns></returns>
		public static async Task<uint> ReadUInt32Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt32");

			return await reader.GetFieldValueAsync<uint>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable unsigned interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static uint? ReadNullableUInt32(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<uint>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable unsigned interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<uint?> ReadNullableUInt32Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<uint>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable unsigned interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<uint?> ReadNullableUInt32Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<uint>(columnIndex, token);
		}
		#endregion

		#region Read long Values
		/// <summary>
		/// Reads a long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static long ReadInt64(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int64");

			return reader.GetFieldValue<long>(columnIndex);
		}

		/// <summary>
		/// Reads a long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<long> ReadInt64Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int64");

			return await reader.GetFieldValueAsync<long>(columnIndex);
		}

		/// <summary>
		/// Reads a long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<long> ReadInt64Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Int64");

			return await reader.GetFieldValueAsync<long>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static long? ReadNullableInt64(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<long>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<long?> ReadNullableInt64Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<long>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<long?> ReadNullableInt64Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<long>(columnIndex, token);
		}
		#endregion

		#region Read ulong Values
		/// <summary>
		/// Reads an unsigned long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static ulong ReadUInt64(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt64");

			return reader.GetFieldValue<ulong>(columnIndex);
		}

		/// <summary>
		/// Reads an unsigned long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<ulong> ReadUInt64Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt64");

			return await reader.GetFieldValueAsync<ulong>(columnIndex);
		}

		/// <summary>
		/// Reads an unsigned long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<ulong> ReadUInt64Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to UInt64");

			return await reader.GetFieldValueAsync<ulong>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable unsigned long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static ulong? ReadNullableUInt64(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<ulong>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable unsigned long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<ulong?> ReadNullableUInt64Async(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<ulong>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable unsigned long interger value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<ulong?> ReadNullableUInt64Async(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<ulong>(columnIndex, token);
		}
		#endregion

		#region Read Guid Values
		/// <summary>
		/// Reads a Guid value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static Guid ReadGuid(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Guid");

			return reader.GetFieldValue<Guid>(columnIndex);
		}

		/// <summary>
		/// Reads a Guid value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<Guid> ReadGuidAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Guid");

			return await reader.GetFieldValueAsync<Guid>(columnIndex);
		}

		/// <summary>
		/// Reads a Guid value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<Guid> ReadGuidAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to Guid");

			return await reader.GetFieldValueAsync<Guid>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable Guid value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static Guid? ReadNullableGuid(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<Guid>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable Guid value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<Guid?> ReadNullableGuidAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<Guid>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable Guid value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<Guid?> ReadNullableGuidAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<Guid>(columnIndex, token);
		}
		#endregion

		#region Read string Values
		/// <summary>
		/// Reads a string value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static string ReadString(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetString(columnIndex);
		}

		/// <summary>
		/// Reads a string value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<string> ReadStringAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (await reader.IsDBNullAsync(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<string>(columnIndex);
		}

		/// <summary>
		/// Reads a string value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<string> ReadStringAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (await reader.IsDBNullAsync(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<string>(columnIndex, token);
		}
		#endregion

		#region Read decimal Values
		/// <summary>
		/// Reads a decimal value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static decimal ReadDecimal(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to decimal");

			return reader.GetFieldValue<decimal>(columnIndex);
		}

		/// <summary>
		/// Reads a decimal value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<decimal> ReadDecimalAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to decimal");

			return await reader.GetFieldValueAsync<decimal>(columnIndex);
		}

		/// <summary>
		/// Reads a decimal value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<decimal> ReadDecimalAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to decimal");

			return await reader.GetFieldValueAsync<decimal>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable decimal value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static decimal? ReadNullableDecimal(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<decimal>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable decimal value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<decimal?> ReadNullableDecimalAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<decimal>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable decimal value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<decimal?> ReadNullableDecimalAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<decimal>(columnIndex, token);
		}
		#endregion

		#region Read char Values
		/// <summary>
		/// Reads a char value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static char ReadChar(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to char");

			return reader.GetFieldValue<char>(columnIndex);
		}

		/// <summary>
		/// Reads a char value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<char> ReadCharAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to char");

			return await reader.GetFieldValueAsync<char>(columnIndex);
		}

		/// <summary>
		/// Reads a char value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<char> ReadCharAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to char");

			return await reader.GetFieldValueAsync<char>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable char value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static char? ReadNullableChar(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<char>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable char value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<char?> ReadNullableCharAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<char>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable char value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<char?> ReadNullableCharAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<char>(columnIndex, token);
		}
		#endregion

		#region Read double Values
		/// <summary>
		/// Reads a double value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static double ReadDouble(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to double");

			return reader.GetFieldValue<double>(columnIndex);
		}

		/// <summary>
		/// Reads a double value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<double> ReadDoubleAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to double");

			return await reader.GetFieldValueAsync<double>(columnIndex);
		}

		/// <summary>
		/// Reads a double value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<double> ReadDoubleAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to double");

			return await reader.GetFieldValueAsync<double>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable double value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static double? ReadNullableDouble(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<double>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable double value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<double?> ReadNullableDoubleAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<double>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable double value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<double?> ReadNullableDoubleAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<double>(columnIndex, token);
		}
		#endregion

		#region Read single Values
		/// <summary>
		/// Reads a single value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static double ReadSingle(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to single");

			return reader.GetFieldValue<float>(columnIndex);
		}

		/// <summary>
		/// Reads a single value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<double> ReadSingleAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to single");

			return await reader.GetFieldValueAsync<float>(columnIndex);
		}

		/// <summary>
		/// Reads a single value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<double> ReadSingleAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to single");

			return await reader.GetFieldValueAsync<float>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable single value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static double? ReadNullableSingle(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<float>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable single value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<double?> ReadNullableSingleAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<float>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable single value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<double?> ReadNullableSingleAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<float>(columnIndex, token);
		}
		#endregion

		#region Read DateTime Values
		/// <summary>
		/// Reads a date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static DateTime ReadDateTime(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to DateTime");

			return reader.GetFieldValue<DateTime>(columnIndex);
		}

		/// <summary>
		/// Reads a date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<DateTime> ReadDateTimeAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to DateTime");

			return await reader.GetFieldValueAsync<DateTime>(columnIndex);
		}

		/// <summary>
		/// Reads a date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<DateTime> ReadDateTimeAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to DateTime");

			return await reader.GetFieldValueAsync<DateTime>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static DateTime? ReadNullableDateTime(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<DateTime>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<DateTime?> ReadNullableDateTimeAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<DateTime>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<DateTime?> ReadNullableDateTimeAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<DateTime>(columnIndex, token);
		}
		#endregion

		#region Read DateTimeOffset Values
		/// <summary>
		/// Reads a date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static DateTimeOffset ReadDateTimeOffset(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to DateTimeOffset");

			return reader.GetFieldValue<DateTimeOffset>(columnIndex);
		}

		/// <summary>
		/// Reads a date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<DateTimeOffset> ReadDateTimeOffsetAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to DateTimeOffset");

			return await reader.GetFieldValueAsync<DateTimeOffset>(columnIndex);
		}

		/// <summary>
		/// Reads a date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<DateTimeOffset> ReadDateTimeOffsetAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to DateTimeOffset");

			return await reader.GetFieldValueAsync<DateTimeOffset>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static DateTimeOffset? ReadNullableDateTimeOffset(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<DateTimeOffset>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<DateTimeOffset?> ReadNullableDateTimeOffsetAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<DateTimeOffset>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable date/time value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<DateTimeOffset?> ReadNullableDateTimeOffsetAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<DateTimeOffset>(columnIndex, token);
		}
		#endregion

		#region Read TimeSpan Values
		/// <summary>
		/// Reads a TimeSpan value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static TimeSpan ReadTimeSpan(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to TimeSpan");

			return reader.GetFieldValue<TimeSpan>(columnIndex);
		}

		/// <summary>
		/// Reads a TimeSpan value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<TimeSpan> ReadTimeSpanAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to TimeSpan");

			return await reader.GetFieldValueAsync<TimeSpan>(columnIndex);
		}

		/// <summary>
		/// Reads a TimeSpan value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The cancellation token</param>
		/// <returns></returns>
		public static async Task<TimeSpan> ReadTimeSpanAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				throw new InvalidCastException("Null value cannot be cast to TimeSpan");

			return await reader.GetFieldValueAsync<TimeSpan>(columnIndex, token);
		}

		/// <summary>
		/// Reads a nullable TimeSpan value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static TimeSpan? ReadNullableTimeSpan(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return reader.GetFieldValue<TimeSpan>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable TimeSpan value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <returns></returns>
		public static async Task<TimeSpan?> ReadNullableTimeSpanAsync(this SqlDataReader reader, string columnName)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<TimeSpan>(columnIndex);
		}

		/// <summary>
		/// Reads a nullable TimeSpan value from the database
		/// </summary>
		/// <param name="reader">The SqlDataReader object</param>
		/// <param name="columnName">The name of the column to read from</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<TimeSpan?> ReadNullableTimeSpanAsync(this SqlDataReader reader, string columnName, CancellationToken token)
		{
			var columnIndex = reader.GetOrdinal(columnName);

			if (reader.IsDBNull(columnIndex))
				return null;

			return await reader.GetFieldValueAsync<TimeSpan>(columnIndex, token);
		}
		#endregion

		#region Read Property Values
		/// <summary>
		/// Read the property
		/// </summary>
		/// <param name="reader"></param>
		/// <param name="property"></param>
		public static object ReadProperty(this SqlDataReader reader, PropertyInfo property)
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
			var propertyType = property.PropertyType;

			if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
			{
				propertyType = Nullable.GetUnderlyingType(property.PropertyType);
			}

			if (propertyType.IsEnum)
			{
				var enumMember = propertyType.GetMembers().FirstOrDefault(member => member.DeclaringType == propertyType && member.Name != "value__");
				var stringType = enumMember.GetCustomAttribute<StringValue>();
				var enumValues = propertyType.GetEnumValues();

				if (stringType != null)
				{
					var propertyValue = reader.ReadString(property.Name);
					var member = propertyType.GetMembers().FirstOrDefault(m => m.DeclaringType == propertyType &&
																		   m.Name != "value__" &&
																		   m.GetCustomAttribute<StringValue>().Value.ToLower() == propertyValue.ToLower());

					if (member != null)
					{
						foreach (var eval in enumValues)
						{
							if (eval.ToString() == member.Name)
							{
								return eval;
							}
						}
					}

					throw new Exception("Invalid data");
				}
				else
				{
					var underlyingType = propertyType.GetEnumUnderlyingType();

					switch (Type.GetTypeCode(underlyingType))
					{
						case TypeCode.Byte:
							{
								var propertyValue = reader.ReadByte(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (byte)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.SByte:
							{
								var propertyValue = reader.ReadSByte(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (SByte)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int16:
							{
								var propertyValue = reader.ReadInt16(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (short)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt16:
							{
								var propertyValue = reader.ReadUInt16(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (ushort)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int32:
							{
								var propertyValue = reader.ReadInt32(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (int)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt32:
							{
								var propertyValue = reader.ReadUInt32(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (uint)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int64:
							{
								var propertyValue = reader.ReadInt64(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (long)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt64:
							{
								var propertyValue = reader.ReadUInt64(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (ulong)eval)
									{
										return eval;
									}
								}
							}
							break;

						default:
							throw new InvalidCastException("Unrecognized data type");
					}

					throw new Exception("Invalid data");
				}
			}
			else
			{
				if (property.PropertyType == typeof(Guid))
				{
					return reader.ReadGuid(property.Name);
				}
				else if (property.PropertyType == typeof(DateTimeOffset))
				{
					return reader.ReadDateTimeOffset(property.Name);
				}
				else if (property.PropertyType == typeof(TimeSpan))
				{
					return reader.ReadTimeSpan(property.Name);
				}
				else
				{
					//	Read the value of the property from the database based on its type.
					//	And then set the value in the object.
					return (Type.GetTypeCode(propertyType)) switch
					{
						TypeCode.Boolean => reader.ReadBoolean(property.Name),
						TypeCode.Byte => reader.ReadByte(property.Name),
						TypeCode.Char => reader.ReadChar(property.Name),
						TypeCode.DateTime => reader.ReadDateTime(property.Name),
						TypeCode.Decimal => reader.ReadDecimal(property.Name),
						TypeCode.Double => reader.ReadDouble(property.Name),
						TypeCode.Single => reader.ReadSingle(property.Name),
						TypeCode.Int16 => reader.ReadInt16(property.Name),
						TypeCode.Int32 => reader.ReadInt32(property.Name),
						TypeCode.Int64 => reader.ReadInt64(property.Name),
						TypeCode.SByte => reader.ReadSByte(property.Name),
						TypeCode.String => reader.ReadString(property.Name),
						TypeCode.UInt16 => reader.ReadUInt16(property.Name),
						TypeCode.UInt32 => reader.ReadUInt32(property.Name),
						TypeCode.UInt64 => reader.ReadUInt64(property.Name),
						_ => throw new InvalidCastException("Unrecognized data type"),
					};
				}
			}
		}

		/// <summary>
		/// Read the property
		/// </summary>
		/// <param name="reader"></param>
		/// <param name="property"></param>
		public static async Task<object> ReadPropertyAsync(this SqlDataReader reader, PropertyInfo property)
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
			var propertyType = property.PropertyType;

			if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
			{
				propertyType = Nullable.GetUnderlyingType(property.PropertyType);
			}

			if (propertyType.IsEnum)
			{
				var enumMember = propertyType.GetMembers().FirstOrDefault(member => member.DeclaringType == propertyType && member.Name != "value__");
				var stringType = enumMember.GetCustomAttribute<StringValue>();
				var enumValues = propertyType.GetEnumValues();

				if (stringType != null)
				{
					var propertyValue = await reader.ReadStringAsync(property.Name);
					var member = propertyType.GetMembers().FirstOrDefault(m => m.DeclaringType == propertyType &&
																		   m.Name != "value__" &&
																		   m.GetCustomAttribute<StringValue>().Value.ToLower() == propertyValue.ToLower());

					if (member != null)
					{
						foreach (var eval in enumValues)
						{
							if (eval.ToString() == member.Name)
							{
								return eval;
							}
						}
					}

					throw new Exception("Invalid data");
				}
				else
				{
					var underlyingType = propertyType.GetEnumUnderlyingType();

					switch (Type.GetTypeCode(underlyingType))
					{
						case TypeCode.Byte:
							{
								var propertyValue = await reader.ReadByteAsync(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (byte)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.SByte:
							{
								var propertyValue = await reader.ReadSByteAsync(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (SByte)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int16:
							{
								var propertyValue = await reader.ReadInt16Async(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (short)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt16:
							{
								var propertyValue = await reader.ReadUInt16Async(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (ushort)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int32:
							{
								var propertyValue = await reader.ReadInt32Async(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (int)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt32:
							{
								var propertyValue = await reader.ReadUInt32Async(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (uint)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int64:
							{
								var propertyValue = await reader.ReadInt64Async(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (long)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt64:
							{
								var propertyValue = await reader.ReadUInt64Async(property.Name);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (ulong)eval)
									{
										return eval;
									}
								}
							}
							break;

						default:
							throw new InvalidCastException("Unrecognized data type");
					}

					throw new Exception("Invalid data");
				}
			}
			else
			{
				if (property.PropertyType == typeof(Guid))
				{
					return await reader.ReadGuidAsync(property.Name);
				}
				else if (property.PropertyType == typeof(DateTimeOffset))
				{
					return await reader.ReadDateTimeOffsetAsync(property.Name);
				}
				else if (property.PropertyType == typeof(TimeSpan))
				{
					return await reader.ReadTimeSpanAsync(property.Name);
				}
				else
				{
					//	Read the value of the property from the database based on its type.
					//	And then set the value in the object.
					return (Type.GetTypeCode(propertyType)) switch
					{
						TypeCode.Boolean => await reader.ReadBooleanAsync(property.Name),
						TypeCode.Byte => await reader.ReadByteAsync(property.Name),
						TypeCode.Char => await reader.ReadCharAsync(property.Name),
						TypeCode.DateTime => await reader.ReadDateTimeAsync(property.Name),
						TypeCode.Decimal => await reader.ReadDecimalAsync(property.Name),
						TypeCode.Double => await reader.ReadDoubleAsync(property.Name),
						TypeCode.Single => await reader.ReadSingleAsync(property.Name),
						TypeCode.Int16 => await reader.ReadInt16Async(property.Name),
						TypeCode.Int32 => await reader.ReadInt32Async(property.Name),
						TypeCode.Int64 => await reader.ReadInt64Async(property.Name),
						TypeCode.SByte => await reader.ReadSByteAsync(property.Name),
						TypeCode.String => await reader.ReadStringAsync(property.Name),
						TypeCode.UInt16 => await reader.ReadUInt16Async(property.Name),
						TypeCode.UInt32 => await reader.ReadUInt32Async(property.Name),
						TypeCode.UInt64 => await reader.ReadUInt64Async(property.Name),
						_ => throw new InvalidCastException("Unrecognized data type"),
					};
				}
			}
		}

		/// <summary>
		/// Read the property
		/// </summary>
		/// <param name="reader"></param>
		/// <param name="property"></param>
		/// <param name="token">The Cancellation token</param>
		public static async Task<object> ReadPropertyAsync(this SqlDataReader reader, PropertyInfo property, CancellationToken token)
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
			var propertyType = property.PropertyType;

			if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
			{
				propertyType = Nullable.GetUnderlyingType(property.PropertyType);
			}

			if (propertyType.IsEnum)
			{
				var enumMember = propertyType.GetMembers().FirstOrDefault(member => member.DeclaringType == propertyType && member.Name != "value__");
				var stringType = enumMember.GetCustomAttribute<StringValue>();
				var enumValues = propertyType.GetEnumValues();

				if (stringType != null)
				{
					var propertyValue = await reader.ReadStringAsync(property.Name, token);
					var member = propertyType.GetMembers().FirstOrDefault(m => m.DeclaringType == propertyType &&
																		   m.Name != "value__" &&
																		   m.GetCustomAttribute<StringValue>().Value.ToLower() == propertyValue.ToLower());

					if (member != null)
					{
						foreach (var eval in enumValues)
						{
							if (eval.ToString() == member.Name)
							{
								return eval;
							}
						}
					}

					throw new Exception("Invalid data");
				}
				else
				{
					var underlyingType = propertyType.GetEnumUnderlyingType();

					switch (Type.GetTypeCode(underlyingType))
					{
						case TypeCode.Byte:
							{
								var propertyValue = await reader.ReadByteAsync(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (byte)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.SByte:
							{
								var propertyValue = await reader.ReadSByteAsync(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (SByte)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int16:
							{
								var propertyValue = await reader.ReadInt16Async(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (short)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt16:
							{
								var propertyValue = await reader.ReadUInt16Async(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (ushort)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int32:
							{
								var propertyValue = await reader.ReadInt32Async(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (int)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt32:
							{
								var propertyValue = await reader.ReadUInt32Async(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (uint)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.Int64:
							{
								var propertyValue = await reader.ReadInt64Async(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (long)eval)
									{
										return eval;
									}
								}
							}
							break;

						case TypeCode.UInt64:
							{
								var propertyValue = await reader.ReadUInt64Async(property.Name, token);

								foreach (var eval in enumValues)
								{
									if (propertyValue == (ulong)eval)
									{
										return eval;
									}
								}
							}
							break;

						default:
							throw new InvalidCastException("Unrecognized data type");
					}

					throw new Exception("Invalid data");
				}
			}
			else
			{
				if (property.PropertyType == typeof(Guid))
				{
					return await reader.ReadGuidAsync(property.Name, token);
				}
				else if (property.PropertyType == typeof(DateTimeOffset))
				{
					return await reader.ReadDateTimeOffsetAsync(property.Name, token);
				}
				else if (property.PropertyType == typeof(TimeSpan))
				{
					return await reader.ReadTimeSpanAsync(property.Name, token);
				}
				else
				{
					//	Read the value of the property from the database based on its type.
					//	And then set the value in the object.
					return (Type.GetTypeCode(propertyType)) switch
					{
						TypeCode.Boolean => await reader.ReadBooleanAsync(property.Name, token),
						TypeCode.Byte => await reader.ReadByteAsync(property.Name, token),
						TypeCode.Char => await reader.ReadCharAsync(property.Name, token),
						TypeCode.DateTime => await reader.ReadDateTimeAsync(property.Name, token),
						TypeCode.Decimal => await reader.ReadDecimalAsync(property.Name, token),
						TypeCode.Double => await reader.ReadDoubleAsync(property.Name, token),
						TypeCode.Single => await reader.ReadSingleAsync(property.Name, token),
						TypeCode.Int16 => await reader.ReadInt16Async(property.Name, token),
						TypeCode.Int32 => await reader.ReadInt32Async(property.Name, token),
						TypeCode.Int64 => await reader.ReadInt64Async(property.Name, token),
						TypeCode.SByte => await reader.ReadSByteAsync(property.Name, token),
						TypeCode.String => await reader.ReadStringAsync(property.Name, token),
						TypeCode.UInt16 => await reader.ReadUInt16Async(property.Name, token),
						TypeCode.UInt32 => await reader.ReadUInt32Async(property.Name, token),
						TypeCode.UInt64 => await reader.ReadUInt64Async(property.Name, token),
						_ => throw new InvalidCastException("Unrecognized data type"),
					};
				}
			}
		}
		#endregion

		#region Read item
		/// <summary>
		/// Creates and populates an object of type T from the data in the database.
		/// </summary>
		/// <param name="reader">The SqlDataReader used to populate the object</param>
		/// <param name="node">The list of fields to populate</param>
		/// <param name="T">The type of object to create an populate</param>
		/// <returns></returns>
		public static object Read(this SqlDataReader reader, RqlNode node, Type T)
		{
			var model = Activator.CreateInstance(T);
			PropertyInfo[] properties = T.GetProperties();
			var aggregates = RqlUtilities.ExtractAggregates(node);

			if (aggregates != null && aggregates.Count > 0)
			{
				foreach (var aggregate in aggregates)
				{
					var property = properties.FirstOrDefault(p => string.Equals(p.Name, aggregate.Value<string>(), StringComparison.OrdinalIgnoreCase));

					if (property != null)
					{
						var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

						if (memberAttribute != null)
						{
							property.SetValue(model, reader.ReadProperty(property));
						}
					}
				}
			}
			else
			{
				var selectFields = RqlUtilities.ExtractClause(RqlNodeType.SELECT, node);

				foreach (var property in properties)
				{
					//	Decide if we want to include this field in the result set. It must be a member attribute to be read from the database
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						//	If the select list is empty, include all fields.
						//	If the select list is not empty, and the field is the primary key, include it.
						//	If the select list is not empty, and the field is not the primary key, only include it if it is in the list.
						var readProperty = true;

						if (selectFields != null &&     //	we have a list 
							selectFields.Value<List<string>>().Count > 0 && //	and it has at least one entries in it.
																			//  and this field is not in the list.
							selectFields.Value<List<string>>().FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.OrdinalIgnoreCase)) == null)
						{
							readProperty = property.GetCustomAttribute<MemberAttribute>().IsPrimaryKey ? true : false;
						}

						if (readProperty)
						{
							if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
							{
								var underlyingType = Nullable.GetUnderlyingType(property.PropertyType);

								if (reader.IsDBNull(reader.GetOrdinal(property.Name)))
								{
									property.SetValue(model, null);
								}
								else
								{
									property.SetValue(model, reader.ReadProperty(property));
								}
							}
							else
							{
								property.SetValue(model, reader.ReadProperty(property));
							}
						}
					}
				}
			}

			return model;
		}

		/// <summary>
		/// Creates and populates an object of type T from the data in the database.
		/// </summary>
		/// <param name="reader">The SqlDataReader used to populate the object</param>
		/// <param name="node">The list of fields to populate</param>
		/// <param name="T">The type of object to create an populate</param>
		/// <returns></returns>
		public static async Task<object> ReadAsync(this SqlDataReader reader, RqlNode node, Type T)
		{
			var model = Activator.CreateInstance(T);
			PropertyInfo[] properties = T.GetProperties();
			var aggregates = RqlUtilities.ExtractAggregates(node);

			if (aggregates != null && aggregates.Count > 0)
			{
				foreach (var aggregate in aggregates)
				{
					var property = properties.FirstOrDefault(p => string.Equals(p.Name, aggregate.Value<string>(), StringComparison.OrdinalIgnoreCase));

					if (property != null)
					{
						var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

						if (memberAttribute != null)
						{
							property.SetValue(model, await reader.ReadPropertyAsync(property));
						}
					}
				}
			}
			else
			{
				var selectFields = RqlUtilities.ExtractClause(RqlNodeType.SELECT, node);

				foreach (var property in properties)
				{
					//	Decide if we want to include this field in the result set. It must be a member attribute to be read from the database
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						//	If the select list is empty, include all fields.
						//	If the select list is not empty, and the field is the primary key, include it.
						//	If the select list is not empty, and the field is not the primary key, only include it if it is in the list.
						var readProperty = true;

						if (selectFields != null &&     //	we have a list 
							selectFields.Value<List<string>>().Count > 0 && //	and it has at least one entries in it.
																			//  and this field is not in the list.
							selectFields.Value<List<string>>().FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.OrdinalIgnoreCase)) == null)
						{
							readProperty = property.GetCustomAttribute<MemberAttribute>().IsPrimaryKey ? true : false;
						}

						if (readProperty)
						{
							if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
							{
								var underlyingType = Nullable.GetUnderlyingType(property.PropertyType);

								if (reader.IsDBNull(reader.GetOrdinal(property.Name)))
								{
									property.SetValue(model, null);
								}
								else
								{
									property.SetValue(model, await reader.ReadPropertyAsync(property));
								}
							}
							else
							{
								property.SetValue(model, await reader.ReadPropertyAsync(property));
							}
						}
					}
				}
			}

			return model;
		}


		/// <summary>
		/// Creates and populates an object of type T from the data in the database.
		/// </summary>
		/// <param name="reader">The SqlDataReader used to populate the object</param>
		/// <param name="node">The list of fields to populate</param>
		/// <param name="T">The type of object to create an populate</param>
		/// <param name="token">The Cancellation token</param>
		/// <returns></returns>
		public static async Task<object> ReadAsync(this SqlDataReader reader, RqlNode node, Type T, CancellationToken token)
		{
			var model = Activator.CreateInstance(T);
			PropertyInfo[] properties = T.GetProperties();
			var aggregates = RqlUtilities.ExtractAggregates(node);

			if (aggregates != null && aggregates.Count > 0)
			{
				foreach (var aggregate in aggregates)
				{
					var property = properties.FirstOrDefault(p => string.Equals(p.Name, aggregate.Value<string>(), StringComparison.OrdinalIgnoreCase));

					if (property != null)
					{
						var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

						if (memberAttribute != null)
						{
							property.SetValue(model, await reader.ReadPropertyAsync(property, token));
						}
					}
				}
			}
			else
			{
				var selectFields = RqlUtilities.ExtractClause(RqlNodeType.SELECT, node);

				foreach (var property in properties)
				{
					//	Decide if we want to include this field in the result set. It must be a member attribute to be read from the database
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						//	If the select list is empty, include all fields.
						//	If the select list is not empty, and the field is the primary key, include it.
						//	If the select list is not empty, and the field is not the primary key, only include it if it is in the list.
						var readProperty = true;

						if (selectFields != null &&     //	we have a list 
							selectFields.Value<List<string>>().Count > 0 && //	and it has at least one entries in it.
																			//  and this field is not in the list.
							selectFields.Value<List<string>>().FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.OrdinalIgnoreCase)) == null)
						{
							readProperty = property.GetCustomAttribute<MemberAttribute>().IsPrimaryKey ? true : false;
						}

						if (readProperty)
						{
							if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
							{
								var underlyingType = Nullable.GetUnderlyingType(property.PropertyType);

								if (reader.IsDBNull(reader.GetOrdinal(property.Name)))
								{
									property.SetValue(model, null);
								}
								else
								{
									property.SetValue(model, await reader.ReadPropertyAsync(property, token));
								}
							}
							else
							{
								property.SetValue(model, await reader.ReadPropertyAsync(property, token));
							}
						}
					}
				}
			}

			return model;
		}
		#endregion
	}
}
