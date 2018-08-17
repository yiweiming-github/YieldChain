using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using YieldChain.EFExtension.BulkExtensions;
using MySql.Data.MySqlClient;

namespace YieldChain.EFExtension.BulkExtensions.MySql
{
    /// <summary>
    /// Extend to support bulk insert for Mysql
    /// </summary>
    public static class MySqlBulkExtensions
    {
        public static void BulkInsert<T>(this DbContext context, IEnumerable<T> entities)
        {
            if (entities != null && entities.Count() > 0)
            {
                var provider = new BulkInsertProvider(context);
                var sql = provider.GenerateInsertSql<T>(entities);
                
                provider.ExecuteSql(sql);
            }
        }

        public static void BulkDelete<T>(this DbContext context, IEnumerable<T> entities)
        {
            if (entities != null && entities.Count() > 0)
            {
                var provider = new BulkInsertProvider(context);
                var sql = provider.GenerateDeleteSql<T>(entities);
                provider.ExecuteSql(sql);
            }
        }

        public static void BulkUpdate<T>(this DbContext context, IEnumerable<T> entities)
        {
            if (entities != null && entities.Count() > 0)
            {
                var provider = new BulkInsertProvider(context);
                var sql = provider.GenerateUpdateSql<T>(entities);
                provider.ExecuteSqlWithTransaction(sql);
            }
        }
    }

    /// <summary>
    /// MySql bulk insert provider class.
    /// Convert a list of entities into INSERT SQL text and execute it.
    /// </summary>
    public class BulkInsertProvider
    {
        public BulkInsertProvider(DbContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Execute Sql
        /// </summary>
        /// <param name="sql"></param>
        public void ExecuteSql(string sql)
        {
            var connection = (MySqlConnection)_context.Database.Connection;
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            using (var sqlCmd = new MySqlCommand(sql, connection))
            {
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.ExecuteNonQuery();
            }

            connection.Dispose();
        }

        /// <summary>
        /// Execute Sql
        /// </summary>
        /// <param name="sql"></param>
        public void ExecuteSqlWithTransaction(string sql)
        {
            var connection = (MySqlConnection)_context.Database.Connection;
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            using (var trans = connection.BeginTransaction())
            {
                try
                {
                    using (var sqlCmd = new MySqlCommand(sql, connection))
                    {
                        sqlCmd.CommandType = CommandType.Text;
                        sqlCmd.ExecuteNonQuery();
                    }
                    trans.Commit();
                }
                catch(Exception ex)
                {
                    trans.Rollback();
                }
            }

            connection.Dispose();
        }

        #region INSERT
        /// <summary>
        /// Generate INSERT SQL text
        /// </summary>
        /// <typeparam name="T">entity</typeparam>
        /// <param name="entities">a list of entities</param>
        /// <param name="hasIdColumn">has id column or not, if it's true, will skip column with name "id"</param>
        /// <returns>the INSERT SQL text</returns>
        public string GenerateInsertSql<T>(IEnumerable<T> entities, bool hasIdColumn = true)
        {
            var tableMapping = DbMapper.GetDbMapping(_context)[typeof(T)];
            var columns = hasIdColumn ? tableMapping.Columns.Where(x => x.ColumnName != "id") : tableMapping.Columns;
            var stringBuilder = new StringBuilder();
            stringBuilder.Append(GenerateSchemaPartOfInsertSql(tableMapping, columns));
            stringBuilder.Append(GenerateValuePartOfInsertSql<T>(entities, columns));
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Generate the “INSERT .... VALUES” part of an INSERT SQL
        /// </summary>
        /// <param name="tableMapping">table mapping info</param>
        /// <param name="columns">column mapping info</param>
        /// <returns>the part of INSERT SQL text</returns>
        private string GenerateSchemaPartOfInsertSql(TableMapping tableMapping, IEnumerable<ColumnMapping> columns)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append($"INSERT INTO {tableMapping.TableName} (");
            stringBuilder.Append(string.Join(",", columns.Select(x => x.ColumnName)));
            stringBuilder.Append(") VALUES ");
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Generate the part after "VALUES" of an INSERT SQL
        /// </summary>
        /// <typeparam name="T">entity</typeparam>
        /// <param name="entities">a list of entities</param>
        /// <param name="columns">column mapping info</param>
        /// <returns>the part of INSERT SQL text</returns>
        private string GenerateValuePartOfInsertSql<T>(IEnumerable<T> entities, IEnumerable<ColumnMapping> columns)
        {
            var sqls = new List<string>();
            foreach (var entity in entities)
            {
                sqls.Add(GenerateSingleValueRowOfInsertSql(entity, columns));
            }
            return string.Join(",", sqls) + ";";
        }

        /// <summary>
        /// Generate a single row of VALUES part of INSERT SQL, for example: ("aaa", 1.0, "bbb")
        /// </summary>
        /// <typeparam name="T">entity</typeparam>
        /// <param name="entity">entity</param>
        /// <param name="columns">column mapping info</param>
        /// <returns>the part of INSERT SQL text</returns>
        private string GenerateSingleValueRowOfInsertSql<T>(T entity, IEnumerable<ColumnMapping> columns)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append("(");
            var fields = new List<string>();
            foreach (var column in columns)
            {
                fields.Add(GenerateFieldOfInsertSql<T>(entity, column));
            }
            stringBuilder.Append(string.Join(",", fields));
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Generate a single field of a row in INSERT SQL
        /// </summary>
        /// <typeparam name="T">entity</typeparam>
        /// <param name="entity">entity</param>
        /// <param name="columns">column mapping info</param>
        /// <returns>the part of INSERT SQL text</returns>
        private string GenerateFieldOfInsertSql<T>(T entity, ColumnMapping column)
        {
            var obj = entity.GetPropertyValue(column.ColumnName);
            if (obj == null)
            {
                return "NULL";
            }
            else if (obj is string || obj is DateTime)
            {
                return $"\'{obj.ToString()}\'";
            }
            else
            {
                return obj.ToString();
            }
        }
        #endregion

        #region DELETE
        /// <summary>
        /// Generate DELETE SQL text
        /// </summary>
        /// <typeparam name="T">entity</typeparam>
        /// <param name="entities">a list of entities</param>
        /// <returns>the SQL</returns>
        public string GenerateDeleteSql<T>(IEnumerable<T> entities)
        {
            var tableMapping = DbMapper.GetDbMapping(_context)[typeof(T)];
            var pkColumn = tableMapping.Columns.FirstOrDefault(x => x.ColumnName == "id");
            if (pkColumn == null || !pkColumn.IsPk)
            {
                throw new Exception("Can only perform bulk delete on a table with pk column named \"id\"");
            }
            var stringBuilder = new StringBuilder();
            stringBuilder.Append(GenerateSchemaPartOfDeleteSql(tableMapping));
            stringBuilder.Append(GenerateConditionPartOfDeleteSql(entities));
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Generate DELETE FROM .... part
        /// </summary>
        /// <param name="tableMapping">table mapping info</param>        
        /// <returns>the part of DELETE SQL text</returns>
        private string GenerateSchemaPartOfDeleteSql(TableMapping tableMapping)
        {
            return $"DELETE FROM {tableMapping.TableName} WHERE id IN ";
        }

        /// <summary>
        /// Generate DELETE condition part
        /// </summary>
        /// <typeparam name="T">entity</typeparam>
        /// <param name="entities">a list of entities</param>
        /// <returns>the part of DELETE SQL text</returns>
        private string GenerateConditionPartOfDeleteSql<T>(IEnumerable<T> entities)
        {
            var ids = string.Join(",", entities.Select(x => x.GetPropertyValue("id").ToString()));
            return $"( {ids} );";
        }
        #endregion

        #region UPDATE
        /// <summary>
        /// Generate Update SQL
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <returns></returns>
        public string GenerateUpdateSql<T>(IEnumerable<T> entities)
        {
            var tableMapping = DbMapper.GetDbMapping(_context)[typeof(T)];
            var pkColumn = tableMapping.Columns.FirstOrDefault(x => x.ColumnName == "id");
            if (pkColumn == null || !pkColumn.IsPk)
            {
                throw new Exception("Can only perform bulk update on a table with pk column named \"id\"");
            }
            var columns = tableMapping.Columns.Where(x => x.ColumnName != "id");
            var stringBuilder = new StringBuilder();
            foreach (var entity in entities)
            {
                stringBuilder.Append(GenerateUpdateSqlForSingleRecord(entity, tableMapping.TableName, columns));
            }
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Generate UPDATE SQL for one single record
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="columns"></param>
        /// <returns></returns>
        private string GenerateUpdateSqlForSingleRecord<T>(T entity, string tableName, IEnumerable<ColumnMapping> columns)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append($"UPDATE {tableName} SET ");
            var fields = new List<string>();
            foreach (var column in columns)
            {
                fields.Add(GenerateFieldOfUpdateSql<T>(entity, column));
            }
            stringBuilder.Append(string.Join(",", fields));
            stringBuilder.Append($" WHERE id = {entity.GetPropertyValue("id")}; ");
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Generate UPDATE SQL for one data field
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <param name="column"></param>
        /// <returns></returns>
        private string GenerateFieldOfUpdateSql<T>(T entity, ColumnMapping column)
        {
            var obj = entity.GetPropertyValue(column.ColumnName);
            if (obj == null)
            {
                return $"`{column.ColumnName}` = NULL";
            }
            else if (obj is string || obj is DateTime)
            {
                return $"`{column.ColumnName}` = \'{obj.ToString()}\'";
            }
            else
            {
                return $"`{column.ColumnName}` = {obj.ToString()}";
            }
        }
        #endregion

        private DbContext _context;
    }

    public class BulkTransaction : IDisposable
    {
        public BulkTransaction(DbContext db)
        {
            _provider = new BulkInsertProvider(db);
            _sql = "";
        }

        public void BulkInsert<T>(IEnumerable<T> entities)
        {
            _sql += _provider.GenerateInsertSql<T>(entities);
        }

        public void BulkDelete<T>(IEnumerable<T> entities)
        {
            _sql += _provider.GenerateDeleteSql<T>(entities);
        }

        public void BulkUpdate<T>(IEnumerable<T> entities)
        {
            _sql += _provider.GenerateUpdateSql(entities);
        }

        public void Commit()
        {
            if (!string.IsNullOrWhiteSpace(_sql))
            {
                _provider.ExecuteSqlWithTransaction(_sql);
            }
        }

        public void Dispose()
        {
            _sql = null;
            _provider = null;
            GC.SuppressFinalize(this);
        }

        private string _sql;
        private BulkInsertProvider _provider;
    }
}
