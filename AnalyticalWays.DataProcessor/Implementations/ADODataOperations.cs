using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Implementations {
    public class ADODataOperations : IDataOperations<StockInformation> {
        private readonly string _connectionString;

        public ADODataOperations(IConfiguration conf) {
            _connectionString = conf.GetConnectionString("AnalyticalWaysDatabase");
        }

        private DataTable ToDataTable(List<StockInformation> datos) {
            DataTable table = new DataTable("StockInformation");
            table.Columns.Add("pos", typeof(string));
            table.Columns.Add("product", typeof(string));
            table.Columns.Add("date", typeof(DateTime));
            table.Columns.Add("stock", typeof(int));
            foreach (StockInformation item in datos) {
                DataRow row = table.NewRow();
                row["pos"] = item.Pos;
                row["product"] = item.Product;
                row["date"] = item.StockDate;
                row["stock"] = item.Stock;
                table.Rows.Add(row);
            }
            return table;
        }


        public async Task<bool> AppendData(IEnumerable<StockInformation> datos) {
            try {
                using SqlConnection conn = new SqlConnection(_connectionString);
                using SqlBulkCopy bulk = new SqlBulkCopy(conn) {
                    DestinationTableName = "StockInformation"
                };
                DataTable datosTable = ToDataTable((List<StockInformation>)datos);
                await conn.OpenAsync();
                await bulk.WriteToServerAsync(datosTable);
                return bulk.RowsCopied > 0;
            } catch (Exception ex) {
                _ = ex;
                throw;
            }
        }

        public async Task<bool> DeletePreviousData() {
            try {
                using SqlConnection conn = new SqlConnection(_connectionString);
                using SqlCommand command = new SqlCommand("truncate table [dbo].[StockInformation];", conn);
                await conn.OpenAsync();
                await command.ExecuteNonQueryAsync();
                return true;
            } catch (Exception ex) {
                _ = ex;
                throw;
            }
        }

        public async Task<bool> ExistsPreviousData() {
            try {
                using SqlConnection conn = new SqlConnection(_connectionString);
                using SqlCommand command = new SqlCommand("select count(1) from [dbo].[StockInformation];", conn);
                await conn.OpenAsync();
                int filas = (int)await command.ExecuteScalarAsync();
                return filas > 0;
            } catch (Exception ex) {
                _ = ex;
                throw;
            }
        }
    }
}
