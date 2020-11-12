using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Implementations {
    /// <summary>
    /// Operaciones de almacenamiento de datos en base de datos SQL Server utilizando ADO.NET para la entidad <see cref="StockInformation"/>
    /// </summary>
    public class ADODataOperations : IDataOperations<StockInformation> {
        private readonly string _connectionString;

        /// <summary>
        /// Crea una nueva instancia de la clase <see cref="ADODataOperations"/>
        /// </summary>
        /// <param name="conf">Servicio de configuración de la aplicación</param>
        public ADODataOperations(IConfiguration conf) {
            _connectionString = conf.GetConnectionString("AnalyticalWaysDatabase");
        }

        /// <summary>
        /// Convierte una lista de elementos a DataTable
        /// </summary>
        /// <param name="datos">Lista de información de stock</param>
        /// <returns>DataTable con la información de Stock</returns>
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

        // Verifica si existen datos previos en el repositorio de datos
        /// <inheritdoc/>
        public async Task<bool> ExistsPreviousData(CancellationToken cancellationToken) {
            using SqlConnection conn = new SqlConnection(_connectionString);
            using SqlCommand command = new SqlCommand("select count(1) from [dbo].[StockInformation];", conn);
            await conn.OpenAsync(cancellationToken);
            int filas = (int)await command.ExecuteScalarAsync(cancellationToken);
            return filas > 0;
        }

        // Permite borrar los datos previos en el repositorio de datos
        /// <inheritdoc/>
        public async Task<bool> DeletePreviousData(CancellationToken cancellationToken) {
            using SqlConnection conn = new SqlConnection(_connectionString);
            using SqlCommand command = new SqlCommand("truncate table [dbo].[StockInformation];", conn);
            await conn.OpenAsync(cancellationToken);
            await command.ExecuteNonQueryAsync(cancellationToken);
            // En este caso devolvemos true dado que la operación no reporta registros afectados. Si hubiera
            // algun error se gestionaría en el programa principal (caso de no ejecución)
            return true;
        }

        // Agrega el listado de registros en el repositorio de datos
        /// <inheritdoc/>
        public async Task<bool> AppendData(IEnumerable<StockInformation> datos, CancellationToken cancellationToken) {
            using SqlConnection conn = new SqlConnection(_connectionString);
            using SqlBulkCopy bulk = new SqlBulkCopy(conn) {
                DestinationTableName = "StockInformation",                
            };
            DataTable datosTable = ToDataTable((List<StockInformation>)datos);
            await conn.OpenAsync(cancellationToken);
            await bulk.WriteToServerAsync(datosTable, cancellationToken);
            return bulk.RowsCopied > 0;
        }
    }
}
