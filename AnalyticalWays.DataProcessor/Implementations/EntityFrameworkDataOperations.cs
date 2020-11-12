using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Implementations {
    /// <summary>
    /// Operaciones de almacenamiento de datos en base de datos SQL Server utilizando Entity Framewo para la entidad <see cref="StockInformation"/>
    /// </summary>
    public class EntityFrameworkDataOperations : IDataOperations<StockInformation> {
        private readonly IServiceScope _scope;
        private readonly AnalyticalWaysTestDbContext _db;

        /// <summary>
        /// Crea una nueva instancia de la clase <see cref="EntityFrameworkDataOperations"/>
        /// </summary>
        /// <param name="ssf">Gestor de servicios</param>
        public EntityFrameworkDataOperations(IServiceScopeFactory ssf) {
            // Creamos una instancia con ciclo de vida Scoped de forma manual ya que la instancia del proceso padre
            // de la aplicación es Singleton y no permite realizar inyecciones de depencias Scoped desde la configuración
            // de servicios
            _scope = ssf.CreateScope();
            _db = _scope.ServiceProvider.GetService<AnalyticalWaysTestDbContext>();
        }

        // Verifica si existen datos previos en el repositorio de datos
        /// <inheritdoc/>
        public async Task<bool> ExistsPreviousData(CancellationToken cancellationToken) {
            int resultado = await _db.StockInformation.CountAsync(cancellationToken);
            return resultado > 0;
        }

        // Permite borrar los datos previos en el repositorio de datos
        /// <inheritdoc/>
        public async Task<bool> DeletePreviousData(CancellationToken cancellationToken) {
            await _db.Database.ExecuteSqlRawAsync("truncate table [dbo].[StockInformation]", cancellationToken);
            // En este caso devolvemos true dado que la operación no reporta registros afectados. Si hubiera
            // algun error se gestionaría en el programa principal (caso de no ejecución)
            return true;
        }

        // Agrega el listado de registros en el repositorio de datos
        /// <inheritdoc/>
        public async Task<bool> AppendData(IEnumerable<StockInformation> datos, CancellationToken cancellationToken) {
            await _db.BulkInsertAsync(datos as List<StockInformation>, cancellationToken: cancellationToken);
            // En este caso devolvemos true dado que la operación no reporta registros afectados. Si hubiera
            // algun error se gestionaría en el programa principal (caso de no ejecución)
            return true;
        }
    }
}
