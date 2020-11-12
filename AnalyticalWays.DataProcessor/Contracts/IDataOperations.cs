using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Contracts {
    /// <summary>
    /// Define las operaciones a realizar sobre el repositorio de datos
    /// </summary>
    /// <typeparam name="TData">Datos a almacenar</typeparam>
    public interface IDataOperations<TData> {
        /// <summary>
        /// Verifica si existen datos previos en el repositorio de datos
        /// </summary>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns>Indicador si existen o no datos en el repositorio de datos</returns>
        Task<bool> ExistsPreviousData(CancellationToken cancellationToken);

        /// <summary>
        /// Permite borrar los datos previos en el repositorio de datos
        /// </summary>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns>Indicador de éxito o fallo de la operación</returns>
        Task<bool> DeletePreviousData(CancellationToken cancellationToken);

        /// <summary>
        /// Agrega el listado de registros del tipo <typeparamref name="TData"/> en el repositorio de datos
        /// </summary>
        /// <param name="datos">Listado de datos a agregar</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns>Indicador de éxito o fallo de la operación</returns>
        Task<bool> AppendData(IEnumerable<TData> datos, CancellationToken cancellationToken);
    }
}
