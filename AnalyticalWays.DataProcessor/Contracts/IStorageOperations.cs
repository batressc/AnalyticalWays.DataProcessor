using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Contracts {
    /// <summary>
    /// Define las operaciones a realizar sobre el espacio de almacenamiento de archivos
    /// </summary>
    public interface IStorageOperations {
        /// <summary>
        /// Determina si el archivo especificado existe en el espacio de almacenamiento
        /// </summary>
        /// <param name="filename">Nombre del archivo</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns>Indicador si existe o no el archivo especificado</returns>
        Task<bool> FileExists(string filename, CancellationToken cancellationToken);

        /// <summary>
        /// Obtiene la información del archivo especificado y permite aplicar las acciones a realizar con el Stream de datos del archivo
        /// </summary>
        /// <param name="filename">Nombre del archivo</param>
        /// <param name="cancellationToken">Token de cancelación</param>
        /// <returns>Stream del archivo para su lectura</returns>
        Task<Stream> ReadFile(string filename, CancellationToken cancellationToken);
    }
}
